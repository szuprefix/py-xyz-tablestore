from .schema import Schema

from django.utils.functional import cached_property
from django.core.paginator import Paginator
from django.dispatch import Signal

from rest_framework.pagination import PageNumberPagination
from rest_framework import permissions, exceptions
from rest_framework import viewsets, response, serializers, fields
from django_filters.rest_framework.backends import DjangoFilterBackend

class MongoPaginator(Paginator):

    @cached_property
    def count(self):
        # print('count')
        return self.object_list.count()


class MongoPageNumberPagination(PageNumberPagination):
    django_paginator_class = MongoPaginator
    page_size = 100
    page_size_query_param = 'page_size'
    max_page_size = 1000


def get_paginated_response(view, query, wrap=lambda a: a):
    pager = MongoPageNumberPagination()
    ds = pager.paginate_queryset(query, view.request, view=view)
    rs = [wrap(a) for a in ds]
    return pager.get_paginated_response(json_util._json_convert(rs))



class MongoSerializer(serializers.ModelSerializer):

    def get_fields(self):
        assert hasattr(self, 'Meta'), (
            'Class {serializer_class} missing "Meta" attribute'.format(
                serializer_class=self.__class__.__name__
            )
        )
        assert hasattr(self.Meta, 'store'), (
            'Class {serializer_class} missing "Meta.store" attribute'.format(
                serializer_class=self.__class__.__name__
            )
        )
        schema = Schema()
        store = self.Meta.store
        rs = {}
        fm = {
            'string': fields.CharField,
            'integer': fields.IntegerField,
            'number': fields.FloatField,
            'array': fields.ListField,
            'object': fields.JSONField
        }
        for fn, ft in schema.desc(store.name).items():
            field = fm[ft]()
            rs[fn] = field
        return rs


mongo_posted = Signal()


class MongoViewSet(viewsets.ViewSet):
    permission_classes = [permissions.IsAdminUser]
    store_name = None
    store_class = None

    def dispatch(self, request, *args, **kwargs):
        self.store = self.get_store()
        return super(MongoViewSet, self).dispatch(request, *args, **kwargs)

    def get_store(self, name=None):
        if name:
            return Store(name=name)
        if self.store_class:
            return self.store_class()
        elif self.store_name:
            return Store(name=self.store_name)
        raise exceptions.NotFound()

    def get_foreign_key(self, store_name, id):
        st = self.get_store(store_name)
        return st.collection.get(id=id)

    def options(self, request, *args, **kwargs):
        # print(self.metadata_class)
        # return super(MongoViewSet, self).options(request, *args, **kwargs)
        sc = Schema().desc(self.get_store().name)
        return response.Response(sc)

    def get_serialize_fields(self):
        return None

    def filter_query(self, cond):
        return cond

    def list(self, request):
        # print(request.query_params)
        qps = request.query_params
        cond = self.store.normalize_filter(qps)
        # print(cond)
        cond = self.filter_query(cond)
        randc = qps.get('_random')
        ordering = qps.get('ordering')
        kwargs = {}
        if ordering:
            kwargs['sort'] = [ordering_to_sort(ordering)]
        if randc:
            rs = self.store.random_find(cond, count=int(randc), fields=self.get_serialize_fields())
            return response.Response(dict(results=json_util._json_convert(rs)))
        rs = self.store.find(cond, self.get_serialize_fields(), **kwargs)
        return get_paginated_response(self, rs, wrap=self.eval_foreign_keys)

    def eval_foreign_keys(self, d):
        fks = getattr(self, 'foreign_keys', None)
        return self.store.eval_foreign_keys(d, foreign_keys=fks)

    def get_object(self, id=None):
        _id = id if id else self.kwargs['pk']
        cond = {'_id': ObjectId(_id)}
        return json_util._json_convert(self.eval_foreign_keys(self.store.collection.find_one(cond, None)))

    def retrieve(self, request, pk):
        return response.Response(self.get_object())

    def get_serialized_data(self):
        d = {}
        d.update(self.request.data)
        return d

    def update(self, request, pk, *args, **kargs):
        instance = self.get_object()
        data = self.get_serialized_data()
        # print(data)
        self.store.update({'_id': ObjectId(pk)}, data)
        new_instance = self.get_object()
        mongo_posted.send_robust(sender=type(self), instance=new_instance, update=data, created=False)
        return response.Response(json_util._json_convert(new_instance))

    def create(self, request, *args, **kargs):
        data = self.get_serialized_data()
        r = self.store.collection.insert_one(data)
        return response.Response(self.get_object(r.inserted_id))

    def patch(self, request, pk, *args, **kargs):
        return self.update(request, pk, *args, **kargs)
