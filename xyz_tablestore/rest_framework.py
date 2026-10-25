"""Django REST Framework views for TableStore.

Subclasses must declare serializer_class with explicit writable fields. PUT
replaces attributes; PATCH only changes supplied attributes. Primary keys are
immutable. Composite keys use lookup_fields={column: url_kwarg}.
"""
import base64
import binascii

from django.core.exceptions import ImproperlyConfigured
from django.dispatch import Signal
from rest_framework import exceptions, permissions, response, serializers, status, viewsets
from rest_framework.utils.urls import replace_query_param, remove_query_param
from tablestore import Condition, OTSServiceError, Row, RowExistenceExpectation, SortOrder

from .store import Store
from .utils import dict2row


class TableStoreSerializer(serializers.Serializer):
    """Declare fields explicitly; no Django model or inferred schema is required."""


store_posted = Signal()


class TableStoreViewSet(viewsets.GenericViewSet):
    permission_classes = [permissions.IsAdminUser]
    store_name = None
    store_class = None
    lookup_fields = None
    page_size = 100
    max_page_size = 100
    filter_fields = None
    filter_types = None
    ordering_fields = None

    def get_store(self, name=None):
        if name is not None:
            return Store(name=name)
        if self.store_class is not None:
            return self.store_class()
        if self.store_name:
            return Store(name=self.store_name)
        raise ImproperlyConfigured('Set store_class or store_name.')

    @property
    def store(self):
        if not hasattr(self, '_store'):
            self._store = self.get_store()
        return self._store

    def get_serialize_fields(self):
        return None

    def filter_query(self, cond):
        return cond

    def get_primary_key(self):
        mapping = self.lookup_fields
        if mapping is None:
            if len(self.store.pks) != 1:
                raise ImproperlyConfigured('Composite keys require lookup_fields.')
            mapping = {self.store.pks[0]: self.lookup_url_kwarg or self.lookup_field}
        result = {}
        for field, kind in self.store.primary_key_schema:
            if field not in mapping or mapping[field] not in self.kwargs:
                raise ImproperlyConfigured('lookup_fields must map every primary key to a URL kwarg.')
            value = self.kwargs[mapping[field]]
            if kind == 'INTEGER':
                try:
                    value = int(value)
                except (TypeError, ValueError):
                    raise exceptions.ValidationError({field: 'Expected an integer primary key.'})
            result[field] = value
        return result

    def get_object(self):
        obj = self.store.get(self.get_primary_key())
        if obj is None:
            raise exceptions.NotFound()
        self.check_object_permissions(self.request, obj)
        return obj

    def _positive_int(self, name, default, maximum=None):
        try:
            value = int(self.request.query_params.get(name, default))
        except (TypeError, ValueError):
            raise exceptions.ValidationError({name: 'Expected a positive integer.'})
        if value < 1 or (maximum is not None and value > maximum):
            raise exceptions.ValidationError({name: 'Value outside the allowed range.'})
        return value

    def get_filters(self):
        result = {}
        reserved = {'page', 'page_size', 'next_token', 'ordering', 'format'}
        operators = {'eq', 'ne', 'in', 'nin', 'gt', 'gte', 'lt', 'lte', 'wildcard', 'regex', 'exists'}
        for key in self.request.query_params:
            if key in reserved:
                continue
            field, separator, op = key.partition('__')
            if (self.filter_fields is not None and field not in self.filter_fields) or (separator and op not in operators) or key.startswith('_'):
                raise exceptions.ValidationError({key: 'Unsupported filter.'})
            value = self.request.query_params[key]
            converter = (self.filter_types or {}).get(field, str)
            try:
                if op in ('in', 'nin'):
                    values = self.request.query_params.getlist(key)
                    value = [converter(v) for item in values for v in item.split(',')]
                elif op != 'exists':
                    value = converter(value)
            except (TypeError, ValueError):
                raise exceptions.ValidationError({key: 'Invalid filter value.'})
            result[key] = value
        return self.filter_query(result)

    def list(self, request, *args, **kwargs):
        size = self._positive_int('page_size', self.page_size, self.max_page_size)
        token = request.query_params.get('next_token')
        if token is not None:
            if not token or 'page' in request.query_params:
                raise exceptions.ValidationError('Use either page or a nonempty next_token.')
            try:
                base64.b64decode(token, altchars=b'-_', validate=True)
            except (ValueError, binascii.Error):
                raise exceptions.ValidationError({'next_token': 'Invalid token.'})
            page = None
        else:
            page = self._positive_int('page', 1)
            if (page - 1) * size > self.store.MAX_OFFSET:
                raise exceptions.ValidationError({'page': 'Use next_token for deep pagination.'})
        sort = []
        for item in filter(None, request.query_params.get('ordering', '').split(',')):
            field = item.lstrip('-')
            if not field or (self.ordering_fields is not None and field not in self.ordering_fields):
                raise exceptions.ValidationError({'ordering': 'Unsupported ordering field.'})
            sort.append((field, SortOrder.DESC if item.startswith('-') else SortOrder.ASC))
        result = self.store.search(
            self.get_filters(), columns=self.get_serialize_fields(),
            sort_fields=sort or None, page_no=page, page_size=size, next_token=token,
        )
        url = remove_query_param(request.build_absolute_uri(), 'page')
        next_url = replace_query_param(url, 'next_token', result['next_token']) if result['next_token'] else None
        previous = replace_query_param(url, 'page', page - 1) if page and page > 1 else None
        return response.Response({
            'count': result['total'], 'next': next_url, 'previous': previous,
            'next_token': result['next_token'],
            'results': self.get_serializer(result['items'], many=True).data,
        })

    def retrieve(self, request, *args, **kwargs):
        return response.Response(self.get_serializer(self.get_object()).data)

    def _validated_data(self, instance=None, partial=False):
        if not isinstance(self.request.data, dict):
            raise exceptions.ValidationError('Expected an object.')
        serializer = self.get_serializer(instance, data=self.request.data, partial=partial)
        serializer.is_valid(raise_exception=True)
        data = dict(serializer.validated_data)
        if any(value is None for value in data.values()):
            raise exceptions.ValidationError('Null attributes are not supported yet.')
        return data

    def create(self, request, *args, **kwargs):
        data = self._validated_data()
        missing = [key for key in self.store.pks if key not in data]
        if missing:
            raise exceptions.ValidationError({key: 'Primary key is required.' for key in missing})
        keys = {key: data[key] for key in self.store.pks}
        obj, created = self.store.get_or_create(keys, data)
        if not created:
            raise exceptions.ValidationError('A row with this primary key already exists.')
        store_posted.send_robust(sender=type(self), instance=obj, update=data, created=True)
        return response.Response(self.get_serializer(obj).data, status=status.HTTP_201_CREATED)

    def update(self, request, *args, **kwargs):
        partial = kwargs.pop('partial', False)
        instance = self.get_object()
        keys = self.get_primary_key()
        data = self._validated_data(instance, partial)
        for key, value in keys.items():
            if key in data and data[key] != value:
                raise exceptions.ValidationError({key: 'Primary keys cannot be changed.'})
        attributes = {key: value for key, value in data.items() if key not in keys}
        row = dict2row(dict(attributes, **keys), self.store.pks)
        condition = Condition(RowExistenceExpectation.EXPECT_EXIST)
        try:
            if partial:
                if attributes:
                    self.store.client.update_row(self.store.name, Row(row.primary_key, {'put': row.attribute_columns}), condition)
            else:
                self.store.client.put_row(self.store.name, row, condition=condition)
        except OTSServiceError as error:
            if error.code == 'OTSConditionCheckFail':
                raise exceptions.NotFound()
            raise
        obj = self.get_object()
        store_posted.send_robust(sender=type(self), instance=obj, update=data, created=False)
        return response.Response(self.get_serializer(obj).data)

    def partial_update(self, request, *args, **kwargs):
        return self.update(request, *args, partial=True, **kwargs)


# Backwards-compatible import names for existing subclasses.
MongoSerializer = TableStoreSerializer
MongoViewSet = TableStoreViewSet
mongo_posted = store_posted
