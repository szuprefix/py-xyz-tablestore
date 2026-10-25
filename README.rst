xyz-tablestore
==============

Django REST Framework integration
---------------------------------

Install the optional dependencies with ``pip install 'xyz-tablestore[rest]'``.
Configure the OTS environment variables used by ``Store`` and provide explicit
serializer fields::

    from rest_framework import serializers
    from rest_framework.routers import DefaultRouter
    from xyz_tablestore.rest_framework import TableStoreSerializer, TableStoreViewSet

    class UserSerializer(TableStoreSerializer):
        id = serializers.CharField(required=False)
        name = serializers.CharField()
        age = serializers.IntegerField(required=False)

    class UserViewSet(TableStoreViewSet):
        store_name = 'users'
        serializer_class = UserSerializer
        filter_fields = ['name', 'age']
        filter_types = {'age': int}
        ordering_fields = ['age', 'id']

    router = DefaultRouter()
    router.register('users', UserViewSet, basename='users')
    urlpatterns = router.urls

Admin permission is required by default. The table and search index must already
exist; list queries use the search index. Configure normal DRF authentication in
the host application.

GET accepts ``page`` (starting at 1), ``page_size`` (1-100), ``ordering=-age,id``,
and field filters such as ``age__gte=18`` and ``age__in=18,20``. Use
``filter_types`` to convert query string values; unspecified fields remain strings.
Set ``filter_fields`` and ``ordering_fields`` to restrict accepted field names.
List responses contain ``count``, ``next``, ``previous``, ``next_token``, and
``results``. Follow ``next`` for token pagination; do not combine ``page`` with
``next_token``. Token pagination has no previous-page link.

POST requires all primary key fields and rejects duplicate keys (400). PUT
replaces all attributes with validated writable fields; PATCH only updates
submitted attributes. Primary keys cannot change. Null attributes and deletion
are not supported in this first implementation. Read-back after writes uses
primary-key reads, without waiting for the search index to refresh.

For a custom Store subclass, set ``store_class``. For composite keys, provide
custom URL patterns and ``lookup_fields = {'tenant': 'tenant', 'id': 'pk'}``,
where values name URL keyword arguments. INTEGER primary keys are converted
from URL strings.

The old ``MongoViewSet``, ``MongoSerializer``, and ``mongo_posted`` import names
remain aliases. Existing subclasses must migrate to explicit serializer fields;
Mongo schema inference, random queries, and foreign-key expansion are not used.
``store_posted`` is emitted after create/update. ``send_robust`` means receiver
failures do not roll back writes.

Run isolated tests with ``python -m unittest discover -s tests``.
