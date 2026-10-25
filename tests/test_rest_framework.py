import unittest
from types import SimpleNamespace
from unittest.mock import Mock

from django.conf import settings
if not settings.configured:
    settings.configure(SECRET_KEY='tests', ALLOWED_HOSTS=['testserver'], REST_FRAMEWORK={'UNAUTHENTICATED_USER': None})
import django
django.setup()
from rest_framework import serializers
from rest_framework.routers import SimpleRouter
from rest_framework.test import APIRequestFactory, force_authenticate
from tablestore import OTSServiceError, SortOrder, RowExistenceExpectation
from xyz_tablestore.rest_framework import TableStoreSerializer, TableStoreViewSet
from xyz_tablestore.store import Store


class UserSerializer(TableStoreSerializer):
    id = serializers.CharField(required=False)
    name = serializers.CharField()
    age = serializers.IntegerField(required=False)


class RESTTests(unittest.TestCase):
    def setUp(self):
        self.store = Mock(pks=['id'], primary_key_schema=[('id', 'STRING')], MAX_OFFSET=10000)
        self.store.name = 'users'
        self.store.get.return_value = {'id': 'a', 'name': 'Alice'}
        self.store.search.return_value = {'items': [{'id': 'a', 'name': 'Alice'}], 'total': 1, 'next_token': None}
        store = self.store
        class Users(TableStoreViewSet):
            serializer_class = UserSerializer
            filter_types = {'age': int}
            ordering_fields = ['age']
            def get_store(self, name=None):
                return store
        self.view = Users
        self.factory = APIRequestFactory()

    def request(self, method, path='/', data=None, pk=None):
        request = getattr(self.factory, method)(path, data=data, format='json')
        force_authenticate(request, user=SimpleNamespace(is_staff=True, is_authenticated=True))
        actions = {'get': 'retrieve' if pk is not None else 'list', 'post': 'create', 'put': 'update', 'patch': 'partial_update'}
        return self.view.as_view({method: actions[method]})(request, **({'pk': pk} if pk is not None else {}))

    def test_list_uses_search_and_serialization(self):
        result = self.request('get', '/?page=2&page_size=10&age__gte=18&ordering=-age')
        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.data['count'], 1)
        self.assertEqual(result.data['results'][0]['name'], 'Alice')
        self.store.search.assert_called_once_with({'age__gte': 18}, columns=None, sort_fields=[('age', SortOrder.DESC)], page_no=2, page_size=10, next_token=None)
        self.assertIn('page=1', result.data['previous'])

    def test_bad_pagination_and_filters_do_not_query(self):
        for query in ['page=0', 'page=x', 'page_size=101', 'page=102&page_size=100', 'next_token=bad!', 'next_token=YQ==&page=1', 'age__gte=bad', '_random=5', 'ordering=-secret']:
            with self.subTest(query=query):
                self.assertEqual(self.request('get', '/?' + query).status_code, 400)
        self.store.search.assert_not_called()

    def test_token_pagination(self):
        self.store.search.return_value['next_token'] = 'Yg=='
        result = self.request('get', '/?next_token=YQ==')
        self.assertEqual(result.status_code, 200)
        self.assertIsNone(self.store.search.call_args.kwargs['page_no'])
        self.assertIsNone(result.data['previous'])
        self.assertIn('next_token=Yg%3D%3D', result.data['next'])

    def test_missing_object_returns_404(self):
        self.store.get.return_value = None
        self.assertEqual(self.request('get', pk='missing').status_code, 404)

    def test_create_requires_keys_and_validates_fields(self):
        for data in [{'name': 'Alice'}, {'id': 'a', 'age': 'bad'}, []]:
            self.assertEqual(self.request('post', data=data).status_code, 400)
        self.store.get_or_create.assert_not_called()

    def test_create_returns_201_and_duplicate_is_rejected(self):
        self.store.get_or_create.return_value = ({'id': 'a', 'name': 'Alice'}, True)
        self.assertEqual(self.request('post', data={'id': 'a', 'name': 'Alice'}).status_code, 201)
        self.store.get_or_create.return_value = ({'id': 'a', 'name': 'Alice'}, False)
        self.assertEqual(self.request('post', data={'id': 'a', 'name': 'Alice'}).status_code, 400)

    def test_patch_updates_only_attributes_and_router_supports_it(self):
        result = self.request('patch', data={'age': 20}, pk='a')
        self.assertEqual(result.status_code, 200)
        args = self.store.client.update_row.call_args.args
        self.assertEqual(args[1].primary_key, [('id', 'a')])
        self.assertEqual(args[1].attribute_columns, {'put': [('age', 20)]})
        self.assertEqual(args[2].row_existence_expectation, RowExistenceExpectation.EXPECT_EXIST)
        router = SimpleRouter()
        router.register('users', self.view, basename='users')
        detail = next(url for url in router.urls if url.name == 'users-detail')
        self.assertEqual(detail.callback.actions['patch'], 'partial_update')

    def test_put_replaces_attributes_and_primary_keys_are_immutable(self):
        self.assertEqual(self.request('put', data={'name': 'Bob'}, pk='a').status_code, 200)
        row = self.store.client.put_row.call_args.args[1]
        self.assertEqual(row.primary_key, [('id', 'a')])
        self.assertEqual(row.attribute_columns, [('name', 'Bob')])
        self.assertEqual(self.request('patch', data={'id': 'b'}, pk='a').status_code, 400)
        self.store.client.update_row.assert_not_called()

    def test_object_permission_is_checked(self):
        from rest_framework.permissions import BasePermission
        class DenyObject(BasePermission):
            def has_object_permission(self, request, view, obj):
                return False
        self.view.permission_classes = [DenyObject]
        self.assertEqual(self.request('patch', data={'age': 20}, pk='a').status_code, 403)
        self.store.client.update_row.assert_not_called()

    def test_update_does_not_recreate_concurrently_deleted_row(self):
        self.store.client.update_row.side_effect = OTSServiceError(409, 'OTSConditionCheckFail', 'missing')
        self.assertEqual(self.request('patch', data={'age': 20}, pk='a').status_code, 404)

    def test_composite_integer_primary_key(self):
        self.store.pks = ['tenant', 'id']
        self.store.primary_key_schema = [('tenant', 'STRING'), ('id', 'INTEGER')]
        self.view.lookup_fields = {'tenant': 'tenant', 'id': 'pk'}
        request = self.factory.get('/')
        force_authenticate(request, user=SimpleNamespace(is_staff=True))
        result = self.view.as_view({'get': 'retrieve'})(request, tenant='team', pk='12')
        self.assertEqual(result.status_code, 200)
        self.store.get.assert_called_with({'tenant': 'team', 'id': 12})

    def test_store_search_handles_terminal_token(self):
        store = Store.__new__(Store)
        store.name, store.index_name = 'users', 'users_index'
        store.client = Mock()
        store.client.search.return_value = SimpleNamespace(rows=[], total_count=0, next_token=None)
        self.assertIsNone(store.search()['next_token'])
