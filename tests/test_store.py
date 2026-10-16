import logging
import unittest
from unittest.mock import Mock

from tablestore import OTSServiceError, RowExistenceExpectation

from xyz_tablestore.store import Store


class GetOrCreateTests(unittest.TestCase):
    def setUp(self):
        self.store = Store.__new__(Store)
        self.store.pks = ['id']
        self.store.client = Mock()
        self.store.client.logger = logging.Logger('test-store')
        self.store.get = Mock(return_value=None)

    def test_existing_row_is_not_written(self):
        record = {'id': 'one', 'name': 'existing'}
        self.store.get.return_value = record
        self.assertEqual(
            self.store.get_or_create({'id': 'one'}, {'name': 'new'}),
            (record, False),
        )
        self.store.client.put_row.assert_not_called()

    def test_create_preserves_inputs_and_condition_values(self):
        cond = {'id': 'one', 'name': 'chosen'}
        defaults = {'id': 'other', 'name': 'default', 'tags': ['a'], 'empty': None}
        record, created = self.store.get_or_create(cond, defaults)
        self.assertTrue(created)
        self.assertEqual(record, {'id': 'one', 'name': 'chosen', 'tags': ['a']})
        self.assertEqual(defaults['id'], 'other')
        self.assertEqual(cond, {'id': 'one', 'name': 'chosen'})
        self.store.get.assert_called_once_with({'id': 'one'})
        condition = self.store.client.put_row.call_args.kwargs['condition']
        self.assertEqual(condition.row_existence_expectation, RowExistenceExpectation.EXPECT_NOT_EXIST)
        self.assertEqual(self.store.client.logger.filters, [])

    def test_concurrent_insert_returns_existing_row(self):
        record = {'id': 'one', 'name': 'winner'}
        self.store.get.side_effect = [None, record]
        self.store.client.put_row.side_effect = OTSServiceError(409, 'OTSConditionCheckFail', 'exists')
        self.assertEqual(self.store.get_or_create({'id': 'one'}), (record, False))
        self.assertEqual(self.store.client.logger.filters, [])

    def test_other_service_errors_propagate(self):
        error = OTSServiceError(500, 'OTSInternalServerError', 'failed')
        self.store.client.put_row.side_effect = error
        with self.assertRaises(OTSServiceError):
            self.store.get_or_create({'id': 'one'})
        self.assertEqual(self.store.client.logger.filters, [])

    def test_missing_composite_key_is_rejected_before_read(self):
        self.store.pks = ['tenant', 'id']
        with self.assertRaisesRegex(ValueError, 'Missing primary key field: tenant'):
            self.store.get_or_create({'id': 'one'})
        self.store.get.assert_not_called()


if __name__ == '__main__':
    unittest.main()
