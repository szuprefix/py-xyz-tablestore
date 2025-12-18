from .store import Store
import datetime
from .utils import json_schema

class Schema(Store):
    name = 'XYZ_STORE_SCHEMA'

    def guess(self, name, *args, **kwargs):
        st = Store(name=name)
        rs = {}
        for d in st.random_find(*args, **kwargs):
            rs.update(json_schema(d))
        self.upsert({'name': name}, {'guess': rs})
        return rs

    def desc(self, name, *args, **kwargs):
        d = self.collection.find_one({'name': name}, {'_id': 0})
        if not d or not d.get('guess'):
            self.guess(name, *args, **kwargs)
            d = self.collection.find_one({'name': name}, {'_id': 0})
        return d
