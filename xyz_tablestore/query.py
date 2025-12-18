from copy import deepcopy
from .lookup import normalize_filter_condition
from django.db.models.query import QuerySet

MAX_GET_RESULTS = 21

class QuerySet:
    def __init__(self, collection):
        self.collection = collection
        self._query = {}
        self._sort = []
        self._limit = None
        self._skip = 0  # 为分页预留
        self._result_cache = None


    def __iter__(self):
        self._fetch_all()
        return iter(self._result_cache)

    def __len__(self):
        self._fetch_all()
        return len(self._result_cache)

    def __getitem__(self, k):
        """Retrieve an item or slice from the set of results."""
        if not isinstance(k, (int, slice)):
            raise TypeError(
                "QuerySet indices must be integers or slices, not %s."
                % type(k).__name__
            )
        if (isinstance(k, int) and k < 0) or (
            isinstance(k, slice)
            and (
                (k.start is not None and k.start < 0)
                or (k.stop is not None and k.stop < 0)
            )
        ):
            raise ValueError("Negative indexing is not supported.")

        if self._result_cache is not None:
            return self._result_cache[k]

        if isinstance(k, slice):
            qs = self._chain()
            qs._skip = 0 if k.start is None else int(k.start)
            qs._limit = k.stop - qs._skip if k.stop else None
            qs._fetch_all()
            rs = qs._result_cache
            return rs[:: k.step] if k.step else rs

        qs = self._chain()
        qs._skip = k
        qs._limit = 1
        qs._fetch_all()
        return qs._result_cache[0]

    def _chain(self):
        """创建新QuerySet实例，继承当前状态（关键！）"""
        new_qs = QuerySet(self.collection)
        # 深拷贝所有状态（避免修改原始对象）
        new_qs._query = deepcopy(self._query)
        new_qs._sort = self._sort.copy()
        new_qs._limit = self._limit
        new_qs._skip = self._skip
        return new_qs


    def filter(self, **kwargs):
        """正确实现：返回新对象，不修改self"""
        mongo_query = normalize_filter_condition(kwargs)
        new_query = {**self._query, **mongo_query}
        clone = self._chain()
        clone._query.update(new_query)
        return clone

    def exclude(self, **kwargs):
        """正确实现：返回新对象"""
        mongo_query = normalize_filter_condition(kwargs)
        # 转换为 $not + 条件
        exclude_query = {field: {"$not": cond} for field, cond in mongo_query.items()}
        new_query = {**self._query, **exclude_query} if self._query else exclude_query
        clone = self._chain()
        clone._query = new_query
        return clone

    def order_by(self, *fields):
        """正确实现：返回新对象"""
        clone = self._chain()
        clone._sort = []
        for field in fields:
            if field.startswith('-'):
                clone._sort.append((field[1:], -1))
            else:
                clone._sort.append((field, 1))
        return clone

    # limit、offset等方法同理，都要用_chain()
    def limit(self, n):
        clone = self._chain()
        clone._limit = n
        return clone

    def offset(self, n):
        clone = self._chain()
        clone._skip = n
        return clone

    # def all(self):
    #     return self._fetch_all()

    def _fetch_all(self):
        if self._result_cache is None:
            self._result_cache = list(self._execute_query())

    def _execute_query(self):
        """执行查询并返回结果列表"""
        cursor = self.collection.find(self._query)
        if self._sort:
            cursor = cursor.sort(self._sort)
        if self._skip:
            cursor = cursor.skip(self._skip)
        if self._limit:
            cursor = cursor.limit(self._limit)
        return cursor

    def get(self, *args, **kwargs):
        """
        Perform the query and return a single object matching the given
        keyword arguments.
        """
        clone = self.filter(*args, **kwargs)
        limit = MAX_GET_RESULTS
        clone.limit(limit)
        num = len(clone)
        if num == 1:
            return clone._result_cache[0]
        if not num:
            raise Exception(
                "%s matching query does not exist." % self.collection.name
            )
        raise Exception(
            "get() returned more than one %s -- it returned %s!"
            % (
                self.collection.name,
                num if not limit or num < limit else "more than %s" % (limit - 1),
            )
        )

    def first(self):
        """Return the first object of a query or None if no match is found."""
        queryset = self
        for obj in queryset[:1]:
            return obj

    def last(self):
        """Return the last object of a query or None if no match is found."""
        queryset = self
        for obj in queryset[:1]:
            return obj

    def count(self):
        if not self._query:
            return self.collection.estimated_document_count()
        return self.collection.count_documents(self._query)