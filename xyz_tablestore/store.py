import os, json, logging, math
from tablestore import *
from .lookup import build_tablestore_query

def get_client(
        endpoint=os.getenv('OTS_ENDPOINT', 'http://dbs-maxday.us-west-1.vpc.ots.aliyuncs.com'),
        instance_name=os.getenv('OTS_DB', 'maxday'),
        access_key_id=os.getenv('OTS_KEY_ID'),
        access_key_secret=os.getenv('OTS_KEY_SECRET')
):
    return OTSClient(endpoint, access_key_id, access_key_secret, instance_name)


class Store:
    primary_key_schema = [('id', 'STRING')]
    name = 'test'
    MAX_OFFSET = 10000

    def __init__(self, name=None):
        self.client = get_client()
        if name:
            self.name = name
        self.pks = [f[0] for f in self.primary_key_schema]

    def create(self):
        table_meta = TableMeta(self.name, self.primary_key_schema)
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))

        self.client.create_table(table_meta, table_options, reserved_throughput)

    def encode(self, v):
        if isinstance(v, (list, tuple, dict)):
            return json.dumps(v)
        return v

    def decode(self, v):
        if isinstance(v, str):
            if (v.startswith('[') and v.endswith(']')) or (v.startswith('{') and v.endswith('}')):
                try:
                    v = json.loads(v)
                except Exception as e:
                    logging.warning(f'json error:{e}')
                return v
        return v

    def dict2row(self, d):
        pfs = []
        fs = []
        for k, v in d.items():
            if v is None:
                continue
            if k in self.pks:
                pfs.append((k, v))
            else:
                fs.append((k, self.encode(v)))
        return Row(pfs, fs)

    def row2dict(self, row):
        d = dict()
        r = row if isinstance(row, tuple) else [row.primary_key, row.attribute_columns]
        for s in r:
            for f in s:
                d[f[0]] = self.decode(f[1])
        return d

    def get(self, cond):
        primary_key = list(cond.items())
        consumed, return_row, next_token = self.client.get_row(self.name, primary_key)
        return self.row2dict(return_row) if return_row else None

    def save(self, d):
        row = self.dict2row(d)
        self.client.put_row(self.name, row)

    def upsert(self, d):
        row = self.dict2row(d)
        condition = Condition(RowExistenceExpectation.IGNORE)  # 忽略是否存在，总是执行
        self.client.update_row(self.name, row, condition)

    def _get_sort(self, sort_fields):
        if not sort_fields:
            return None
        return Sort(sorters=[
            FieldSort(field, order=order) for field, order in sort_fields
        ])

    def count(self, query={}, index_name=None,):
        query = build_tablestore_query(query)
        search_q = SearchQuery(
            query=query,
            get_total_count=True
        )
        if not index_name:
            index_name = f'{self.name}_index'
        rs = self.client.search(
            table_name=self.name,
            index_name=index_name,
            search_query=search_q,
            columns_to_get=ColumnsToGet(return_type=ColumnReturnType.NONE)
        )
        return rs.total_count

    def search(
            self,
            query=None,
            columns=None,
            sort_fields=None,  # 排序列，如 [('age', SortOrder.ASC)]
            page_no=None,
            page_size=10,
            next_token=None,
            index_name=None,
            max_page_no_limit=True
    ):
        if page_size < 1:
            page_size = 10
        if page_size > 100:
            page_size = 100

        query = build_tablestore_query(query)

        sort = self._get_sort(sort_fields)

        if page_no is not None and next_token is not None:
            raise ValueError("Cannot use both page_no and token")

        if next_token is not None:
            # ===== Token 分页（推荐用于深度分页）=====
            search_q = SearchQuery(
                query=query,
                sort=sort,
                limit=page_size,
                token=next_token,
                get_total_count=True
            )
        else:
            # ===== PageNo 分页（仅用于浅层）=====
            if page_no is None:
                page_no = 1
            if page_no < 1:
                page_no = 1

            offset = (page_no - 1) * page_size

            if max_page_no_limit and offset > self.MAX_OFFSET:
                raise ValueError(
                    f"Page number too large. Max allowed page_no is "
                    f"{self.MAX_OFFSET // page_size + 1} (offset <= {self.MAX_OFFSET})"
                )

            search_q = SearchQuery(
                query=query,
                sort=sort,
                offset=offset,
                limit=page_size,
                get_total_count=True
            )


        if not index_name:
            index_name = f'{self.name}_index'

        if columns:
            columns_to_get = ColumnsToGet(
                return_type=ColumnReturnType.SPECIFIED,
                column_names=columns
            )
        else:
            columns_to_get = ColumnsToGet(return_type=ColumnReturnType.ALL)

        # 执行搜索
        try:
            rs = self.client.search(
                table_name=self.name,
                index_name=index_name,
                search_query=search_q,
                columns_to_get=columns_to_get
            )

            items = []
            for item in rs.rows:
                items.append(self.row2dict(item))

            total = rs.total_count
            total_pages = math.ceil(total / page_size) if total > 0 else 1
            result = {
                "items": items,
                "total": total,
                "page_size": page_size,
                "next_token": rs.next_token,
                "has_more": bool(rs.next_token),
                "total_pages": total_pages,
            }

            if next_token is None:
                # 当前是 pageNo 模式
                result.update({
                    "page_no": page_no,
                    "has_prev": page_no > 1,
                    "has_next": page_no < total_pages,
                    "can_jump_to_page": offset <= self.MAX_OFFSET  # 是否允许继续用 pageNo
                })
            else:
                # token 模式不提供 page_no（因为无法反推）
                result["page_no"] = None
                result["can_jump_to_page"] = False

            return result

        except Exception as e:
            print(f"Search error: {e}")
            raise
    find = search

    def query_by_index(
            self,
            index_query,
            columns=None,
            sort_fields=None,
            limit=100,
            index_name=None
    ):
        """
        通过二级索引查询（不使用主键）

        :param index_query: dict / Query，由 build_tablestore_query 处理
        :param columns: 返回字段列表
        :param sort_fields: [('field', SortOrder.ASC)]
        :param limit: 返回条数
        :param index_name: 指定索引名
        """
        if limit < 1:
            limit = 10
        if limit > 100:
            limit = 100

        query = build_tablestore_query(index_query)
        sort = self._get_sort(sort_fields)

        search_q = SearchQuery(
            query=query,
            sort=sort,
            limit=limit,
            get_total_count=False
        )

        if not index_name:
            index_name = f'{self.name}_index'

        if columns:
            columns_to_get = ColumnsToGet(
                return_type=ColumnReturnType.SPECIFIED,
                column_names=columns
            )
        else:
            columns_to_get = ColumnsToGet(
                return_type=ColumnReturnType.ALL
            )

        rs = self.client.search(
            table_name=self.name,
            index_name=index_name,
            search_query=search_q,
            columns_to_get=columns_to_get
        )

        return [self.row2dict(row) for row in rs.rows]

    def all(self, batch_size=100):
        """
        全表遍历（主键顺序扫描）
        :return: generator
        """
        if batch_size < 1:
            batch_size = 100
        if batch_size > 500:
            batch_size = 500

        start_pk = None

        while True:
            consumed, rows, next_start_pk = self.client.get_range(
                table_name=self.name,
                direction=Direction.FORWARD,
                inclusive_start_primary_key=start_pk,
                exclusive_end_primary_key=None,
                max_version=1,
                limit=batch_size
            )

            if not rows:
                break

            for row in rows:
                yield self.row2dict(row)

            if not next_start_pk:
                break

            start_pk = next_start_pk
