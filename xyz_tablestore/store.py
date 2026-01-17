from datetime import datetime
import os, json, logging, math, base64, copy
from tablestore import *
from xyz_tablestore.lookup import build_tablestore_query
from tablestore import INF_MIN, INF_MAX, Direction
from .utils import encode, decode, dict2row, row2dict, get_client, map_encode


class Store:
    primary_key_schema = [('id', 'STRING')]
    name = 'test'
    MAX_OFFSET = 10000

    def __init__(self, name=None, index_name=None):
        self.client = get_client()
        if name:
            self.name = name
        self.index_name = index_name if index_name else f'{self.name}_index'
        self.pks = [f[0] for f in self.primary_key_schema]

    def create(self):
        table_meta = TableMeta(self.name, self.primary_key_schema)
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))

        self.client.create_table(table_meta, table_options, reserved_throughput)


    def get(self, cond):
        primary_key = list(cond.items())
        consumed, return_row, next_token = self.client.get_row(self.name, primary_key)
        return row2dict(return_row) if return_row else None

    def save(self, d):
        return self.upsert(d)

    def upsert(self, cond, put={}, set_on_insert={}, **kwargs):
        for k in self.pks:
            if k not in cond:
                raise ValueError(f"Missing primary key field: {k}")
        row = dict2row(dict(**cond,**put), self.pks)

        irow = Row(
            primary_key=row.primary_key,
            attribute_columns=copy.copy(row.attribute_columns)
        )
        if 'increment' in kwargs:
            irow.attribute_columns+=list(kwargs['increment'].items())
        try:
            return True, self.client.put_row(self.name, irow, condition=Condition(RowExistenceExpectation.EXPECT_NOT_EXIST))
        except OTSServiceError as e:
            if e.code == 'OTSConditionCheckFail':
                pass  # 继续更新
            else:
                raise

        # print(row)
        urow = Row(
            primary_key=row.primary_key,
            attribute_columns=dict(put=row.attribute_columns, **dict([(k, list(v.items())) for k,v in kwargs.items()]))
        )
        return False, self.client.update_row(self.name, urow, Condition(RowExistenceExpectation.IGNORE))


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
        rs = self.client.search(
            table_name=self.name,
            index_name=index_name or self.index_name,
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

        query = build_tablestore_query(query)

        sort = self._get_sort(sort_fields)

        if page_no is not None and next_token is not None:
            raise ValueError("Cannot use both page_no and token")

        search_q = SearchQuery(
            query=query,
            sort=sort,
            get_total_count=True
        )
        if next_token is not None:
            # ===== Token 分页（推荐用于深度分页）=====
            search_q.limit=page_size
            search_q.next_token=base64.urlsafe_b64decode(next_token.encode('ascii')) if isinstance(next_token, str) else next_token
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

            search_q.offset=offset
            search_q.limit=page_size

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
                index_name=index_name or self.index_name,
                search_query=search_q,
                columns_to_get=columns_to_get
            )

            items = []
            for item in rs.rows:
                items.append(row2dict(item))

            total = rs.total_count
            total_pages = math.ceil(total / page_size) if total > 0 else 1
            result = {
                "items": items,
                "total": total,
                "page_size": page_size,
                "next_token": base64.urlsafe_b64encode(rs.next_token).decode('ascii'),
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

    def sql_query(self, sql, **kwargs):
        rows, reserved, consumption = self.client.exe_sql_query(sql)
        return [row2dict(a) for a in rows]

    def find(self, *args, **kwargs):
        rs = self.search(*args, **kwargs)
        yield from rs['items']
        while rs['next_token']:
            rs = self.search(*args, **kwargs, next_token=rs['next_token'])
            yield from rs['items']


    def all(self, batch_size=100, columns=None):
        """
        全表遍历（主键顺序扫描）
        """
        if batch_size < 1:
            batch_size = 100
        if batch_size > 500:
            batch_size = 500

        # 起始主键：最小
        start_pk = [(pk, INF_MIN) for pk in self.pks]
        # 结束主键：最大（必须提供，不能是 None）
        end_pk = [(pk, INF_MAX) for pk in self.pks]

        while True:
            consumed, next_start_pk, rows, *_ = self.client.get_range(
                table_name=self.name,
                direction=Direction.FORWARD,
                inclusive_start_primary_key=start_pk,
                exclusive_end_primary_key=end_pk,
                max_version=1,
                limit=batch_size,
                columns_to_get=columns if columns else None
            )

            if not rows:
                break

            for row in rows:
                yield row2dict(row)

            # 扫描结束
            if not next_start_pk:
                break

            # 用 SDK 返回的 next_start_pk 继续
            start_pk = next_start_pk
