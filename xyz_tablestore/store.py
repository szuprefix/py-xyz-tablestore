import os, json, logging
from tablestore import *


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
        r =row if isinstance(row, tuple) else [row.primary_key, row.attribute_columns]
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

    def search(
            self,
            query=None,  # 查询条件，如 TermQuery('status', 'active')
            sort_fields=None,  # 排序列，如 [('age', SortOrder.ASC)]
            limit=10,
            next_token=None,
            index_name=None
    ):
        if query is None:
            query = MatchAllQuery()

        # 构造排序
        sort = None
        if sort_fields:
            sort = Sort(sorters=[
                FieldSort(field, order=order) for field, order in sort_fields
            ])

        if not index_name:
            index_name = f'{self.name}_index'

        # 执行搜索
        try:
            search_response = self.client.search(
                table_name=self.name,
                index_name=index_name,
                search_query=SearchQuery(
                    query=query,
                    sort=sort,
                    limit=limit,
                    get_total_count=True,
                    next_token=next_token
                ),
                columns_to_get=ColumnsToGet(return_type=ColumnReturnType.ALL)
            )

            items = []
            for item in search_response.rows:
                items.append(self.row2dict(item))

            return {
                "items": items,
                "next_token": search_response.next_token,
                "total_count": search_response.total_count
            }

        except Exception as e:
            print(f"Search error: {e}")
            raise
