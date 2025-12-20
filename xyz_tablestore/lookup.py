from tablestore import *

def ensure_list(v):
    if v is None:
        return []
    if isinstance(v, (list, tuple)):
        return list(v)
    return [v]

def build_tablestore_query(data, field_types=None, fields=None, search_fields=None):
    """
    将扁平查询参数转换为 Tablestore 多元索引的 Query 对象（BoolQuery）

    :param data: dict，如 {'age__gte': 18, 'status': 'active'}
    :param field_types: dict，字段名 -> 转换函数，如 {'age': int}
    :param fields: list，允许查询的字段白名单
    :param search_fields: list，用于全局搜索的字段列表（对应 data['search']）
    :return: Query 对象（通常是 BoolQuery）
    """
    if not data:
        return MatchAllQuery()
    field_types = field_types or {}
    fields = set(fields) if fields else None
    search_fields = search_fields or []

    must_queries = []      # AND 条件
    should_queries = []    # OR 条件（用于 search）
    must_not_queries = []  # NOT 条件

    # 1. 处理全局搜索（search）
    if search_fields:
        sv = data.get('search')
        if sv and isinstance(sv, str) and sv.strip():
            for fn in search_fields:
                if fields and fn not in fields:
                    continue
                # Tablestore 只支持通配符 * 和 ?，不支持正则
                pattern = sv.strip().replace('%', '*').replace('_', '?')
                if not pattern.endswith('*'):
                    pattern += '*'  # 默认前缀匹配
                should_queries.append(WildcardQuery(fn, pattern))

    # 2. 处理其他字段
    for key, value in data.items():
        if key == 'search':
            continue

        # 解析字段名和操作符
        parts = key.split('__')
        base_field = parts[0]
        op = parts[-1] if len(parts) > 1 else 'eq'

        # 字段白名单检查
        if fields and base_field not in fields:
            continue

        # 字段类型转换
        if not isinstance(value, dict):
            converter = field_types.get(base_field)
            if converter:
                try:
                    value = converter(value)
                except Exception:
                    continue  # 或抛异常，根据需求

        # 判断是否为 .not 否定
        is_not = False
        if base_field.endswith('.not'):
            is_not = True
            base_field = base_field[:-4]

        # 构建查询
        query = None
        if op == 'eq':
            query = TermQuery(base_field, value)
        elif op == 'ne':
            # ne 用 must_not 实现
            query = TermQuery(base_field, value)
            is_not = True  # 转为 not
        elif op == 'in':
            value_list = ensure_list(value)
            if value_list:
                query = TermsQuery(base_field, value_list)
        elif op == 'nin':
            value_list = ensure_list(value)
            if value_list:
                query = TermsQuery(base_field, value_list)
                is_not = True
        elif op in ('gt', 'gte', 'lt', 'lte'):
            range_args = {}
            if op == 'gt':
                range_args['gt'] = value
            elif op == 'gte':
                range_args['gte'] = value
            elif op == 'lt':
                range_args['lt'] = value
            elif op == 'lte':
                range_args['lte'] = value
            query = RangeQuery(base_field, **range_args)
        elif op in ('regex', 'wildcard'):
            pattern = str(value).replace('%', '*').replace('_', '?')
            if '*' not in pattern and '?' not in pattern:
                pattern += '*'  # 默认模糊
            query = WildcardQuery(base_field, pattern)
        elif op == 'exists':
            # exists=true 表示存在，false 表示不存在
            exists_flag = value not in ['0', 'false', False, 'False']
            query = ExistsQuery(base_field)
            if not exists_flag:
                is_not = True
        else:
            # 默认当作 eq 处理
            query = TermQuery(base_field, value)

        # 添加到对应列表
        if query:
            if is_not:
                must_not_queries.append(query)
            else:
                must_queries.append(query)

    # 3. 组合查询
    bool_clauses = {}
    if must_queries:
        bool_clauses['must'] = must_queries
    if should_queries:
        bool_clauses['should'] = should_queries
        bool_clauses['minimum_should_match'] = 1  # 至少匹配一个 should
    if must_not_queries:
        bool_clauses['must_not'] = must_not_queries

    if not bool_clauses:
        return MatchAllQuery()
    elif len(bool_clauses) == 1 and 'must' in bool_clauses and len(must_queries) == 1 and not should_queries and not must_not_queries:
        # 只有一个 must 查询，直接返回
        return must_queries[0]
    else:
        return BoolQuery(**bool_clauses)