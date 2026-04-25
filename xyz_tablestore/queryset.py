from typing import List, Dict, Any, Optional, Union, Tuple
from dataclasses import dataclass, field
from enum import Enum
import re


class Operator(Enum):
    """查询操作符映射"""
    EXACT = "="
    IEXACT = "LIKE"
    CONTAINS = "LIKE"
    ICONTAINS = "LIKE"
    GT = ">"
    GTE = ">="
    LT = "<"
    LTE = "<="
    IN = "IN"
    ISNULL = "IS NULL"
    RANGE = "BETWEEN"
    STARTSWITH = "LIKE"
    ISTARTSWITH = "LIKE"
    ENDSWITH = "LIKE"
    IENDSWITH = "LIKE"


@dataclass
class Q:
    """查询条件对象，支持 & (AND) 和 | (OR) 操作"""
    condition: str = ""
    params: List[Any] = field(default_factory=list)
    connector: str = "AND"
    negated: bool = False
    children: List['Q'] = field(default_factory=list)

    def __init__(self, *args, _connector: str = "AND", _negated: bool = False, **kwargs):
        self.children = []
        self.connector = _connector
        self.negated = _negated
        self.condition = ""
        self.params = []

        # 处理 Q 对象组合
        if args:
            self.children = list(args)
        # 处理 kwargs 条件
        elif kwargs:
            self._parse_kwargs(kwargs)

    def _parse_kwargs(self, kwargs: Dict[str, Any]):
        """解析 kwargs 为 SQL 条件"""
        conditions = []
        params = []

        for key, value in kwargs.items():
            # 解析字段名和操作符，如 name__icontains
            if '__' in key:
                field, op = key.rsplit('__', 1)
            else:
                field, op = key, 'exact'

            op_enum = Operator(op.upper()) if hasattr(Operator, op.upper()) else Operator.EXACT

            if op == 'isnull':
                if value:
                    conditions.append(f"{field} IS NULL")
                else:
                    conditions.append(f"{field} IS NOT NULL")
            elif op in ['in', 'range']:
                if op == 'range':
                    conditions.append(f"{field} BETWEEN ? AND ?")
                    params.extend([value[0], value[1]])
                else:
                    placeholders = ', '.join(['?' for _ in value])
                    conditions.append(f"{field} IN ({placeholders})")
                    params.extend(value)
            elif op in ['contains', 'icontains']:
                conditions.append(f"{field} LIKE ?")
                params.append(f"%{value}%")
            elif op in ['startswith', 'istartswith']:
                conditions.append(f"{field} LIKE ?")
                params.append(f"{value}%")
            elif op in ['endswith', 'iendswith']:
                conditions.append(f"{field} LIKE ?")
                params.append(f"%{value}")
            elif op == 'iexact':
                conditions.append(f"LOWER({field}) = LOWER(?)")
                params.append(value)
            else:
                conditions.append(f"{field} {op_enum.value} ?")
                params.append(value)

        self.condition = f" {self.connector} ".join(conditions)
        self.params = params

    def __and__(self, other: 'Q') -> 'Q':
        return Q(self, other, _connector="AND")

    def __or__(self, other: 'Q') -> 'Q':
        return Q(self, other, _connector="OR")

    def __invert__(self) -> 'Q':
        return Q(self, _connector=self.connector, _negated=not self.negated)

    def to_sql(self) -> Tuple[str, List[Any]]:
        """递归生成 SQL WHERE 子句"""
        if self.children:
            child_sqls = []
            all_params = []

            for child in self.children:
                sql, params = child.to_sql()
                if sql:
                    child_sqls.append(f"({sql})")
                    all_params.extend(params)

            final_sql = f" {self.connector} ".join(child_sqls)
            if self.negated:
                final_sql = f"NOT ({final_sql})"
            return final_sql, all_params

        if self.negated and self.condition:
            return f"NOT ({self.condition})", self.params

        return self.condition, self.params


class QuerySet:
    """类似 Django 的 QuerySet 实现"""

    def __init__(self, store):
        self.store = store
        self.table_name = store.name

        # 查询状态
        self._filters: List[Q] = []
        self._excludes: List[Q] = []
        self._order_by: List[str] = []
        self._limit: Optional[int] = None
        self._offset: Optional[int] = None
        self._select: List[str] = ["*"]
        self._distinct: bool = False
        self._aggregate: Optional[Dict[str, str]] = None

        # 结果缓存
        self._result_cache: Optional[List[Dict]] = None
        self._count_cache: Optional[int] = None

    def _clone(self) -> 'QuerySet':
        """创建 QuerySet 副本，支持链式调用"""
        clone = QuerySet(self.store)
        clone._filters = self._filters.copy()
        clone._excludes = self._excludes.copy()
        clone._order_by = self._order_by.copy()
        clone._limit = self._limit
        clone._offset = self._offset
        clone._select = self._select.copy()
        clone._distinct = self._distinct
        clone._aggregate = self._aggregate
        return clone

    def filter(self, *args, **kwargs) -> 'QuerySet':
        """添加 AND 条件"""
        clone = self._clone()
        if args:
            clone._filters.extend(args)
        if kwargs:
            clone._filters.append(Q(**kwargs))
        return clone

    def exclude(self, *args, **kwargs) -> 'QuerySet':
        """添加排除条件 (NOT)"""
        clone = self._clone()
        if args:
            clone._excludes.extend(args)
        if kwargs:
            clone._excludes.append(Q(**kwargs))
        return clone

    def get(self, *args, **kwargs) -> Dict:
        """获取单条记录，结果不唯一或不存在时抛出异常"""
        clone = self.filter(*args, **kwargs)
        clone._limit = 2  # 检查是否有多条
        results = list(clone)

        if len(results) == 0:
            raise DoesNotExist(f"{self.table_name} 匹配查询的对象不存在")
        if len(results) > 1:
            raise MultipleObjectsReturned(f"期望 1 条，但找到 {len(results)} 条")

        return results[0]

    def first(self) -> Optional[Dict]:
        try:
            return self[0]  # __getitem__(0) 会自动设置 limit=1
        except IndexError:
            return None

    def last(self) -> Optional[Dict]:
        """返回最后一条记录（需要排序）"""
        if not self._order_by:
            # 默认按主键倒序，假设主键为 id
            return self.order_by('-id').first()
        # 反转排序方向
        reversed_order = []
        for field in self._order_by:
            if field.startswith('-'):
                reversed_order.append(field[1:])
            else:
                reversed_order.append(f"-{field}")
        return self.order_by(*reversed_order).first()

    def order_by(self, *fields) -> 'QuerySet':
        """排序：'-field' 倒序，'field' 正序"""
        clone = self._clone()
        clone._order_by = list(fields)
        return clone

    def values(self, *fields) -> 'QuerySet':
        """指定查询字段"""
        clone = self._clone()
        clone._select = list(fields) if fields else ["*"]
        return clone

    def distinct(self) -> 'QuerySet':
        """去重"""
        clone = self._clone()
        clone._distinct = True
        return clone

    def limit(self, n: int) -> 'QuerySet':
        """限制返回数量"""
        clone = self._clone()
        clone._limit = n
        return clone

    def offset(self, n: int) -> 'QuerySet':
        """跳过前 n 条"""
        clone = self._clone()
        clone._offset = n
        return clone

    def count(self) -> int:
        """返回数量（使用 COUNT 聚合）"""
        if self._count_cache is not None:
            return self._count_cache

        # 构建 COUNT 查询
        clone = self._clone()
        clone._aggregate = {"count": "COUNT(*)"}
        clone._select = ["COUNT(*) as count"]

        result = clone._execute()
        self._count_cache = result[0]['count'] if result else 0
        return self._count_cache

    def exists(self) -> bool:
        """检查是否存在"""
        return self.limit(1).count() > 0

    def aggregate(self, **kwargs) -> Dict:
        """聚合查询，如 aggregate(sum_price=Sum('price'))"""
        # 这里简化实现，实际可支持 Sum, Avg, Max, Min 等
        select_fields = []
        for alias, expr in kwargs.items():
            if isinstance(expr, str):
                select_fields.append(f"{expr} as {alias}")
            else:
                # 假设 expr 是聚合函数对象
                select_fields.append(f"{expr.to_sql()} as {alias}")

        clone = self._clone()
        clone._select = select_fields
        result = clone._execute()
        return result[0] if result else {}

    def __iter__(self):
        """使 QuerySet 可迭代"""
        if self._result_cache is None:
            self._result_cache = self._execute()
        return iter(self._result_cache)

    def __len__(self):
        return len(list(self))

    def __getitem__(self, k):
        """支持切片操作 qs[0], qs[1:10]"""
        if isinstance(k, int):
            if k < 0:
                raise ValueError("不支持负数索引")
            clone = self._clone()
            clone._offset = k
            clone._limit = 1
            results = clone._execute()
            return results[0] if results else None

        if isinstance(k, slice):
            clone = self._clone()
            start, stop, step = k.indices(999999)  # 大数作为上限
            if step != 1:
                raise ValueError("不支持步长不为 1 的切片")
            clone._offset = start
            clone._limit = stop - start
            return clone

        raise TypeError("索引必须是整数或切片")

    def _build_where(self) -> Tuple[str, List[Any]]:
        """构建 WHERE 子句"""
        conditions = []
        params = []

        # 处理 filter
        for q in self._filters:
            sql, p = q.to_sql()
            if sql:
                conditions.append(f"({sql})")
                params.extend(p)

        # 处理 exclude (NOT)
        for q in self._excludes:
            sql, p = q.to_sql()
            if sql:
                conditions.append(f"NOT ({sql})")
                params.extend(p)

        where_clause = " AND ".join(conditions) if conditions else "1=1"
        return where_clause, params

    def _build_sql(self) -> Tuple[str, List[Any]]:
        """构建完整 SQL 语句"""
        # SELECT 字段
        select_clause = ", ".join(self._select)
        if self._distinct:
            select_clause = f"DISTINCT {select_clause}"

        # WHERE 条件
        where_clause, params = self._build_where()

        # 构建基础 SQL
        sql = f"SELECT {select_clause} FROM {self.table_name} WHERE {where_clause}"

        # ORDER BY
        if self._order_by:
            order_parts = []
            for field in self._order_by:
                if field.startswith('-'):
                    order_parts.append(f"{field[1:]} DESC")
                elif field.startswith('?'):
                    order_parts.append("RANDOM()")  # 随机排序
                else:
                    order_parts.append(f"{field} ASC")
            sql += f" ORDER BY {', '.join(order_parts)}"

        # LIMIT 和 OFFSET
        if self._limit is not None:
            sql += f" LIMIT {self._limit}"
        if self._offset is not None:
            sql += f" OFFSET {self._offset}"

        return sql, params

    def _execute(self) -> List[Dict]:
        """执行 SQL 查询"""
        sql, params = self._build_sql()

        # 参数化查询（TableStore SQL 支持 ? 占位符）
        # 注意：TableStore 的 SQL 接口可能有不同的参数绑定方式
        # 这里假设支持标准 ? 占位符，或者你需要适配为 :param 风格

        # 如果有参数，需要格式化到 SQL 中（TableStore SQL 可能不支持预编译参数）
        # 这里做一个简单的参数替换（生产环境请使用安全的参数绑定）
        final_sql = self._substitute_params(sql, params)

        return self.store.sql_query(final_sql)

    def _substitute_params(self, sql: str, params: List[Any]) -> str:
        """将 ? 占位符替换为实际值（注意 SQL 注入防护）"""

        # 简单实现：处理字符串转义
        def escape(val):
            if isinstance(val, str):
                # 防止 SQL 注入的基础转义
                val = val.replace("'", "''")
                return f"'{val}'"
            elif isinstance(val, bool):
                return 'TRUE' if val else 'FALSE'
            elif val is None:
                return 'NULL'
            return str(val)

        # 替换 ? 占位符
        parts = sql.split('?')
        if len(parts) - 1 != len(params):
            raise ValueError(f"占位符数量 ({len(parts) - 1}) 与参数数量 ({len(params)}) 不匹配")

        result = parts[0]
        for i, param in enumerate(params):
            result += escape(param) + parts[i + 1]

        return result

    def raw(self, sql: str, params: List[Any] = None) -> List[Dict]:
        """执行原始 SQL"""
        if params:
            sql = self._substitute_params(sql, params)
        return self.store.sql_query(sql)


# 异常类
class DoesNotExist(Exception):
    pass


class MultipleObjectsReturned(Exception):
    pass


# 聚合函数辅助类
class Sum:
    def __init__(self, field: str):
        self.field = field

    def to_sql(self) -> str:
        return f"SUM({self.field})"


class Avg:
    def __init__(self, field: str):
        self.field = field

    def to_sql(self) -> str:
        return f"AVG({self.field})"


class Count:
    def __init__(self, field: str = "*"):
        self.field = field

    def to_sql(self) -> str:
        return f"COUNT({self.field})"


class Max:
    def __init__(self, field: str):
        self.field = field

    def to_sql(self) -> str:
        return f"MAX({self.field})"


class Min:
    def __init__(self, field: str):
        self.field = field

    def to_sql(self) -> str:
        return f"MIN({self.field})"


# 在你的 Store 类中添加这个方法
class TableStoreManager:
    """TableStore 管理类，包含 QuerySet 支持"""

    def __init__(self, client):
        self.client = client

    def sql_query(self, sql, **kwargs):
        """你原有的查询方法"""
        rows, reserved, consumption = self.client.exe_sql_query(sql)
        return [self._row2dict(a) for a in rows]

    def _row2dict(self, row):
        """将 TableStore 行转换为字典"""
        # 根据你的实际 row2dict 实现调整
        return row  # 占位

    def table(self, table_name: str) -> QuerySet:
        """获取 QuerySet 入口"""
        return QuerySet(self, table_name)

    # 快捷方法
    def all(self, table_name: str) -> QuerySet:
        return self.table(table_name)

    def filter(self, table_name: str, **kwargs) -> QuerySet:
        return self.table(table_name).filter(**kwargs)

    def get(self, table_name: str, **kwargs) -> Dict:
        return self.table(table_name).get(**kwargs)


# ============ 使用示例 ============

"""
# 初始化（假设你的 Store 类已有 client）
store = TableStoreManager(client)

# 基础查询
users = store.table("users").filter(age__gt=18).all()

# 链式调用
results = (store.table("orders")
    .filter(status="paid", created_at__gt="2024-01-01")
    .exclude(is_deleted=True)
    .order_by("-created_at", "amount")
    .values("id", "user_id", "amount")
    .limit(10)
    .offset(20))

# 复杂 Q 对象查询
from datetime import datetime
q1 = Q(status="pending") | Q(status="processing")
q2 = Q(created_at__lt=datetime.now())
q3 = Q(amount__gt=100)

results = store.table("orders").filter(q1 & q2).exclude(q3).order_by("-id")

# 获取单条
user = store.table("users").get(id=123)

# 切片
recent = store.table("logs").order_by("-timestamp")[:10]  # 最近10条
page2 = store.table("products")[10:20]  # 分页

# 聚合
stats = store.table("orders").filter(status="completed").aggregate(
    total_amount=Sum("amount"),
    avg_amount=Avg("amount"),
    max_amount=Max("amount"),
    count=Count()
)

# 检查存在
if store.table("users").filter(email="test@example.com").exists():
    print("用户已存在")

# 计数
pending_count = store.table("orders").filter(status="pending").count()

# 原始 SQL（保留灵活性）
raw_results = store.table("users").raw(
    "SELECT * FROM users WHERE id IN (?, ?)", 
    [1, 2, 3]
)
"""