"""
TableStore QuerySet - Django ORM 风格的查询接口

使用示例:
    from tablestore import OTSClient

    # 定义模型
    class User(TableStoreModel):
        class Meta:
            table_name = 'users'
            primary_keys = ['user_id']

        user_id = Field()
        name = Field()
        age = Field()
        email = Field()

    # 初始化
    client = OTSClient('endpoint', 'access_id', 'access_key', 'instance')
    User.objects.client = client

    # 查询
    users = User.objects.filter(age__gte=18).filter(name__contains='张').order_by('-age')[:10]
    for user in users:
        print(user.name, user.age)
"""

from typing import Any, List, Dict, Optional, Union, Tuple
from copy import deepcopy
import operator


class Field:
    """字段定义"""

    def __init__(self, default=None, primary_key=False):
        self.default = default
        self.primary_key = primary_key
        self.name = None

    def __set_name__(self, owner, name):
        self.name = name


class QuerySet:
    """类似 Django 的 QuerySet 实现"""

    # 查询操作符映射
    OPERATORS = {
        'exact': operator.eq,
        'gt': operator.gt,
        'gte': operator.ge,
        'lt': operator.lt,
        'lte': operator.le,
        'contains': lambda a, b: b in str(a),
        'startswith': lambda a, b: str(a).startswith(b),
        'endswith': lambda a, b: str(a).endswith(b),
        'in': lambda a, b: a in b,
    }

    def __init__(self, model_class, client=None):
        self.model_class = model_class
        self.client = client
        self._filters = []
        self._order_by = []
        self._limit = None
        self._offset = 0
        self._cache = None
        self._fetched = False

    def _clone(self):
        """克隆 QuerySet"""
        clone = QuerySet(self.model_class, self.client)
        clone._filters = self._filters.copy()
        clone._order_by = self._order_by.copy()
        clone._limit = self._limit
        clone._offset = self._offset
        return clone

    def filter(self, **kwargs):
        """过滤查询

        支持的查询操作符:
            exact: 精确匹配 (默认)
            gt: 大于
            gte: 大于等于
            lt: 小于
            lte: 小于等于
            contains: 包含
            startswith: 开头匹配
            endswith: 结尾匹配
            in: 在列表中

        例如:
            User.objects.filter(age__gte=18)
            User.objects.filter(name__contains='张')
        """
        clone = self._clone()
        for key, value in kwargs.items():
            clone._filters.append((key, value))
        return clone

    def exclude(self, **kwargs):
        """排除查询"""
        clone = self._clone()
        for key, value in kwargs.items():
            clone._filters.append((f'not_{key}', value))
        return clone

    def order_by(self, *fields):
        """排序

        例如:
            User.objects.order_by('age')  # 升序
            User.objects.order_by('-age')  # 降序
        """
        clone = self._clone()
        clone._order_by = list(fields)
        return clone

    def get(self, **kwargs):
        """获取单个对象"""
        results = list(self.filter(**kwargs))
        if len(results) == 0:
            raise self.model_class.DoesNotExist(
                f"{self.model_class.__name__} matching query does not exist"
            )
        if len(results) > 1:
            raise self.model_class.MultipleObjectsReturned(
                f"get() returned more than one {self.model_class.__name__}"
            )
        return results[0]

    def first(self):
        """获取第一个对象"""
        results = list(self[:1])
        return results[0] if results else None

    def last(self):
        """获取最后一个对象"""
        results = list(self)
        return results[-1] if results else None

    def count(self):
        """计数"""
        return len(list(self))

    def exists(self):
        """检查是否存在"""
        return self.count() > 0

    def all(self):
        """获取所有对象"""
        return self._clone()

    def __getitem__(self, key):
        """支持切片操作"""
        clone = self._clone()
        if isinstance(key, slice):
            start = key.start or 0
            stop = key.stop
            clone._offset = start
            if stop is not None:
                clone._limit = stop - start
        elif isinstance(key, int):
            if key < 0:
                # 负索引需要先获取所有数据
                results = list(self)
                return results[key]
            clone._offset = key
            clone._limit = 1
            results = list(clone)
            return results[0] if results else None
        return clone

    def _fetch(self):
        """从 TableStore 获取数据"""
        if self._fetched:
            return self._cache

        if not self.client:
            raise ValueError("Client not set. Use Model.objects.client = client")

        # 这里需要根据实际的 TableStore API 来实现
        # 以下是模拟实现
        self._cache = self._fetch_from_tablestore()
        self._fetched = True
        return self._cache

    def _fetch_from_tablestore(self):
        """从 TableStore 获取数据的实际实现

        注意: 这是一个简化的实现框架
        实际使用时需要根据 TableStore 的 API 来调整
        """
        from tablestore import INF_MIN, INF_MAX
        from tablestore import Direction, ColumnCondition, CompositeColumnCondition, LogicalOperator

        table_name = self.model_class._meta['table_name']
        primary_keys = self.model_class._meta['primary_keys']

        # 构建主键范围
        # 注意: TableStore 的查询需要指定主键范围
        # 这里简化处理,实际应用需要更复杂的逻辑
        inclusive_start_primary_key = [(pk, INF_MIN) for pk in primary_keys]
        exclusive_end_primary_key = [(pk, INF_MAX) for pk in primary_keys]

        # 构建列条件
        column_condition = self._build_column_condition()

        # 执行范围查询
        consumed, next_start_primary_key, row_list, next_token = self.client.get_range(
            table_name,
            Direction.FORWARD,
            inclusive_start_primary_key,
            exclusive_end_primary_key,
            columns_to_get=[],  # 获取所有列
            limit=None,
            column_filter=column_condition,
            max_version=1
        )

        # 转换为模型实例
        results = []
        for primary_key, attributes in row_list:
            instance = self.model_class()

            # 设置主键
            for pk_name, pk_value in primary_key:
                setattr(instance, pk_name, pk_value)

            # 设置属性
            for attr_name, attr_value, _ in attributes:
                setattr(instance, attr_name, attr_value)

            results.append(instance)

        # 应用内存过滤
        results = self._apply_filters(results)

        # 应用排序
        results = self._apply_ordering(results)

        # 应用分页
        if self._offset:
            results = results[self._offset:]
        if self._limit is not None:
            results = results[:self._limit]

        return results

    def _build_column_condition(self):
        """构建列条件"""
        if not self._filters:
            return None

        from tablestore import ColumnCondition, CompositeColumnCondition, LogicalOperator
        from tablestore import SingleColumnCondition, ComparatorType

        conditions = []
        for filter_key, value in self._filters:
            # 解析过滤键
            if '__' in filter_key:
                field_name, lookup = filter_key.rsplit('__', 1)
            else:
                field_name, lookup = filter_key, 'exact'

            # 映射到 TableStore 的比较类型
            if lookup == 'exact':
                comparator = ComparatorType.EQUAL
            elif lookup == 'gt':
                comparator = ComparatorType.GREATER_THAN
            elif lookup == 'gte':
                comparator = ComparatorType.GREATER_EQUAL
            elif lookup == 'lt':
                comparator = ComparatorType.LESS_THAN
            elif lookup == 'lte':
                comparator = ComparatorType.LESS_EQUAL
            else:
                # 其他复杂条件需要在内存中过滤
                continue

            condition = SingleColumnCondition(field_name, value, comparator, pass_if_missing=False)
            conditions.append(condition)

        if not conditions:
            return None

        if len(conditions) == 1:
            return conditions[0]

        # 组合多个条件
        composite = CompositeColumnCondition(LogicalOperator.AND)
        for cond in conditions:
            composite.add_sub_condition(cond)
        return composite

    def _apply_filters(self, results):
        """在内存中应用过滤条件"""
        for filter_key, value in self._filters:
            is_negated = filter_key.startswith('not_')
            if is_negated:
                filter_key = filter_key[4:]

            if '__' in filter_key:
                field_name, lookup = filter_key.rsplit('__', 1)
            else:
                field_name, lookup = filter_key, 'exact'

            if lookup not in self.OPERATORS:
                continue

            op_func = self.OPERATORS[lookup]
            filtered = []
            for obj in results:
                field_value = getattr(obj, field_name, None)
                try:
                    match = op_func(field_value, value)
                    if is_negated:
                        match = not match
                    if match:
                        filtered.append(obj)
                except (TypeError, AttributeError):
                    continue
            results = filtered

        return results

    def _apply_ordering(self, results):
        """应用排序"""
        if not self._order_by:
            return results

        for field in reversed(self._order_by):
            reverse = field.startswith('-')
            field_name = field[1:] if reverse else field
            results = sorted(
                results,
                key=lambda obj: getattr(obj, field_name, None) or '',
                reverse=reverse
            )

        return results

    def __iter__(self):
        """迭代器"""
        return iter(self._fetch())

    def __len__(self):
        """长度"""
        return len(self._fetch())

    def __repr__(self):
        return f"<QuerySet [{', '.join(repr(obj) for obj in self._fetch()[:5])}]>"


class Manager:
    """模型管理器"""

    def __init__(self):
        self.client = None

    def __get__(self, instance, owner):
        if instance is not None:
            raise AttributeError("Manager isn't accessible via instances")
        return QuerySet(owner, self.client)


class TableStoreModelMeta(type):
    """模型元类"""

    def __new__(mcs, name, bases, attrs):
        # 收集字段
        fields = {}
        for key, value in list(attrs.items()):
            if isinstance(value, Field):
                fields[key] = value

        # 添加 Meta 信息
        meta = attrs.get('Meta')
        if meta:
            attrs['_meta'] = {
                'table_name': getattr(meta, 'table_name', name.lower()),
                'primary_keys': getattr(meta, 'primary_keys', ['id']),
            }
        else:
            attrs['_meta'] = {
                'table_name': name.lower(),
                'primary_keys': ['id'],
            }

        attrs['_fields'] = fields

        # 添加管理器
        if 'objects' not in attrs:
            attrs['objects'] = Manager()

        # 创建异常类
        attrs['DoesNotExist'] = type(f'{name}.DoesNotExist', (Exception,), {})
        attrs['MultipleObjectsReturned'] = type(f'{name}.MultipleObjectsReturned', (Exception,), {})

        return super().__new__(mcs, name, bases, attrs)


class TableStoreModel(metaclass=TableStoreModelMeta):
    """TableStore 模型基类"""

    objects = Manager()

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def save(self):
        """保存到 TableStore"""
        if not self.objects.client:
            raise ValueError("Client not set")

        table_name = self._meta['table_name']
        primary_keys = self._meta['primary_keys']

        # 构建主键
        pk_list = [(pk, getattr(self, pk)) for pk in primary_keys]

        # 构建属性列
        attribute_columns = []
        for field_name, field in self._fields.items():
            if field_name not in primary_keys:
                value = getattr(self, field_name, field.default)
                if value is not None:
                    attribute_columns.append((field_name, value))

        # 写入
        row = (pk_list, attribute_columns)
        self.objects.client.put_row(table_name, row)

    def delete(self):
        """从 TableStore 删除"""
        if not self.objects.client:
            raise ValueError("Client not set")

        table_name = self._meta['table_name']
        primary_keys = self._meta['primary_keys']

        pk_list = [(pk, getattr(self, pk)) for pk in primary_keys]
        self.objects.client.delete_row(table_name, pk_list, None)

    def __repr__(self):
        pk_values = ', '.join(
            f"{pk}={getattr(self, pk, None)}"
            for pk in self._meta['primary_keys']
        )
        return f"<{self.__class__.__name__}: {pk_values}>"


# ============ 使用示例 ============

if __name__ == '__main__':
    # 定义模型
    class User(TableStoreModel):
        class Meta:
            table_name = 'users'
            primary_keys = ['user_id']

        user_id = Field(primary_key=True)
        name = Field()
        age = Field()
        email = Field()
        city = Field()


    # 模拟数据(实际使用时连接真实的 TableStore)
    print("=== QuerySet 使用示例 ===\n")

    # 1. 基本过滤
    print("1. 过滤年龄 >= 18 的用户:")
    print("   User.objects.filter(age__gte=18)")

    # 2. 链式过滤
    print("\n2. 链式过滤:")
    print("   User.objects.filter(age__gte=18).filter(city='北京')")

    # 3. 排序
    print("\n3. 按年龄降序:")
    print("   User.objects.order_by('-age')")

    # 4. 切片
    print("\n4. 获取前 10 条:")
    print("   User.objects.all()[:10]")

    # 5. 获取单个对象
    print("\n5. 获取单个对象:")
    print("   user = User.objects.get(user_id='123')")

    # 6. 检查存在
    print("\n6. 检查是否存在:")
    print("   User.objects.filter(email__contains='@gmail.com').exists()")

    # 7. 计数
    print("\n7. 计数:")
    print("   User.objects.filter(age__gte=18).count()")

    # 8. 复杂查询
    print("\n8. 复杂查询:")
    print("   User.objects.filter(age__gte=18, city='上海').order_by('-age')[:5]")