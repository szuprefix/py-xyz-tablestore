import django_filters

def create_auto_filter_set(model, filterset_fields, filterset_base=django_filters.FilterSet):
    """
    用 type() 动态创建 AutoFilterSet 类，避免闭包引用失败的问题。
    """

    if not filterset_fields:
        return None

    # 先构造 Meta 基类
    MetaBase = getattr(filterset_base, "Meta", object)

    # 动态构造内部 Meta 类
    Meta = type(
        "Meta",
        (MetaBase,),
        {
            "model": model,
            "fields": filterset_fields,
        }
    )

    # 构造 AutoFilterSet 类
    AutoFilterSet = type(
        "AutoFilterSet",
        (filterset_base,),
        {
            "Meta": Meta,
        }
    )

    return AutoFilterSet
