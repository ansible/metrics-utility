from insights_analytics_collector import register


@register("config", "1.0", description="CONFIG", config=True)
def config(since, **kwargs):
    return {"version": "1.0"}


@register("json1", "1.1", description="json1")
def json1(**kwargs):
    return {"json1": "True"}


@register("json2", "1.2", description="json2")
def json2(**kwargs):
    return {"json2": "True"}


@register("json3", "1.3", description="json3")
def json3(**kwargs):
    return {"json3": "True"}
