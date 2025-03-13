from insights_analytics_collector import register
from tests.functional.helpers import simple_csv, trivial_slicing


@register("config", "1.0", description="CONFIG", config=True)
def config(since, **kwargs):
    return {"version": "1.0"}


@register("simple_json1", "1.0", description="json1")
def simple_json1(**kwargs):
    return {"simple_json": "True"}


@register("csv_no_slicing_1-2x", "1.0", format="csv", description="CSV No slicing 1")
def csv_no_slicing1(full_path, **kwargs):
    return simple_csv(full_path, "csv_no_slicing_1-2x", 2, 100)


@register(
    "csv_with_slicing_1-5x",
    "1.0",
    format="csv",
    description="CSV With Slicing 1a",
    fnc_slicing=trivial_slicing,
)
def csv_with_slicing1a(full_path, **kwargs):
    return simple_csv(full_path, "csv_with_slicing_1-5x", 2, 100)


@register(
    "csv_with_slicing_1-5x",
    "1.0",
    format="csv",
    description="CSV With Slicing 1b",
    fnc_slicing=trivial_slicing,
)
def csv_with_slicing1b(full_path, **kwargs):
    return simple_csv(full_path, "csv_with_slicing_1-5x", 3, 100)


@register("csv_no_slicing_2-1x", "1.0", format="csv", description="CSV No slicing 2")
def csv_no_slicing2(full_path, **kwargs):
    return simple_csv(full_path, "csv_no_slicing_2-1x", 1, 100)


@register(
    "csv_with_slicing_2-3x",
    "1.0",
    format="csv",
    description="CSV With Slicing 2a",
    fnc_slicing=trivial_slicing,
)
def csv_with_slicing2a(full_path, **kwargs):
    return simple_csv(full_path, "csv_with_slicing_2-3x", 2, 100)


@register("csv_no_slicing_3-10x", "1.0", format="csv", description="CSV No slicing 3")
def csv_no_slicing3(full_path, **kwargs):
    return simple_csv(full_path, "csv_no_slicing_3-10x", 10, 100)


@register(
    "csv_with_slicing_3-2x",
    "2.0",
    format="csv",
    description="CSV With Slicing 3",
    fnc_slicing=trivial_slicing,
)
def csv_with_slicing3(full_path, **kwargs):
    return simple_csv(full_path, "csv_with_slicing_3-2x", 2, 100)


@register("csv_no_slicing_4-12x", "1.0", format="csv", description="CSV No slicing 3")
def csv_no_slicing4(full_path, **kwargs):
    return simple_csv(full_path, "csv_no_slicing_4-12x", 12, 100)


@register(
    "csv_with_slicing_2-3x",
    "2.0",
    format="csv",
    description="CSV With Slicing 2b",
    fnc_slicing=trivial_slicing,
)
def csv_with_slicing2b(full_path, **kwargs):
    return simple_csv(full_path, "csv_with_slicing_2-3x", 1, 100)


@register(
    "csv_with_slicing_4-3x",
    "2.0",
    format="csv",
    description="CSV With Slicing 4",
    fnc_slicing=trivial_slicing,
)
def csv_with_slicing4(full_path, **kwargs):
    return simple_csv(full_path, "csv_with_slicing_4-3x", 3, 100)
