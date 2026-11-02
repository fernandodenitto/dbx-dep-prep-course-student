# Source of the bundle's Lakeflow pipeline. Thin on purpose: the logic is imported, and it is
# the same logic the notebooks and the tests use.
# `root_path: ../src` in the pipeline resource is what puts lakeshore_pro on sys.path here.

from pyspark import pipelines as dp

from lakeshore_pro import add_cart_total, parse_bus_orders, reject_reason


@dp.materialized_view(comment="Bus orders, graded by the lakeshore_pro quality ladder")
@dp.expect("not_rejected", "reject_reason IS NULL")     # records the count, keeps the row
def graded_orders():
    bus = spark.read.table(spark.conf.get("source_table"))
    return parse_bus_orders(bus).transform(reject_reason).transform(add_cart_total)
