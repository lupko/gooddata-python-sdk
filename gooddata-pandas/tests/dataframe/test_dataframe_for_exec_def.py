# (C) 2022 GoodData Corporation
from gooddata_pandas import DataFrameFactory
from gooddata_sdk import Attribute, ExecutionDefinition, ObjId, SimpleMetric


def test_dataframe_for_exec_def_two_dim1(gdf: DataFrameFactory):
    exec_def = ExecutionDefinition(
        attributes=[
            Attribute(local_id="region", label="region"),
            Attribute(local_id="state", label="state"),
            Attribute(local_id="product_category", label="products.category"),
        ],
        metrics=[
            SimpleMetric(local_id="price", item=ObjId(id="price", type="fact")),
            SimpleMetric(local_id="order_amount", item=ObjId(id="order_amount", type="metric")),
        ],
        filters=[],
        dimensions=[["state", "region"], ["product_category", "measureGroup"]],
    )

    result = gdf.for_exec_def(exec_def=exec_def)
    print(str(result))


def test_dataframe_for_exec_def_two_dim2(gdf: DataFrameFactory):
    exec_def = ExecutionDefinition(
        attributes=[
            Attribute(local_id="region", label="region"),
            Attribute(local_id="state", label="state"),
            Attribute(local_id="product_category", label="products.category"),
        ],
        metrics=[
            SimpleMetric(local_id="price", item=ObjId(id="price", type="fact")),
            SimpleMetric(local_id="order_amount", item=ObjId(id="order_amount", type="metric")),
        ],
        filters=[],
        dimensions=[["region", "state", "product_category"], ["measureGroup"]],
    )

    result = gdf.for_exec_def(exec_def=exec_def)
    print(str(result))


def test_dataframe_for_exec_def_two_dim3(gdf: DataFrameFactory):
    exec_def = ExecutionDefinition(
        attributes=[
            Attribute(local_id="region", label="region"),
            Attribute(local_id="state", label="state"),
            Attribute(local_id="product_category", label="products.category"),
        ],
        metrics=[
            SimpleMetric(local_id="price", item=ObjId(id="price", type="fact")),
            SimpleMetric(local_id="order_amount", item=ObjId(id="order_amount", type="metric")),
        ],
        filters=[],
        dimensions=[["product_category"], ["region", "state", "measureGroup"]],
    )

    result = gdf.for_exec_def(exec_def=exec_def)
    print(str(result))


def test_dataframe_for_exec_def_one_dim1(gdf: DataFrameFactory):
    exec_def = ExecutionDefinition(
        attributes=[
            Attribute(local_id="region", label="region"),
            Attribute(local_id="state", label="state"),
            Attribute(local_id="product_category", label="products.category"),
        ],
        metrics=[
            SimpleMetric(local_id="price", item=ObjId(id="price", type="fact")),
            SimpleMetric(local_id="order_amount", item=ObjId(id="order_amount", type="metric")),
        ],
        filters=[],
        dimensions=[["region", "state", "product_category", "measureGroup"]],
    )

    result = gdf.for_exec_def(exec_def=exec_def)
    print(str(result))


def test_dataframe_for_exec_def_one_dim2(gdf: DataFrameFactory):
    exec_def = ExecutionDefinition(
        attributes=[
            Attribute(local_id="region", label="region"),
            Attribute(local_id="state", label="state"),
            Attribute(local_id="product_category", label="products.category"),
        ],
        metrics=[
            SimpleMetric(local_id="price", item=ObjId(id="price", type="fact")),
            SimpleMetric(local_id="order_amount", item=ObjId(id="order_amount", type="metric")),
        ],
        filters=[],
        dimensions=[[], ["region", "state", "product_category", "measureGroup"]],
    )

    result = gdf.for_exec_def(exec_def=exec_def)
    print(str(result))
