import pyspark.sql.functions as F
from dagster import (
    Any,
    Field,
    Int,
    Output,
    OutputDefinition,
    String,
    make_python_type_usable_as_dagster_type,
    solid,
)
from dagster_pyspark import DataFrame as DagsterPySparkDataFrame
from pyspark.sql import DataFrame

from utils import flatten_list

# Make pyspark.sql.DataFrame map to dagster_pyspark.DataFrame
make_python_type_usable_as_dagster_type(
    python_type=DataFrame, dagster_type=DagsterPySparkDataFrame
)


@solid(
    config_schema={
        "distinct_col": Field(
            String,
            is_required=True,
            description="column name relevant to the groupby filter",
        ),
        "col_gb": Field(
            list, is_required=True, description="column names for groupby filter"
        ),
        "min_weight": Field(
            Int, is_required=True, description="min_weight to filter on distict column"
        ),
        "max_weight": Field(
            Int, is_required=True, description="max_weight to filter on distict column"
        ),
    }
)
def filter_edges_by_threshold(context, df: DataFrame) -> DataFrame:
    """
    Filter dataframe by frequency of EdgeID
    """
    col_gb = context.solid_config.get("col_gb")
    distinct_col = context.solid_config.get("distinct_col")
    min_weight = context.solid_config.get("min_weight")
    max_weight = context.solid_config.get("max_weight")

    dfg = (
        df.groupby(col_gb)
        .agg(F.countDistinct(f"{distinct_col}"))
        .withColumnRenamed(f"count(DISTINCT {distinct_col})", "weight")
        .filter(f"weight >= {min_weight} and weight <= {max_weight}")
    )
    dfg_ = dfg.join(df, on=col_gb, how="left")
    dfg_ = dfg_.drop("weight").dropDuplicates()
    dfg_ = dfg_.cache()
    context.log.info(f"filter_edges_by_threshold: count={dfg_.count()}")
    return dfg_


@solid(
    config_schema={
        "col_edges": Field(
            String,
            default_value="EdgeID",
            description="column which is common to the nodes to create edges",
        ),
        "col_nodeid": Field(
            String, default_value="NodeID", description="column name of the node"
        ),
        "node_att": Field(
            Any, is_required=False, description="list of column edge attribute to keep"
        ),
    }
)
def create_edges_from_edgeID(context, df: DataFrame,) -> DataFrame:
    """
    Create edges (node1, node2) based on a common attribute (edgeid) 
    """
    EdgeID = context.solid_config.get("col_edges")
    NodeID = context.solid_config.get("col_nodeid")
    node_att = context.solid_config.get("node_att")

    suffix = "2"
    NodeID_y = f"{NodeID}{suffix}"
    query_statement = f"{NodeID} > {NodeID_y}"

    if node_att:
        node_att_y = [c + f"{suffix}" for c in node_att]
    else:
        node_att_y = []

    newcols = [f"{c}{suffix}" if c != EdgeID else c for c in df.columns]
    df2 = df.toDF(*newcols).select(*[[EdgeID, NodeID_y] + node_att_y])

    df = (
        df.join(df2, on=[EdgeID], how="left")
        .filter(query_statement)
        .dropDuplicates()
        .withColumnRenamed("NodeID", "NodeID1")
        .withColumnRenamed(NodeID_y, "NodeID2")
    )
    for col in node_att:
        df = df.withColumnRenamed(col, f"{col}1")
    df = df.cache()
    context.log.info(f"create_edges_from_edgeID: count={df.count()}")
    return df


@solid(
    output_defs=(
        [
            OutputDefinition(name="df_edges", dagster_type=DataFrame),
            OutputDefinition(name="df_nodes", dagster_type=DataFrame),
        ]
    ),
    config_schema={
        "cols_gb": Field(list, is_required=True, description="cols_gb"),
        "col_date": Field(String, is_required=False, description="col_date name"),
        "min_edges": Field(Int, default_value=1, description="number of edges to keep"),
        "node_att": Field(
            Any, is_required=False, description="list of column edge attribute to keep"
        ),
        "weight_distinct_col": Field(
            String,
            is_required=True,
            description="Column to use for weight edge - normally EdgeID",
        ),
    },
)
def edges_agg(context, df: DataFrame):
    cols_gb = context.solid_config.get("cols_gb")
    col_date = context.solid_config.get("col_date")
    min_edges = context.solid_config.get("min_edges")
    node_att = context.solid_config.get("node_att")
    weight_distinct_col = context.solid_config.get("weight_distinct_col")

    context.log.debug(
        f"edges_agg: cols_gb={cols_gb}, col_date={col_date}, min_edges={min_edges}"
    )

    edge_weight_col = "edge_weight"
    filter_query = f"{edge_weight_col}>={min_edges}"

    node_att_x = []
    node_att_y = []

    if node_att:
        node_att_x = [f"{c}1" for c in node_att]
        node_att_y = [f"{c}2" for c in node_att]

    collect_set_list = list(
        set(df.columns) - set(cols_gb) - set(node_att_x + node_att_y)
    )

    agg_list = [F.countDistinct(F.col(weight_distinct_col)).alias(edge_weight_col),] + [
        F.collect_set(F.col(c).cast("string")).cast("string").alias(f"{c}_att")
        for c in collect_set_list
    ]
    if col_date:
        agg_list += [
            F.min(F.col(col_date).cast("string")).alias(f"{col_date}_min"),
            F.max(F.col(col_date).cast("string")).alias(f"{col_date}_max"),
        ]

    df_edges = df.groupBy(cols_gb).agg(*agg_list).filter(filter_query)

    node_att_dict = {k: v for k, v in zip(cols_gb, [node_att_x, node_att_y])}
    df_nodes = df.select(*cols_gb + node_att_x + node_att_y)
    df_nodes = create_df_node(df_nodes, node_att_dict)
    df_edges_nodes = create_df_node(df_edges, {k: None for k in cols_gb})
    df_nodes = df_nodes.join(
        df_edges_nodes, on="NodeID", how="right"
    )  # keep only nodes that are in df_edges

    df_edges = df_edges.cache()
    df_nodes = df_nodes.cache()

    context.log.info(f"df_edges: count={df_edges.count}, columns={df_edges.columns}")
    context.log.info(f"df_nodes: count={df_nodes.count}, columns={df_nodes.columns}")

    yield Output(df_edges, "df_edges")
    yield Output(df_nodes, "df_nodes")


def create_df_node(df: DataFrame, node_attributes) -> DataFrame:
    """
    Creating df_node from df, base on node_attributes
    :param df: dataframe
    :param node_attributes: dict, e.g. {'NodeID1': ['LABELx', 'NAME', 'ATT']},
    :return: df_node dataframe
    """
    df_node = None
    for elem in node_attributes.items():
        node_columns = [x for x in flatten_list(elem) if x]
        _nodeID = node_columns[0]
        _df_node = df.select(node_columns)

        for col in node_columns:
            _df_node = _df_node.withColumnRenamed(
                f"{col}", f"{col[:-1]}"
            )  # "idEntity:ID" # ":LABEL"

        if df_node is None:
            df_node = _df_node
        else:
            df_node = df_node.union(_df_node)

    df_node = df_node.dropDuplicates()
    return df_node
