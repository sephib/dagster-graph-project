import pyspark.sql.functions as F
from dagster import (
    Any,
    Array,
    Bool,
    Field,
    Int,
    OutputDefinition,
    Permissive,
    String,
    make_python_type_usable_as_dagster_type,
    solid,
)
from dagster_pyspark import DataFrame as DagsterPySparkDataFrame
from pyspark.sql import DataFrame

import pyspark_transform as transform

# Make pyspark.sql.DataFrame map to dagster_pyspark.DataFrame
make_python_type_usable_as_dagster_type(
    python_type=DataFrame, dagster_type=DagsterPySparkDataFrame
)


@solid(
    output_defs=[OutputDefinition(dagster_type=DataFrame, name="df")],
    config_schema={
        "col_new": Field(String, is_required=True, description="new column name"),
        "subset": Field(
            Array(String), is_required=True, description="list of columns to concat"
        ),
        "sep": Field(
            String, is_required=False, description="seperator between columns"
        ),
    },
)
def concat(context, df: DataFrame) -> DataFrame:
    col_new = context.solid_config.get("col_new")
    subset = context.solid_config.get("subset")
    sep = context.solid_config.get("sep")
    context.log.debug(f"concat: col_new={col_new}, subset={subset}, sep={sep}")
    return transform.concat(df, col=col_new, subset=subset, sep=sep)


@solid(
    output_defs=[OutputDefinition(dagster_type=DataFrame, name="df")],
    config_schema={
        "col_new": Field(String, is_required=True, description="new column name"),
        "value": Field(String, is_required=True, description="column value"),
    },
)
def lit(context, df: DataFrame) -> DataFrame:
    col_new = context.solid_config.get("col_new")
    value = context.solid_config.get("value")
    context.log.debug(f"lit: col_new={col_new}, value={value}")
    return transform.assignF(df, col=col_new, func="lit", attr=value)


@solid(
    output_defs=[OutputDefinition(dagster_type=DataFrame, name="df")],
    config_schema={
        "type": Field(String, is_required=True, description="type to convert"),
        "subset": Field(
            list,
            is_required=True,
            description="optional list of column names to consider",
        ),
    },
)
def astype(context, df: DataFrame) -> DataFrame:
    _type = context.solid_config.get("type")
    subset = context.solid_config.get("subset")
    context.log.debug(f"astype: cols={subset}, type={_type}")
    return transform.astype(df, cols=subset, type=_type)


@solid(
    output_defs=[OutputDefinition(dagster_type=DataFrame, name="df")],
    config_schema={
        "col_new": Field(String, is_required=True, description="new column name"),
        "col_existing": Field(
            String, is_required=True, description="existing column name"
        ),
        "len": Field(Int, is_required=True, description="len"),
        "pad": Field(String, is_required=True, description="pad char"),
    },
)
def lpad(context, df: DataFrame) -> DataFrame:
    col_new = context.solid_config.get("col_new")
    col_existing = context.solid_config.get("col_existing")
    len = context.solid_config.get("len")
    pad = context.solid_config.get("pad")
    context.log.debug(
        f"lpad: col_new={col_new}, col_existing={col_existing}, len={len}, pad={pad}"
    )
    return transform.assignF(
        df, col=col_new, func="lpad", attr={"col": col_existing, "len": len, "pad": pad}
    )


@solid(
    output_defs=[OutputDefinition(dagster_type=DataFrame, name="df")],
    config_schema={
        "cols": Field(
            list, is_required=True, description="list of cols to convert to timestamp"
        ),
        "format": Field(String, is_required=True, description="timestamp format"),
    },
)
def to_timestamp(context, df: DataFrame) -> DataFrame:
    cols = context.solid_config.get("cols")
    format = context.solid_config.get("format")
    context.log.debug(f"to_timestamp: cols={cols}, format={format}")
    for col in cols:
        df = transform.assignF(df, col, "to_timestamp", {"format": format})
    return df


@solid(
    output_defs=[OutputDefinition(dagster_type=DataFrame, name="df")],
    config_schema={
        "cols": Field(
            list, is_required=True, description="list of cols to convert to date"
        ),
        "format": Field(
            String,
            is_required=False,
            description="timestamp format",
            default_value="dd/MM/yyyy",
        ),
    },
)
def to_date(context, df: DataFrame) -> DataFrame:
    cols = context.solid_config.get("cols")
    format = context.solid_config.get("format")
    context.log.debug(f"to_date: cols={cols}, format={format}")
    for col in cols:
        df = transform.assignF(df, col, "to_date", {"col": col, "format": format})
    return df


@solid(
    output_defs=[OutputDefinition(dagster_type=DataFrame, name="df")],
    config_schema={
        "renamed_cols": Field(
            Permissive(),
            is_required=True,
            description="dict existing (key), new (value) to rename columns",
        ),
    },
)
def rename_cols(context, df: DataFrame) -> DataFrame:
    renamed_cols = context.solid_config.get("renamed_cols")
    if not set(renamed_cols.keys()).issubset(df.columns):
        context.log.warning(
            f"Columns to rename: {set(renamed_cols.keys())-set(df.columns)} are not in dataframe cols: {df.columns}"
        )

    context.log.debug(f"rename_cols: renamed_cols={renamed_cols}")
    df = transform.rename_cols(df, renamed_cols)
    return df


@solid(
    output_defs=[OutputDefinition(dagster_type=DataFrame, name="df")],
    config_schema={
        "subset": Field(list, is_required=True, description="list of columns to drop"),
    },
)
def drop_cols(context, df: DataFrame) -> DataFrame:
    subset = context.solid_config.get("subset")
    context.log.debug(f"drop_cols: subset={subset}")
    return df.drop(*subset)


@solid(
    output_defs=[OutputDefinition(dagster_type=DataFrame, name="df")],
    config_schema={
        "subset": Field(
            list,
            is_required=False,
            description="list of columns to use for drop_duplicates",
        ),
    },
)
def drop_duplicates(context, df: DataFrame) -> DataFrame:
    subset = context.solid_config.get("subset", None)
    if not subset:
        subset = df.columns
    context.log.debug(f"drop_duplicates: subset={subset}")
    return df.drop_duplicates(subset)


@solid(
    output_defs=[OutputDefinition(dagster_type=DataFrame, name="df")],
    config_schema={
        "keep_cols": Field(
            list, is_required=True, description="list of columns to keep"
        ),
    },
)
def use_cols(context, df: DataFrame) -> DataFrame:
    keep_cols = context.solid_config.get("keep_cols")
    if not set(keep_cols).issubset(df.columns):
        context.log.error(
            f"Columns in keep_cols: {set(keep_cols)-set(df.columns)} are not in dataframe cols: {df.columns}"
        )
        exit()
    context.log.debug(f"use_cols: keep_cols={keep_cols}")

    return df.select([col for col in df.columns if col in keep_cols])


@solid(
    output_defs=[OutputDefinition(dagster_type=DataFrame, name="df")],
    config_schema={
        "value": Field(Any, is_required=True, description="fillna value"),
        "subset": Field(
            Any,
            default_value=None,
            is_required=False,
            description="optional list of column names to consider",
        ),
    },
)
def fillna(context, df: DataFrame) -> DataFrame:
    value = context.solid_config.get("value")
    subset = context.solid_config.get("subset")
    context.log.debug(f"fillna: value={value}, subset={subset}")
    return df.fillna(value=value, subset=subset)


@solid(
    output_defs=[OutputDefinition(dagster_type=DataFrame, name="df")],
    config_schema={
        "how": Field(
            String, default_value="any", is_required=False, description="dropna how"
        ),
        "thresh": Field(
            Any, default_value=None, is_required=False, description="dropna thresh",
        ),
        "subset": Field(
            Any,
            default_value=None,
            is_required=False,
            description="optional list of column names to consider",
        ),
    },
)
def dropna(context, df: DataFrame) -> DataFrame:
    how = context.solid_config.get("how")
    thresh = context.solid_config.get("thresh")
    subset = context.solid_config.get("subset")
    context.log.debug(f"dropna: how={how}, thresh={thresh}, subset={subset}")
    return df.dropna(how=how, thresh=thresh, subset=subset)


@solid(
    output_defs=[OutputDefinition(dagster_type=DataFrame, name="df")],
    config_schema={
        "to_replace": Field(Any, is_required=True, description="to_replace"),
        "value": Field(Any, is_required=False, description="replace value"),
        "subset": Field(
            Any,
            default_value=None,
            is_required=False,
            description="optional list of column names to consider",
        ),
    },
)
def replace(context, df: DataFrame) -> DataFrame:
    to_replace = context.solid_config.get("to_replace")
    value = context.solid_config.get("value")
    subset = context.solid_config.get("subset")
    context.log.debug(
        f"replace: to_replace={to_replace}, value={value}, subset={subset}"
    )
    return transform.replace(df=df, to_replace=to_replace, subset=subset, value=value)


@solid(
    output_defs=[OutputDefinition(dagster_type=DataFrame, name="df")],
    config_schema={
        "column_filter": Field(
            str, is_required=True, description="column to filter on"
        ),
        "filter_list": Field(
            list, is_required=True, description="list of values to filter by"
        ),
        "not_in": Field(
            Bool,
            default_value=False,
            is_required=False,
            description="filter not in list",
        ),
    },
)
def filter_isin(context, df: DataFrame) -> DataFrame:
    column_filter = context.solid_config.get("column_filter")
    filter_list = context.solid_config.get("filter_list")
    not_in = context.solid_config.get("not_in")
    context.log.debug(
        f"filter_isin: column_filter={column_filter}, filter_list={filter_list}, not_in: {not_in}"
    )
    if not_in:
        df = df.where(~df[column_filter].isin(filter_list))
    else:
        df = df.where(df[column_filter].isin(filter_list))
    return df


@solid
def union(context, df1: DataFrame, df2: DataFrame) -> DataFrame:
    context.log.debug(f"union: df1_cols={df1.columns}, df2_cols={df2.columns}")
    return transform.customUnion(df1, df2)


@solid(
    output_defs=[OutputDefinition(dagster_type=DataFrame, name="df")],
    config_schema={
        "on_col": Field(String, is_required=True, description="common col to join on"),
        "how_j": Field(
            String,
            is_required=True,
            description="Must be one of the following:"
            "[``inner``, ``cross``, ``outer``, ``full``, ``full_outer``, ``left``, ``left_outer``,"
            "``right``, ``right_outer``, ``left_semi``, and ``left_anti``].",
        ),
    },
)
def join_df(context, df_left: DataFrame, df_right: DataFrame) -> DataFrame:
    on_col = context.solid_config.get("on_col")
    how_j = context.solid_config.get("how_j")
    context.log.debug(f"join_df: on_col={on_col}, how_j={how_j}")
    return df_left.join(df_right, on=on_col, how=how_j).dropDuplicates()


@solid(
    output_defs=[OutputDefinition(dagster_type=DataFrame, name="df")],
    config_schema={
        "col": Field(
            String, is_required=False, description="column names to consider",
        ),
        "min_len": Field(Int, is_required=True, description="min value"),
        "max_len": Field(Int, is_required=True, description="max value"),
    },
)
def filter_length_str_col_solid(context, df: DataFrame) -> DataFrame:
    col = context.solid_config.get("col")
    min_len = context.solid_config.get("min_len")
    max_len = context.solid_config.get("max_len")
    context.log.debug(
        f"filter_length_str_col_solid: col={col}, min_len={min_len}, max_len={max_len}"
    )
    return transform.filter_length_str_col(df, col, min_len, max_len)


@solid(
    output_defs=[OutputDefinition(dagster_type=DataFrame, name="df")],
    config_schema={
        "col": Field(String, is_required=True, description="column to run filter on",),
    },
)
def filter_non_int_rows(context, df: DataFrame) -> DataFrame:
    col = context.solid_config.get("col")
    context.log.debug(f"filter_non_int_rows: col={col}")
    return (
        df.withColumn("Num", F.regexp_extract("NodeID", r"\d+", 0))
        .filter(F.col("Num") != "")
        .drop("Num")
    )


@solid(
    output_defs=[OutputDefinition(dagster_type=DataFrame, name="df")],
    config_schema={
        "query": Field(
            String, is_required=True, description="query to run on the dataframe",
        ),
    },
)
def query_filter_df(context, df: DataFrame) -> DataFrame:
    query = context.solid_config.get("query")
    return df.filter(query)


@solid(
    output_defs=[OutputDefinition(dagster_type=DataFrame, name="df")],
    config_schema={
        "dict_NodeID1": Field(
            Any,
            is_required=True,
            description="dictionary for NodeID att column e.g. {'NodeID1': 'LABEL'}",
        ),
        "dict_NodeID2": Field(
            Any,
            is_required=True,
            description="dictionary for NodeID att column e.g. {'NodeID1': 'LABEL'}",
        ),
    },
)
def order_nodeID_cols(context, df: DataFrame) -> DataFrame:
    dict_NodeID1 = context.solid_config.get("dict_NodeID1")
    dict_NodeID2 = context.solid_config.get("dict_NodeID2")
    return transform.order_nodeID_cols(df, dict_NodeID1, dict_NodeID2)


@solid(
    output_defs=[OutputDefinition(dagster_type=DataFrame, name="df")],
    config_schema={
        "subset": Field(
            Array(String), is_required=True, description="list of columns to clean"
        ),
    },
)
def clean_cols(context, df: DataFrame) -> DataFrame:
    subset = context.solid_config.get("subset")
    context.log.debug(f"clean_cols: subset={subset}")
    return transform.clean_cols(df, subset)


@solid(
    output_defs=[OutputDefinition(dagster_type=DataFrame, name="df")],
    config_schema={
        "subset": Field(
            Array(String), is_required=True, description="list of columns to stringify",
        ),
    },
)
def stringify(context, df: DataFrame) -> DataFrame:
    subset = context.solid_config.get("subset")
    context.log.debug(f"stringify: subset={subset}")
    return transform.stringify(df, subset)


@solid()
def cache(_, df: DataFrame) -> DataFrame:
    df = df.cache()
    return df
