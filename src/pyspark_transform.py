from itertools import cycle
from typing import List

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame


def create_schema(dtype: List[List]) -> T.StructType:
    struct_type = T.StructType()
    for col, _type in dtype:
        struct_type.add(col, _type, False)
    return struct_type


def assignF(df: DataFrame, col: str, func: str, attr) -> DataFrame:
    if isinstance(attr, list):
        return assign(df, col, getattr(F, func)(*attr))
    elif isinstance(attr, dict):
        return assign(df, col, getattr(F, func)(**attr))
    else:
        return assign(df, col, getattr(F, func)(attr))


def assign(df: DataFrame, col: str, val) -> DataFrame:
    return df.withColumn(col, val)


def to_timestamp(df: DataFrame, col: str, format: str) -> DataFrame:
    return assignF(df, col, "unix_timestamp", {"format": format})


def to_date(df: DataFrame, col: str) -> DataFrame:
    return assignF(df, col, "to_date", {"col": col})


def rename_cols(df: DataFrame, renamed_cols: dict) -> DataFrame:
    for existing, new in renamed_cols.items():
        df = df.withColumnRenamed(existing, new)
    return df


def drop_cols(df: DataFrame, subset: list) -> DataFrame:
    return df.drop(*subset)


def drop_duplicates(df: DataFrame, subset: list) -> DataFrame:
    return df.drop_duplicates(*subset)


def drop_null_cols(df: DataFrame, threshold: int = None) -> DataFrame:
    if not threshold:
        threshold = df.count()
    null_cols = (
        df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns])
        .collect()[0]
        .asDict()
    )
    to_drop = [k for k, v in null_cols.items() if v == threshold]
    return df.drop(*to_drop)


def use_cols(df: DataFrame, keep_cols: list) -> DataFrame:
    return df.select([col for col in df.columns if col in keep_cols])


def clean_cols(df: DataFrame, cols: list) -> DataFrame:
    for _col in cols:
        df = df.withColumn(_col, F.regexp_replace(F.col(_col), r"[^0-9a-zA-Zא-ת ]", ""))
    return df


def replace(df: DataFrame, to_replace, subset, **kwargs) -> DataFrame:
    return df.replace(to_replace=to_replace, subset=subset, **kwargs)


def filter_length_str_col(
    df: DataFrame, col: str, min_len: int, max_len: int
) -> DataFrame:
    return df.where((F.length(col) >= min_len) & (F.length(col) <= max_len))


def customUnion(df1: DataFrame, df2: DataFrame) -> DataFrame:
    cols1 = df1.columns
    cols2 = df2.columns
    total_cols = sorted(cols1 + list(set(cols2) - set(cols1)))

    def expr(mycols, allcols):
        def processCols(colname):
            if colname in mycols:
                return colname
            else:
                return F.lit(None).alias(colname)

        cols = map(processCols, allcols)
        return list(cols)

    appended = df1.select(expr(cols1, total_cols)).union(
        df2.select(expr(cols2, total_cols))
    )
    return appended


def astype(df: DataFrame, cols: list, type: str) -> DataFrame:
    for col in cols:
        df = df.withColumn(col, F.col(col).cast(type))
    return df


def lit(df: DataFrame, col: str, attr) -> DataFrame:
    return assignF(df, col=col, func="lit", attr=attr)


def order_nodeID_cols(df: DataFrame, dNodeID1: dict, dNodeID2: dict) -> DataFrame:
    NodeID1, NodeID2 = {**dNodeID1, **dNodeID2}.keys()
    cols_nodes1, cols_nodes2 = [
        [k] + v if isinstance(v, list) else [k] + [v]
        for k, v in {**dNodeID1, **dNodeID2}.items()
    ]
    query = df[NodeID1] > df[NodeID2]
    rename_cols = {}
    for col1, col2 in zip(cols_nodes1, cols_nodes2):
        _col1, _col2 = f"_{col1}", f"_{col2}"
        rename_cols[_col1] = col1
        rename_cols[_col2] = col2
        df = df.withColumn(_col1, F.when(query, df[col1]).otherwise(df[col2]))
        df = df.withColumn(_col2, F.when(query, df[col2]).otherwise(df[col1]))

    df = df.drop(*cols_nodes1 + cols_nodes2)
    for existing, new in rename_cols.items():
        df = df.withColumnRenamed(existing, new)

    return df


def concat(df: DataFrame, col: str, subset: list, sep: str) -> DataFrame:
    if sep:
        df = assignF(df, col=sep, func="lit", attr=sep)
        subset = [[i, j] for i, j in zip(subset, cycle(sep))]
        subset = [item for sublist in subset for item in sublist][:-1]

    df = assignF(df, col=col, func="concat", attr=subset)
    df = df.drop(sep)
    return df


def rename_columns(df: DataFrame, columns: list) -> DataFrame:
    if isinstance(columns, dict):
        for old_name, new_name in columns.items():
            df = df.withColumnRenamed(old_name, new_name)
        return df
    else:
        raise ValueError(f"columns paramter should be a dict")


def stringify(df: DataFrame, columns: list) -> DataFrame:
    for col in columns:
        df = df.withColumn(
            f"{col}", F.concat(F.lit("["), F.concat_ws(",", col), F.lit("]"))
        )
    return df
