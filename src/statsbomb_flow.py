from pathlib import Path

import findspark

findspark.init()

from dagster import (
    ModeDefinition,
    OutputDefinition,
    composite_solid,
    execute_pipeline,
    make_python_type_usable_as_dagster_type,
    pipeline,
)
from dagster_pyspark import DataFrame as DagsterPySparkDataFrame
from dagster_pyspark import pyspark_resource
from pyspark.sql import DataFrame

import solids_edges
import solids_utils
import solids_neo4j
import solids_transform

from utils import set_spark_environ, yamls_to_dict

set_spark_environ()
# Make pyspark.sql.DataFrame map to dagster_pyspark.DataFrame
make_python_type_usable_as_dagster_type(
    python_type=DataFrame, dagster_type=DagsterPySparkDataFrame
)


@composite_solid(
    output_defs=(
        [
            OutputDefinition(name="df_edges", dagster_type=DataFrame),
            OutputDefinition(name="df_nodes", dagster_type=DataFrame),
        ]
    ),
)
def passed_to():
    df_edges_disc = solids_transform.lit.alias("add_col_label1")(
        solids_transform.lit.alias("add_col_label2")(
            solids_transform.astype(
                solids_transform.rename_cols(
                    solids_transform.drop_cols(
                        solids_transform.dropna(solids_utils.read_file())
                    )
                )
            )
        )
    )
    solids_utils.show(df_edges_disc)
    df_edges, df_nodes = solids_edges.edges_agg(df_edges_disc)
    solids_utils.save_header.alias("save_header_edges")(
        solids_transform.rename_cols.alias("rename_cols_neo4j")(df_edges)
    )
    return df_edges, df_nodes


@composite_solid(
    output_defs=(
        [
            OutputDefinition(name="df_edges", dagster_type=DataFrame),
            OutputDefinition(name="df_nodes", dagster_type=DataFrame),
        ]
    ),
)
def played_together():
    df_etl = solids_transform.lit.alias("add_col_label1")(
        solids_transform.dropna(
            solids_transform.use_cols.alias("use_cols")(solids_utils.read_file())
        )
    )

    df_edges_disc = solids_edges.create_edges_from_edgeID(
        solids_transform.concat(
            solids_transform.rename_cols.alias("rename_cols1")(df_etl)
        )
    )
    df_edges, df_nodes = solids_edges.edges_agg(df_edges_disc)
    solids_utils.save_header.alias("save_header_edges")(
        solids_transform.rename_cols.alias("rename_cols_neo4j")(df_edges)
    )
    return df_edges, df_nodes


@composite_solid(
    output_defs=(
        [
            OutputDefinition(name="df_edges", dagster_type=DataFrame),
            OutputDefinition(name="df_nodes", dagster_type=DataFrame),
        ]
    ),
)
def played_in():
    df_etl = solids_transform.drop_duplicates(
        solids_transform.concat(
            solids_transform.lit.alias("add_col_label1")(
                solids_transform.dropna(
                    solids_transform.rename_cols.alias("rename_cols1")(
                        solids_transform.use_cols.alias("use_cols")(
                            solids_utils.read_file()
                        )
                    )
                )
            )
        )
    )

    df_edges_disc = solids_transform.lit.alias("add_col_label2")(
        solids_transform.rename_cols.alias("rename_cols2")(df_etl)
    )

    df_edges, df_nodes = solids_edges.edges_agg(df_edges_disc)

    solids_utils.save_header.alias("save_header_edges")(
        solids_transform.rename_cols.alias("rename_cols_neo4j")(df_edges)
    )
    return df_edges, df_nodes


@composite_solid()
def create_neo4j_db(dfs_nodes: list):
    path_nodes = solids_neo4j.create_nodes(dfs=dfs_nodes)
    hdfs_path = solids_neo4j.create_config(path=path_nodes)
    # Use `copy_to_local` only if the data is stored in 'hdfs'.
    # local_path = solids_neo4j.copy_to_local(hdfs_path=hdfs_path)


@pipeline(mode_defs=[ModeDefinition(resource_defs={"spark": pyspark_resource})])
def statsbomb_pipeline():
    passed_to_edges, passed_to_nodes = passed_to()
    played_together_edges, played_together_nodes = played_together()
    played_in_edges, played_in_nodes = played_in()
    create_neo4j_db(dfs_nodes=[passed_to_nodes, played_together_nodes, played_in_nodes])


if __name__ == "__main__":
    env_dict = yamls_to_dict(
        [
            Path(__file__).parent / "../env_yaml/resources.yaml",
            Path(__file__).parent / "../env_yaml/played_in.yaml",
            Path(__file__).parent / "../env_yaml/played_together.yaml",
            Path(__file__).parent / "../env_yaml/passed_to.yaml",
            Path(__file__).parent / "../env_yaml/create_neo4j_db.yaml",
        ]
    )
    result = execute_pipeline(statsbomb_pipeline, run_config=env_dict)
    assert result.success
