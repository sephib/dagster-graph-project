from pathlib import Path

import pyarrow as pa
from dagster import (
    Failure,
    Field,
    InputDefinition,
    Output,
    OutputDefinition,
    Permissive,
    String,
    solid,
)
from dagster_shell.utils import execute as run_shell

import pyspark_transform
import utils


def run_shell_command(
    shell_command: str, output_logging: str, log, cwd: str = None, env: str = None
) -> str:
    output, return_code = run_shell(
        shell_command=shell_command,
        output_logging=output_logging,
        log=log,
        cwd=cwd,
        env=env,
    )

    if return_code:
        raise Failure(
            description="Shell command execution failed with output: {output}".format(
                output=output
            )
        )
    return output


@solid(
    output_defs=[OutputDefinition(name="local_path", dagster_type=str)],
    config_schema={
        "local_dir": Field(String, is_required=True, description="local_dir path.")
    },
)
def copy_to_local(context, hdfs_path: str) -> str:
    local_dir = context.solid_config.get("local_dir")
    local_path = f"{local_dir}/{Path(hdfs_path).name}"
    context.log.info(f"copy_to_local: from={hdfs_path} to={local_path}")
    run_shell_command(
        shell_command=f"hdfs dfs -copyToLocal {hdfs_path} {local_path}",
        output_logging="STREAM",
        log=context.log,
        cwd=None,
        env=None,
    )
    return local_path


@solid(
    output_defs=[OutputDefinition(name="base_dir", dagster_type=str)],
    config_schema={
        "base_dir": Field(
            String,
            is_required=True,
            description="base dir / environment to save files.",
        ),
        "relationships": Field(
            list, is_required=True, description="list of relationships."
        ),
        "bulkConfig": Field(
            Permissive(),
            is_required=True,
            description="variables for neorj config files",
        ),
    },
)
def create_config(context, path: str) -> str:
    """
    Function to gnerate neo4j config file for bulk import
    """
    base_dir = context.solid_config.get("base_dir")
    relationships = context.solid_config.get("relationships")
    bcf_section = context.solid_config.get("bulkConfig")
    neo4j_db_name = bcf_section.get("neo4j_db_name")
    neo_conf_file = bcf_section.get("localFileName")
    neo_conf_file = str(Path(base_dir, neo_conf_file))

    lines = [
        f"--database={neo4j_db_name}\n".encode("utf8"),
        f"--mode={bcf_section['mode']}\n".encode("utf8"),
        f"--delimiter={bcf_section['delimiter']}\n".encode("utf8"),
        f"--ignore-duplicate-nodes={bcf_section['ignoreDuplicateNodes']}\n".encode(
            "utf8"
        ),
        f"--ignore-missing-nodes={bcf_section['ignoreMissingNodes']}\n".encode("utf8"),
        f"--ignore-extra-columns={bcf_section['ignoreExtraColumns']}\n".encode("utf8"),
        f"--max-memory={bcf_section['maxMemory']}\n".encode("utf8"),
        f"--report-file={Path(bcf_section['reportFile']).parent}\{neo4j_db_name}_{Path(bcf_section['reportFile']).name}\n".encode(
            "utf8"
        ),
        f"--high-io={bcf_section['highIO']}\n".encode("utf8"),
        f'--nodes:Player "nodes/Player_header.csv,nodes/Player/part.*.csv"\n'.encode(
            "utf8"
        ),
        f'--nodes:Team "nodes/Team_header.csv,nodes/Team/part.*.csv"\n'.encode("utf8"),
    ] + [
        f'--relationships:{relationship.upper()} "{relationship.lower()}_header.csv,{relationship.lower()}_edges/part.*.csv"\n'.encode(
            "utf8"
        )
        for relationship in relationships
    ]

    # if str(path) != str(base_dir):
    #     raise ValueError(f"Not the same dir. {str(path)} != {str(base_dir)}")

    context.log.info(f"create_config: neo_config_file={neo_conf_file}")
    if str(neo_conf_file).startswith("hdfs:"):
        pac = pa.hdfs.connect()
        with pac.open(neo_conf_file, "wb") as bcf:
            bcf.writelines(lines)
    else:
        neo_conf_file = neo_conf_file.replace("file:", "c:")
        with open(neo_conf_file, "wb") as f:
            f.writelines(lines)

    context.log.info("Finish to create config files")
    return base_dir


@solid(
    output_defs=[OutputDefinition(dagster_type=str, name="path")],
    config_schema={
        "base_dir": Field(
            String, is_required=True, description="path to save dataframe"
        ),
        "label_types": Field(list, is_required=True, description="list of node types"),
    },
)
def create_nodes(context, dfs: list) -> str:
    export_dir = context.solid_config.get("base_dir")
    label_types = context.solid_config.get("label_types")

    dff = dfs[0]
    for df in dfs[1:]:
        dff = pyspark_transform.customUnion(dff, df)
    dff = dff.cache()

    for node_type in label_types:
        nodeID_neo = f"{node_type.upper()}:ID"
        df_label = dff.filter(dff["Label"] == node_type).dropDuplicates()
        df_label = df_label.withColumnRenamed("NodeID", nodeID_neo)
        df_label = df_label.withColumnRenamed("Label", ":LABEL")
        df_label = pyspark_transform.drop_null_cols(df_label)
        df_label = df_label.cache()
        context.log.info(
            f"nodes_{node_type}: count={df_label.count()}, columns{df_label.columns}"
        )

        utils.save_header(
            df=df_label,
            path=f"{export_dir.replace('file:///', 'C:/')}/nodes/{node_type}_header.csv",
            sep="|",
        )
        path = utils.save_file(
            df=df_label,
            repartition_=2,
            format_save_file="csv",
            options={"sep": "|", "quote": "\u0000"},
            base_dir=export_dir,
            path=f"nodes/{node_type}",
            mode="overwrite",
            saveing_header=False,
            rename=False,
            rename_stem=f"{node_type}",
            suffix=".csv",
            show=False,
        )

    yield Output(str(export_dir), "path")


@solid(
    name="execute_dep_shell_command",
    description="As solid to invoke a shell command with input to cause dependancy.",
    input_defs=[InputDefinition(name="path", dagster_type=str)],
    output_defs=[OutputDefinition(name="result", dagster_type=str)],
    config_schema={
        "environment": Field(
            str, is_required=True, description="Environment to run shell"
        ),
        "script": Field(str, is_required=True, description="Script to run"),
        "args": Field(
            list, is_required=True, description="additiona arguments for script"
        ),
    },
)
def execute_dep_shell_command(context, path: str):
    environment = context.solid_config.get("environment")
    script = context.solid_config.get("script")
    args = context.solid_config.get("args")

    bash_command = f"{environment} {script} {path} {' '.join(args)}"
    context.log.debug(f"bash_command={bash_command}")

    output = run_shell_command(
        shell_command=bash_command,
        output_logging="STREAM",
        log=context.log,
        cwd=None,
        env=None,
    )
    return output
