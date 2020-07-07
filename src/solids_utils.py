# pylint: disable=no-value-for-parameter
from subprocess import call
import pyarrow
from dagster import (
    Any,
    Bool,
    Dict,
    Field,
    Int,
    Output,
    OutputDefinition,
    Permissive,
    String,
    solid,
)
from dagster_pyspark import DataFrame

import pyspark_transform as transform
import utils


@solid(
    config_schema={
        "path": Field(
            String,
            is_required=True,
            description="Path to save the csv file with the `header`.",
        ),
        "sep": Field(
            String,
            is_required=False,
            default_value="|",
            description="Single character as a separator for each field and value.",
        ),
    },
)
def save_header(context, df):
    path = context.solid_config.get("path")
    sep = context.solid_config.get("sep")
    return utils.save_header(df, path, sep)


@solid(
    output_defs=[OutputDefinition(dagster_type=DataFrame, name="df")],
    required_resource_keys={"spark"},
    config_schema={
        "path": Field(
            Any,
            is_required=True,
            description=(
                "String or a list of string for file-system backed data sources."
            ),
        ),
        "dtype": Field(
            list,
            is_required=False,
            description='Dictionary with column types e.g. {"col_name": "string"}.',
        ),
        "format": Field(
            String,
            default_value="csv",
            is_required=False,
            description='String for format of the data source. Default to "parquet".',
        ),
        "options": Field(
            Permissive(
                fields={
                    "inferSchema": Field(Bool, is_required=False),
                    "sep": Field(String, is_required=False),
                    "header": Field(Bool, is_required=False),
                    "encoding": Field(String, is_required=False),
                }
            ),
            is_required=False,
        ),
    },
)
def read_file(context) -> DataFrame:
    path = context.solid_config["path"]
    dtype = context.solid_config.get("dtype")
    _format = context.solid_config.get("format")
    options = context.solid_config.get("options", {})
    context.log.debug(
        f"read_file: path={path}, dtype={dtype}, _format={_format}, options={options}, "
    )
    spark = context.resources.spark.spark_session
    if dtype:
        df = (
            spark.read.format(_format)
            .options(**options)
            .schema(transform.create_schema(dtype))
            .load(path)
        )
    else:
        df = spark.read.format(_format).options(**options).load(path)

    yield Output(df, "df")


@solid(
    output_defs=[OutputDefinition(dagster_type=str, name="path")],
    config_schema={
        "base_dir": Field(
            String,
            is_required=True,
            description="base dir / environment to save files.",
        ),
        "path": Field(
            String, is_required=True, description="*Folder* Path to save dataframe."
        ),
        "format": Field(
            String,
            default_value="csv",
            is_required=False,
            description='Default to "csv" other options are "parquet", JSON.',
        ),
        "mode": Field(
            String,
            default_value="overwrite",
            is_required=False,
            description=(
                "Specifies the behavior of the save operation when data already exsists. (append, overwrite,...)"
            ),
        ),
        "single_file": Field(
            Bool,
            default_value=False,
            is_required=False,
            description="Specifies if outcome should be single file",
        ),
        "repartition": Field(
            Int, is_required=False, description="Number of partitions to save output"
        ),
        "save_header": Field(
            Bool,
            default_value=True,
            is_required=False,
            description="Save the header in a seperate file",
        ),
        "show": Field(
            Bool,
            default_value=True,
            is_required=False,
            description="Save the header in a seperate file",
        ),
        "rename": Field(
            Bool,
            default_value=True,
            is_required=False,
            description="Rename files saved into hdfs.",
        ),
        "rename_stem": Field(
            String, is_required=False, description="Stem to rename files."
        ),
        "rename_suffix": Field(
            String, is_required=False, description="Suffix to rename files"
        ),
        "options": Field(
            Permissive(
                fields={
                    "sep": Field(String, is_required=False),
                    "header": Field(Bool, is_required=False),
                    "codec": Field(String, is_required=False),
                    "compression": Field(String, is_required=False),
                }
            ),
            is_required=False,
        ),
    },
)
def save_file(context, df: DataFrame) -> String:
    base_dir = context.solid_config.get("base_dir")
    path = context.solid_config.get("path")
    repartition_ = context.solid_config.get("repartition")
    save_header = context.solid_config.get("save_header", True)
    rename = context.solid_config.get("rename", True)
    rename_stem = context.solid_config.get("rename_stem")
    single_file = context.solid_config.get("single_file", None)
    format_save_file = context.solid_config.get("format")
    options = context.solid_config.get("options")
    mode = context.solid_config.get("mode")
    show = context.solid_config.get("show")
    suffix = context.solid_config.get("rename_suffix")
    if format_save_file == "parquet":
        save_header = False

    full_path = utils.save_file(
        df=df,
        single_file=single_file,
        repartition_=repartition_,
        format_save_file=format_save_file,
        options=options,
        base_dir=base_dir,
        path=path,
        mode=mode,
        saveing_header=save_header,
        rename=rename,
        rename_stem=rename_stem,
        suffix=suffix,
        show=show,
    )

    context.log.debug(f"Finished saving df to: {full_path}")
    context.log.debug(f"df Columns: {df.columns}")

    yield Output(str(path), "path")


@solid(
    config_schema={
        "num_rows": Field(
            Int,
            is_required=False,
            default_value=5,
            description="Number of rows to display",
        ),
    }
)
def show(context, df: DataFrame):
    num_rows = context.solid_config.get("num_rows")
    context.log.info(f"df.show():\n{df._jdf.showString(num_rows, 20, False)}")


@solid
def count(context, df: DataFrame):
    context.log.info(f"df count: {df.count()}")


@solid(
    config_schema={
        "stem": Field(
            String,
            is_required=True,
            description="The name to rename the exported pyspark file",
        ),
        "suffix": Field(
            String,
            default_value=".csv",
            is_required=False,
            description="The suffix for the renamed file",
        ),
    }
)
def rename_hdfs_files_solid(context, file_path: String):
    """
        rename exported files that are generated from pyspark
    """
    stem = context.solid_config.get("stem")
    suffix = context.solid_config.get("suffix")

    context.log.debug(
        f"try to rename files in path: {file_path} with stem: {stem} and suffix: {suffix}"
    )
    utils.rename_hdfs_files(file_path, stem, suffix)


@solid(
    config_schema={
        "hdfs_directory": Field(
            String, is_required=True, description="The hdfs path to query"
        ),
    }
)
def get_hdfs_files(context) -> Dict:
    pac = pyarrow.hdfs.connect()
    directory = context.solid_config.get("hdfs_directory")
    files = pac.ls(directory)
    files = [file.replace("hdfs://nameservice1/", "hdfs:///") for file in files]
    types_files = {
        k: [file for file in files if file.startswith(f"{directory}/{k}Doc")]
        for k in ["Car", "People", "Place"]
    }
    context.log.debug(f"types_files: {types_files}")
    return types_files


@solid(
    config_schema={
        "hdfs_path": Field(
            String, is_required=True, description="environment to run script"
        ),
    }
)
def get_hdfs_path_solid(context):
    hdfs_path = context.solid_config.get("hdfs_path")
    return hdfs_path
