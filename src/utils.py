import os
import platform
from pathlib import Path as PathLib

import pyarrow as pa
import pyspark.sql.column as Column
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from ruamel.yaml import YAML

yaml = YAML(typ="safe")


def is_function(val) -> bool:
    if isinstance(val, str):
        try:
            val = eval(val)
        except NameError:
            return False
    return callable(val)


def flatten_list(iterable):
    """
    :param iterable:
    :return:
    """
    for elem in iterable:
        if not isinstance(elem, list):
            yield elem
        else:
            for x in flatten_list(elem):
                yield x


def create_spark_con():
    builder = SparkSession.builder
    d = {"master": "yarn", "name": "fc_utils", "spark.executor.memoryOverhead": "8G"}
    for k, v in d.items():
        builder = builder.config(k, v)
    return builder.getOrCreate()


def save_header(df: DataFrame, path: str, sep: str):
    if str(path).startswith("hdfs:"):
        pac = pa.hdfs.connect()
        with pac.open(path, "wb") as bcf:
            bcf.write(f"{sep.join(df.columns)}".encode("utf8"))
    else:
        PathLib(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as f:
            f.write(f"{sep.join(df.columns)}".encode("utf8"))


def delete_files_from_hdfs_dir(hdfs_path: str):
    pc = pa.hdfs.connect()
    [pc.rm(hdfs_file) for hdfs_file in pc.ls(hdfs_path)]
    pc.close()


def rename_hdfs_files(
    file_path: str,
    parent: str = None,
    stem: str = None,
    suffix: str = None,
    pre_suffixes: tuple = (("part")),
) -> str:
    """
        rename files in path with {stem}{i}{suffix}
    """
    phc = pa.hdfs.connect()
    fl = phc.ls(file_path)

    fl = [
        PathLib(f)
        for f in fl
        if PathLib(f).stem.startswith(pre_suffixes)
        | PathLib(f).name.endswith(pre_suffixes)
    ]
    if not fl:
        return ""
    if not stem:
        stem = fl[0].parts[-2]
    if not suffix:
        suffix = fl[0].suffix
    if not parent:
        parent = fl[0].parent

    target_path_ = ""
    for i, f in enumerate(fl):
        new_file_name = f"{stem}{i}{suffix}"
        target_path = PathLib(parent, new_file_name)
        target_path_ = str(target_path).replace("hdfs:/", "hdfs://")
        f = str(f).replace("hdfs:/", "hdfs://")
        print(f"RENAME FILES FROM: {f} TO: {target_path_}")
        phc.mv(f"{f}", f"{target_path_}")
    phc.close()
    return f"{target_path_}"


def save_file(
    df: DataFrame,
    single_file=None,
    repartition_=None,
    format_save_file=None,
    options={},
    base_dir=".",
    path=None,
    mode=None,
    saveing_header=None,
    rename=None,
    rename_stem=None,
    suffix=None,
    show=None,
) -> str:

    cols_att = [c for c in df.columns if c.endswith(("_att", "_set"))]

    def stringify(c: Column):
        return F.concat(F.lit("["), F.concat_ws(",", c), F.lit("]"))

    for col in cols_att:
        df = df.withColumn(f"{col}", stringify(f"{col}"))

    full_path = f"{base_dir}/{path}"
    if not options:
        options = {}

    if single_file:
        (
            df.coalesce(1)
            .write.format(format_save_file)
            .options(**options)
            .save(path=full_path, mode=mode)
        )
    elif repartition_:
        (
            df.repartition(repartition_)
            .write.format(format_save_file)
            .options(**options)
            .save(path=full_path, mode=mode)
        )
    else:
        (
            df.write.format(format_save_file)
            .options(**options)
            .save(path=full_path, mode=mode)
        )

    if rename:
        if not rename_stem:
            rename_stem = "_".join(PathLib(path).parts)
        print(f"try to rename files in path: {path} with stem: {rename_stem} ")
        rename_hdfs_files(
            file_path=full_path, parent=None, stem=rename_stem, suffix=suffix
        )

    if saveing_header:
        if rename_stem:
            rename_stem = f'{rename_stem.split("_")[0]}_header'
        else:
            rename_stem = f"{PathLib(path).parts[0]}_header"
        path_file = f"{full_path}/{rename_stem}.csv"
        save_header(df, path_file, "|")
    if show:
        num_rows = 5
        print(f"df.show():\n{df._jdf.showString(num_rows, 20, False)}")

    return full_path


def yamls_to_dict(path_list: list) -> dict:
    yaml_dict = {}
    if not isinstance(path_list, list):
        path_list = list(path_list)
    for path in path_list:
        if not isinstance(path, dict):
            path = yaml.load(path)
        for k, v in path.items():
            if k in yaml_dict:
                yaml_dict[k].update(v)
            else:
                yaml_dict[k] = v
    return yaml_dict


def set_spark_environ():
    if platform.system() == "Windows":
        os.environ["JAVA_HOME"] = "C:/Progra~1/Java/jdk1.8.0_251"
        os.environ["SPARK_HOME"] = "C:/Spark/spark-2.4.6-bin-hadoop2.7"
    elif platform.system() == "Linux":
        # os.environ["JAVA_HOME"] = "/usr/lib64/jvm/jre-1.8.0-openjdk"
        os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/SPARK2/lib/spark2/"
    else:
        print(f"platform: {platform.system()} has not spesify JAVA/SPARK HOME path")
