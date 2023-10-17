from dagster import Out, Output, MetadataValue, asset
from dagster_pandas import DataFrame, PandasColumn, create_dagster_pandas_dataframe_type

import pandas as pd
import json
import glob

from . import constants

DatasetDataFrame = create_dagster_pandas_dataframe_type(
    name="DatasetDataFrame",
    columns=[
        PandasColumn.string_column("git_url"),
        PandasColumn.string_column("filepath"),
        PandasColumn.string_column("content"),
        PandasColumn.string_column("language"),
        PandasColumn.integer_column("max_line_length"),
        PandasColumn.float_column("avg_line_length"),
        PandasColumn.float_column("alphanum_fraction"),
        PandasColumn.exists("tags"),
    ],
)


@asset(dagster_type=DatasetDataFrame)
def dataset():
    """Read source code dataset from TABBY_ROOT"""

    ds = []
    for path in glob.glob(constants.TABBY_DATASET_FILEPATTERN):
        with open(path, "r") as f:
            for line in f.readlines():
                ds.append(json.loads(line))

    df = pd.DataFrame(ds)
    metadata = {
        "num_records": len(df),
        "preview": MetadataValue.md(
            df.head()[
                [
                    "git_url",
                    "filepath",
                    "language",
                    "max_line_length",
                    "avg_line_length",
                    "alphanum_fraction",
                ]
            ].to_markdown()
        ),
    }
    return Output(df, metadata=metadata)

@asset
def train_dataset(dataset):
    """Filter source code dataset for training / evaluation"""
    from datasets import Dataset

    df = dataset
    df = df[df["max_line_length"] < 300]
    df = df[df["avg_line_length"] < 150]
    metadata = {
        "num_records": len(df),
        "num_filtered_records": len(dataset) - len(df)
    }
    return Output(Dataset.from_pandas(df), metadata=metadata)