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
    """Get source code information from TABBY_ROOT"""

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

EventDataFrame = create_dagster_pandas_dataframe_type(
    name="EventDataFrame",
    columns=[
        PandasColumn.integer_column("ts"),
        PandasColumn.exists("event"),
    ],
)

@asset(dagster_type=EventDataFrame)
def events():
    """Get events information from TABBY_ROOT"""

    ds = []
    for path in glob.glob(constants.TABBY_EVENTS_FILEPATTERN):
        with open(path, "r") as f:
            for line in f.readlines():
                ds.append(json.loads(line))

    df = pd.DataFrame(ds)
    metadata = {
        "num_records": len(df),
        "preview": MetadataValue.md(
            df.head()[
                [
                    "ts",
                    "event"
                ]
            ].to_markdown()
        ),
    }
    return Output(df, metadata=metadata)
