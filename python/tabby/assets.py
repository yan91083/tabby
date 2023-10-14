from dagster import Out, Output, MetadataValue, asset
from dagster_pandas import DataFrame, PandasColumn, create_dagster_pandas_dataframe_type

import pandas as pd
import json
import glob

from . import constants

DatasetDataFrame = create_dagster_pandas_dataframe_type(
    name="DatasetDataFrame",
    columns = [
        PandasColumn.string_column("git_url"),
        PandasColumn.string_column("filepath"),
        PandasColumn.string_column("content"),
        PandasColumn.string_column("language"),

        PandasColumn.integer_column("max_line_length"),
        PandasColumn.float_column("avg_line_length"),
        PandasColumn.float_column("alphanum_fraction"),

        PandasColumn.exists("tags"),
    ]
)


@asset(dagster_type=DatasetDataFrame)
def dataset_files():
    """Get source code information from TABBY_ROOT"""

    ds = []
    for path in glob.glob(constants.TABBY_DATASET_FILEPATTERN):
        with open(path, "r") as f:
            for line in f.readlines():
                ds.append(json.loads(line))

    df = pd.DataFrame(ds)
    return Output(df, metadata={"num_files": len(df) })
