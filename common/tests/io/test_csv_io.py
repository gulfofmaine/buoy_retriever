from pathlib import Path

import dagster as dg
import pandas as pd

from common.io import tags as io
from common.io.csv_io import PandasCsvIoManager
from common.io.datastore import Datastore


def test_csv_io_handle_unpartitioned_output(tmp_path: Path):
    """Test we can write and read a CSV file with the CSV IO Manager"""
    datastore = Datastore(path_stub="test_stub", test_path=str(tmp_path))
    csv_io = PandasCsvIoManager(datastore=datastore)

    output_context = dg.build_output_context(
        definition_metadata={io.DESIRED_PATH: "test_output.csv"},
    )

    # Create a sample DataFrame
    data = {
        "a": [1, 2, 3],
        "b": [4, 5, 6],
    }

    df_to_write = pd.DataFrame(data)

    # Handle output (write the DataFrame to CSV)
    csv_io.handle_output(output_context, df_to_write)

    # Handle input (read the DataFrame from CSV)
    df_read = pd.read_csv(tmp_path / "test_stub" / "test_output.csv")

    # Verify that the written and read DataFrames are the same
    pd.testing.assert_frame_equal(df_to_write, df_read)


def test_csv_io_handle_partitioned_output(tmp_path: Path):
    """Test we can write and read a partitioned CSV file with the CSV IO Manager"""
    datastore = Datastore(path_stub="test_stub", test_path=str(tmp_path))
    csv_io = PandasCsvIoManager(datastore=datastore)

    partition_key = "2023-10-01"
    output_context = dg.build_output_context(
        definition_metadata={
            io.DESIRED_PATH: "partitions/{partition_key_dt:%Y-%m-%d}.csv",
        },
        partition_key=partition_key,
    )

    # Create a sample DataFrame
    data = {
        "x": [10, 20, 30],
        "y": [40, 50, 60],
    }

    df_to_write = pd.DataFrame(data)

    # Handle output (write the DataFrame to CSV)
    csv_io.handle_output(output_context, df_to_write)

    # Handle input (read the DataFrame from CSV)
    df_read = pd.read_csv(
        tmp_path / "test_stub" / "partitions" / f"{partition_key}.csv",
    )

    # Verify that the written and read DataFrames are the same
    pd.testing.assert_frame_equal(df_to_write, df_read)


def test_csv_io_load_unpartitioned_input(tmp_path: Path):
    """Test we can read a CSV file with the CSV IO Manager"""
    datastore = Datastore(path_stub="test_stub", test_path=str(tmp_path))
    csv_io = PandasCsvIoManager(datastore=datastore)

    # Create a sample DataFrame and write it to CSV
    data = {
        "m": [7, 8, 9],
        "n": [10, 11, 12],
    }

    df_to_write = pd.DataFrame(data)
    test_dir = tmp_path / "test_stub"
    test_dir.mkdir(parents=True, exist_ok=True)
    df_to_write.to_csv(test_dir / "input_data.csv", index=False)

    # Now build an input context to read the CSV
    input_context = dg.build_input_context(
        upstream_output=dg.build_output_context(
            definition_metadata={io.DESIRED_PATH: "input_data.csv"},
        ),
    )

    # Load the DataFrame from CSV
    df_read = csv_io.load_input(input_context)

    # Verify that the written and read DataFrames are the same
    pd.testing.assert_frame_equal(df_to_write, df_read)


# def test_csv_io_load_partitioned_input(tmp_path: Path):
#     """Test we can read a partitioned CSV file with the CSV IO Manager"""
#     datastore = Datastore(path_stub="test_stub", test_path=str(tmp_path))
#     csv_io = PandasCsvIoManager(datastore=datastore)

#     partition_key = "2023-11-15"

#     # Create a sample DataFrame and write it to CSV
#     data = {
#         "p": [13, 14, 15],
#         "q": [16, 17, 18],
#     }

#     df_to_write = pd.DataFrame(data)
#     test_dir = tmp_path / "test_stub" / "partitions"
#     test_dir.mkdir(parents=True, exist_ok=True)
#     for key in ["2025-11-01", "2025-11-03"]:
#         df_to_write.to_csv(test_dir / f"{key}.csv", index=False)

#     # Now build an input context to read the partitioned CSV
#     input_context = dg.build_input_context(
#         upstream_output=dg.build_output_context(
#             metadata={io.DESIRED_PATH: "partitions/{partition_key_dt:%Y-%m-%d}.csv"},
#             # partition_key=partition_key,
#         ),
#         dagster_type=dg.PythonObjectDagsterType(python_type=dict),
#         asset_partition_key_range=dg.PartitionKeyRange(start="2025-11-01", end="2025-11-03")
#     )

#     # Load the DataFrame from CSV
#     dfs_read = csv_io.load_input(input_context)

#     # Verify that the written and read DataFrames are the same
#     for key, df_read in dfs_read.items():
#         pd.testing.assert_frame_equal(df_to_write, df_read)
