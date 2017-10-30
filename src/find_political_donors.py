import pandas as pd

from running_median import RunningMedian


def calculate_statistics_by_zip_code(input_stream: pd.DataFrame) -> pd.DataFrame:
    """Calculate zip code-level statistics for data in the `input_stream`.

    Args:
        input_stream: DataFrame containing one or more rows of data to be processed.

    Returns:
        DataFrame containing zip code statistics for the provided data.
    """
    ...


def calculate_statistics_by_date(input_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate daily statistics for data in `input_df`.

    Args:
        input_df: DataFrame containing *all* the data for which to calculate statistics.

    Returns:
        DataFrame containing statistics for input data aggregated by date.
    """
    ...


def main(input_file: str, output_file_zip: str, output_file_date: str) -> int:
    """Calculate statistics for federal election campaign contributions.

    Args:
        input_file: Path to the input file.
        output_file_zip: Path to the file where to write running statistics by ZIP code.
        output_file_date: Path to the file where to write statistics by date.

    Returns:
        System return code.
    """
    ...


if __name__ == '__main__':
    import sys
    sys.exit(main())