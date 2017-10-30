from typing import Iterator

import pandas as pd

from running_median import RunningMedian


COLUMN_NAMES = [
    'CMTE_ID', 'AMNDT_IND', 'RPT_TP', 'TRANSACTION_PGI', 'IMAGE_NUM', 'TRANSACTION_TP', 'ENTITY_TP',
    'NAME', 'CITY', 'STATE', 'ZIP_CODE', 'EMPLOYER', 'OCCUPATION', 'TRANSACTION_DT', 'TRANSACTION_AMT',
    'OTHER_ID', 'TRAN_ID', 'FILE_NUM', 'MEMO_CD', 'MEMO_TEXT', 'SUB_ID'
]
COLUMN_TYPES = {'ZIP_CODE': str, 'TRANSACTION_DT': str}
USECOLS = ['CMTE_ID', 'ZIP_CODE', 'TRANSACTION_DT', 'TRANSACTION_AMT', 'OTHER_ID']


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


def iter_input_file(input_file: str, chunksize: int=100_000) -> Iterator[pd.DataFrame]:
    input_stream = pd.read_csv(
        input_file, sep='|', names=COLUMN_NAMES, usecols=USECOLS, dtype=COLUMN_TYPES, chunksize=chunksize)
    return input_stream


def read_input_file(input_file: str) -> pd.DataFrame:
    input_df = pd.read_csv(input_file, sep='|', names=COLUMN_NAMES, usecols=USECOLS, dtype=COLUMN_TYPES)
    return input_df


def main(input_file: str, output_file_zip: str, output_file_date: str) -> int:
    """Calculate statistics for federal election campaign contributions.

    Args:
        input_file: Path to the input file.
        output_file_zip: Path to the file where to write running statistics by ZIP code.
        output_file_date: Path to the file where to write statistics by date.

    Returns:
        System return code.
    """
    for chunk in iter_input_file(input_file):
        chunk_zip_stats = calculate_statistics_by_zip_code(chunk)
        chunk_zip_stats.to_csv(output_file_zip, mode='a', sep='|', index=False, header=False)

    input_df = read_input_file(input_file)
    date_stats = calculate_statistics_by_date(input_df)
    date_stats.to_csv(output_file_date, mode='a', sep='|', index=False, header=False)


if __name__ == '__main__':
    import sys

    sys.exit(main())
