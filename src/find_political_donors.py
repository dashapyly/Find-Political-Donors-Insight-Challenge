"""
531
"""
import datetime
import logging
import os
from collections import Counter, defaultdict
from typing import Iterator, Optional, Tuple, Dict

import pandas as pd

from running_median import RunningMedian

logger = logging.getLogger(__name__)

COLUMN_NAMES = [
    'CMTE_ID', 'AMNDT_IND', 'RPT_TP', 'TRANSACTION_PGI', 'IMAGE_NUM', 'TRANSACTION_TP', 'ENTITY_TP',
    'NAME', 'CITY', 'STATE', 'ZIP_CODE', 'EMPLOYER', 'OCCUPATION', 'TRANSACTION_DT', 'TRANSACTION_AMT',
    'OTHER_ID', 'TRAN_ID', 'FILE_NUM', 'MEMO_CD', 'MEMO_TEXT', 'SUB_ID'
]
COLUMN_TYPES = {'ZIP_CODE': str, 'TRANSACTION_DT': str, 'OTHER_ID': str}
USECOLS = ['CMTE_ID', 'ZIP_CODE', 'TRANSACTION_DT', 'TRANSACTION_AMT', 'OTHER_ID']


def calculate_statistics_by_zip_code(
        input_stream: pd.DataFrame, run_med: Dict[Tuple[str, str], float], run_count: Dict[Tuple[str, str], int],
        run_total: Dict[Tuple[str, str], float]) -> pd.DataFrame:
    """Calculate zip code-level statistics for data in the `input_stream`.

    Args:
        input_stream: DataFrame containing one or more rows of data to be processed.
        run_med: Dictionary mapping (CMTE_ID, ZIP_CODE) tuples to objects calculating the running median.
        run_count: Dictionary mapping (CMTE_ID, ZIP_CODE) tuples to the running number of contributions.
        run_total: Dictionary mapping (CMTE_ID, ZIP_CODE) tuples to the running total of contributions.

    Returns:
        DataFrame containing zip code statistics for the provided data.
    """
    input_stream = input_stream[
        (input_stream.OTHER_ID.isnull()) &
        (input_stream.ZIP_CODE.str.len() >= 5) &
        (input_stream.CMTE_ID.notnull()) &
        (input_stream.TRANSACTION_AMT.notnull())
        ].copy()
    input_stream['ZIP_CODE_5CHAR'] = input_stream.ZIP_CODE.str[:5]

    input_stream['RUN_MED_ZIP'] = input_stream.apply(
        lambda s: increment_run_med((s['CMTE_ID'], s['ZIP_CODE_5CHAR']), s['TRANSACTION_AMT'], run_med), axis=1)

    input_stream['RUN_COUNT_ZIP'] = input_stream.apply(
        lambda s: increment_run_count((s['CMTE_ID'], s['ZIP_CODE_5CHAR']), run_count), axis=1)

    input_stream['RUN_TOTAL_ZIP'] = input_stream.apply(
        lambda s: increment_run_total((s['CMTE_ID'], s['ZIP_CODE_5CHAR']), s['TRANSACTION_AMT'], run_total), axis=1)

    input_stream.RUN_MED_ZIP = input_stream.RUN_MED_ZIP.apply(round_amount)

    return input_stream[['CMTE_ID', 'ZIP_CODE_5CHAR', 'RUN_MED_ZIP', 'RUN_COUNT_ZIP', 'RUN_TOTAL_ZIP']]


def calculate_statistics_by_date(input_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate daily statistics for data in `input_df`.

    Args:
        input_df: DataFrame containing *all* the data for which to calculate statistics.

    Returns:
        DataFrame containing statistics for input data aggregated by date.
    """
    input_df = input_df[
        (input_df.OTHER_ID.isnull()) &
        (input_df.TRANSACTION_DT.apply(validate_transaction_dt)) &
        (input_df.CMTE_ID.notnull()) &
        (input_df.TRANSACTION_AMT.notnull())
        ]
    grouped = \
        input_df \
            .groupby(['CMTE_ID', 'TRANSACTION_DT']) \
            ['TRANSACTION_AMT'] \
            .agg({'RUN_MED_DATE': 'median', 'RUN_COUNT_DATE': 'count', 'RUN_TOTAL_DATE': 'sum'}) \
            .reset_index()
    grouped.RUN_MED_DATE = grouped.RUN_MED_DATE.apply(round_amount)
    grouped = sort_by_date(grouped)
    return grouped[['CMTE_ID', 'TRANSACTION_DT', 'RUN_MED_DATE', 'RUN_COUNT_DATE', 'RUN_TOTAL_DATE']]


def validate_transaction_dt(transaction_dt: str) -> bool:
    """Make sure that the transaction date (TRANSACTION_DT) is valid.

    Args:
        transaction_dt: Transaction date (TRANSACTION_DT column)

    Returns:
        ``True`` if the transaction date is valid, ``False`` otherwise.
    """
    if pd.isnull(transaction_dt):
        return False
    month = transaction_dt[:2]
    day = transaction_dt[2:4]
    year = transaction_dt[4:]
    is_valid = (1 <= int(month) <= 12) and (1 <= int(day) <= 31) and (1000 <= int(year) <= 2020)
    return is_valid


def round_amount(amount: float) -> Optional[int]:
    """Round `amount` to the nearest integer.

    Round anything below 0.5 down to the nearest integer,
    and anything above 0.5, inclusive, up to the nearest integer.

    Args:
        amount: Amount to round.

    Returns:
        Rounded amount.
    """
    if pd.isnull(amount):
        return None
    else:
        return int(amount) + int(amount % 1 >= 0.5)


def increment_run_med(key: Tuple[str, str], value: float, run_med: Dict[Tuple[str, str], float]):
    """Update running median.

    Args:
        key: CMTE_ID and ZIP_CODE for which to make the update.
        value: Contribution amount.
        run_med: Dictionary mapping keys to RunningMedian class instances.

    Returns:
        The updated median for the given key.
    """
    rolling_median = run_med[key]
    rolling_median.add(value)
    return rolling_median.median


def increment_run_count(key: Tuple[str, str], run_count: Dict[Tuple[str, str], int]) -> int:
    """Update running count.

    Args:
        key: CMTE_ID and ZIP_CODE for which to make the update.
        run_count: Dictionary storing the number of contributions for each key.

    Returns:
        The updated number of contributions for the given key.
    """
    run_count[key] += 1
    return run_count[key]


def increment_run_total(key: Tuple[str, str], value: float, run_total: Dict[Tuple[str, str], float]):
    """Update running total.

    Args:
        key: CMTE_ID and ZIP_CODE for which to make the update.
        value: Contribution amount.
        run_total: Dictionary storing total contributions for each key.

    Returns:
        The updated total of contributions for the given key.
    """
    run_total[key] += value
    return run_total[key]


def sort_by_date(input_df: pd.DataFrame) -> pd.DataFrame:
    """Sort DataFrame by CMTE_ID and TRANSACTION_DT columns.

    Args:
        input_df: DataFrame to sort.

    Returns:
        Sorted DataFrame.
    """
    input_df['year'] = input_df['TRANSACTION_DT'].str[4:8].astype(int)
    input_df['month'] = input_df['TRANSACTION_DT'].str[0:2].astype(int)
    input_df['day'] = input_df['TRANSACTION_DT'].str[2:4].astype(int)
    input_df.sort_values(['CMTE_ID', 'year', 'month', 'day'], inplace=True)
    input_df.drop(pd.Index(['year', 'month', 'day']), axis=1, inplace=True)
    return input_df


def iter_input_file(input_file: str, chunksize: int) -> Iterator[pd.DataFrame]:
    """Read the input file in chunks.

    Args:
        input_file: Filename of the input file.
        chunksize: Number of rows to read in each chunk.

    Returns:
        DataFrame containing the data of each chunk
    """
    input_stream = pd.read_csv(
        input_file, sep='|', names=COLUMN_NAMES, usecols=USECOLS, dtype=COLUMN_TYPES, chunksize=chunksize)
    return input_stream


def read_input_file(input_file: str) -> pd.DataFrame:
    """Read the entire input file into a DataFrame.

    Args:
        input_file: Filename of the input file.

    Returns:
        DataFrame containing the data in the input file.
    """
    input_df = pd.read_csv(input_file, sep='|', names=COLUMN_NAMES, usecols=USECOLS, dtype=COLUMN_TYPES)
    return input_df


def main(input_file: str, output_file_zip: str, output_file_date: str) -> None:
    """Calculate statistics for federal election campaign contributions.

    Args:
        input_file: Path to the input file.
        output_file_zip: Path to the file where to write running statistics by ZIP code.
        output_file_date: Path to the file where to write statistics by date.
    """
    run_med = defaultdict(RunningMedian)
    run_count = Counter()
    run_total = Counter()

    logging.info("Generating '%s'...", output_file_zip)
    try:
        os.remove(output_file_zip)
    except FileNotFoundError:
        pass
    for chunk_idx, chunk in enumerate(iter_input_file(input_file, 100_000)):
        logger.debug("Chunk no: %i", chunk_idx)
        started = datetime.datetime.now()
        chunk_zip_stats = calculate_statistics_by_zip_code(chunk, run_med, run_count, run_total)
        chunk_zip_stats.to_csv(output_file_zip, mode='a', sep='|', index=False, header=False)
        runtime = (datetime.datetime.now() - started).total_seconds()
        logger.debug("Evaluated chunk in %s seconds", runtime)

    logging.info("Generating '%s'...", output_file_date)
    input_df = read_input_file(input_file)
    date_stats = calculate_statistics_by_date(input_df)
    date_stats.to_csv(output_file_date, mode='a', sep='|', index=False, header=False)

    logging.info("Done!")


if __name__ == '__main__':
    import sys

    logging.basicConfig(level=logging.DEBUG)
    main(sys.argv[1], sys.argv[2], sys.argv[3])
