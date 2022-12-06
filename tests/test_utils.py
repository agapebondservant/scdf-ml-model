import pandas as pd
import numpy as np
from scdfutils import utils


def test_valid_date_index():
    arr = [['2022-11-30 11:44:19.855571+00:00', 111, 222], ['2022-11-30 11:49:19.855571+00:00', 1111, 2222]]
    cols = ['one', 'two']

    dataframe = pd.DataFrame([], columns=cols)
    assert not utils.append_json_list_to_dataframe(dataframe, arr, use_epoch_as_datetime=False).empty, 'Failed to handle valid date index'


def test_invalid_date_index():
    arr = [['2022-11-30 11:44:19.855571+00:00', 111, 222], ['invalid date', 1111, 2222]]
    cols = ['one', 'two']

    dataframe = pd.DataFrame([], columns=cols)
    assert utils.append_json_list_to_dataframe(dataframe, arr, use_epoch_as_datetime=False).empty, 'Failed to handle invalid date index'


def test_null_date_index():
    arr = [['2022-11-30 11:44:19.855571+00:00', 111, 222], [None, 1111, 2222]]
    cols = ['one', 'two']

    dataframe = pd.DataFrame([], columns=cols)
    assert utils.append_json_list_to_dataframe(dataframe, arr, use_epoch_as_datetime=False).empty, 'Failed to handle null date index'


def test_numeric_and_string_parsing_in_dataframe():
    arr = [['2022-11-30 11:44:19.855571+00:00', '111', '222'], ['2022-11-30 11:49:19.855571+00:00', '333', 'you']]
    cols = ['one', 'two']

    dataframe = pd.DataFrame([], columns=cols)
    dataframe = utils.append_json_list_to_dataframe(dataframe, arr, use_epoch_as_datetime=False)
    assert len(dataframe.select_dtypes(include=np.number).columns) == 1, 'Number of numeric columns does not match expected'


def test_initialize_time_series_dataframe():
    # 'x': x, 'xlabel': f"Hello you", 'target': y, 'prediction'
    arr = [['2022-11-30 11:44:19.855571+00:00', '111', 'Hello', '18', '5.0'], ['2022-11-30 11:49:19.855571+00:00', '111', 'Hello', '20', '6.0']]
    dataframe = utils.initialize_timeseries_dataframe(arr, 'tests/data/schema.csv')
    print(dataframe)
    assert dataframe.columns.isin(['x', 'xlabel', 'target', 'prediction']).all(), 'Columns do not match expected values'
    assert len(dataframe) == 2, 'Size of dataframe does not match expected value'
    assert type(dataframe.index) == pd.core.indexes.datetimes.DatetimeIndex, 'Index of dataframe does not match expected data type'
