import pytest
from src.push_pipeline import *
from inspect import getgeneratorlocals
from unittest.mock import patch

# TODO: Setup independent test_cars.csv folder/file and filters to test
@pytest.mark.skip
def test_save_data():
    data_writer = save_data('test_file.csv', ['test_headers'], 'output_data')
    data_writer.send(['this is a test line'])
    data_writer.close()
    # print('currentdirectory', os.getcwd())
    os.chdir('output_data')
    with open('test_file.csv') as tf:
        assert next(tf) == 'test_headers\n'
        assert next(tf) == 'this is a test line\n'


def test_header_extract():
    # header_reader = header_extract()
    # header extract needs key
    pass


def test_gen_field_names(dummy_target):
    field_names = gen_field_names(dummy_target)
    field_names.send(class_names[0])

    with file_handler(fnames[0]) as f:
        header_row = next(f)
        field_names.send(header_row)
    dummy_nt = getgeneratorlocals(dummy_target)['ml']
    # nt = next(dummy_target)
    # print(nt)
    assert 'Acceleration' in dir(dummy_nt)
    assert 'Car' in dir(dummy_nt)
    assert 'Cylinders' in dir(dummy_nt)
    assert 'Displacement' in dir(dummy_nt)
    assert 'Horsepower' in dir(dummy_nt)
    assert 'MPG' in dir(dummy_nt)
    assert 'Model' in dir(dummy_nt)
    assert 'Origin' in dir(dummy_nt)


def test_pipeline_handler(dummy_target):
    dummy = dummy_target
    with file_handler(fnames[0]) as ph:
        assert(next(ph)[0]) == 'Car'


def test_date_key_gen(dummy_target, dummy_reader):

    # cars.csv
    delimited_row1 = dummy_reader[0]
    # nyc_parking_tickets_extract.csv
    delimited_row2 = dummy_reader[1]
    # update_status.csv
    delimited_row3 = dummy_reader[2]

    date_parser = date_key_gen(dummy_target)
    gen_row_key = row_key_gen(date_parser)

    date_parser.send(date_keys)  # <- normally sent by pipeline_coro()
    date_parser.send(delimited_row3)

    # date_parser.send(delimited_row3)
    gen_row_key.send(delimited_row3)

    row_key = getgeneratorlocals(dummy_target)['ml']
    assert row_key[0] == str

    datefunc1 = getgeneratorlocals(dummy_target)['ml'][1]
    datefunc2 = getgeneratorlocals(dummy_target)['ml'][2]
    date1 = datefunc1('2017-10-07T00:14:42Z')
    date2 = datefunc2('2016-01-24T21:19:30Z')

    # def check_date(date_obj, year, month, day, hour, minute, second):
    def check_date(date_obj,*args):
        time = ('year', 'month', 'day', 'hour', 'minute', 'second')
        return list(getattr(date_obj, value) for value in time) == [*args]
    assert check_date(date1, 2017, 10, 7, 0, 14, 42)
    assert check_date(date2, 2016, 1, 24, 21, 19, 30)


def test_row_key_gen(dummy_target, dummy_reader):
    # cars.csv
    delimited_row0 = dummy_reader[0]
    # nyc_parking_tickets_extract.csv
    delimited_row1 = dummy_reader[1]
    # update_status.csv
    delimited_row2 = dummy_reader[2]
    # reference keys
    test_key0 = (str, float, int, float, float, float, float, int, str)
    test_key1 = (int, str, str, str, str, int, str, str, str)
    test_key2 = (str, str, str)

    def check_key(row_key, test_key):
        for value, ref in list(zip(row_key, test_key)):
            return value == ref

    gen_row_key = row_key_gen(dummy_target)
    gen_row_key.send(delimited_row0)
    parsed_key0 = getgeneratorlocals(dummy_target)['ml']
    assert check_key(parsed_key0, test_key0)

    gen_row_key.send(delimited_row1)
    parsed_key1 = getgeneratorlocals(dummy_target)['ml']
    assert check_key(parsed_key1, test_key1)

    gen_row_key.send(delimited_row2)
    parsed_key2 = getgeneratorlocals(dummy_target)['ml']
    assert check_key(parsed_key2, test_key2)
