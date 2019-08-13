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
    print('currentdirectory', os.getcwd())
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

    assert all(isinstance(date, datetime) for date in (date1, date2))

    assert date1.year == 2017
    assert date1.month == 10
    assert date1.day == 7
    assert date1.hour == 0
    assert date1.minute == 14
    assert date1.second == 42

    assert date2.year == 2016
    assert date2.month == 1
    assert date2.day == 24
    assert date2.hour == 21
    assert date2.minute == 19
    assert date2.second == 30


# noinspection DuplicatedCode
def test_row_key_gen(dummy_target, dummy_reader):

    def assert_type(test_key, reference_key):
        assert test_key == reference_key

    # cars.csv
    delimited_row0 = dummy_reader[0]
    # nyc_parking_tickets_extract.csv
    delimited_row1 = dummy_reader[1]
    # update_status.csv
    delimited_row2 = dummy_reader[2]

    gen_row_key = row_key_gen(dummy_target)
    # 'Chevrolet Chevelle Malibu;18.0;8;307.0;130.0;3504.;12.0;70;US'
    gen_row_key.send(delimited_row0)

    parsed_key1 = getgeneratorlocals(dummy_target)['ml']
    test_key1 = (str, float, int, float, float, float, float, int, str)
    for test, ref in list(zip(parsed_key1, test_key1)):
        assert test == ref
    # print('next', next(dummy_target))


    # '4006478550,VAD7274,VA,PAS,10/5/2016,5,4D,BMW,BUS LANE VIOLATION'

    gen_row_key.send(delimited_row1)
    parsed_key2 = getgeneratorlocals(dummy_target)['ml']

    print(getgeneratorlocals(dummy_target))
    assert parsed_key2[0] == int
    assert parsed_key2[1] == str
    assert parsed_key2[2] == str
    assert parsed_key2[3] == str
    assert parsed_key2[4] == str
    assert parsed_key2[5] == int
    assert parsed_key2[6] == str
    assert parsed_key2[7] == str
    assert parsed_key2[8] == str

    # '101-71-4702,2017-01-23T11:23:17Z,2016-01-27T04:32:57Z'
    gen_row_key.send(delimited_row2)
    parsed_key3 = getgeneratorlocals(dummy_target)['ml']
    assert (all(key == str for key in parsed_key3))
