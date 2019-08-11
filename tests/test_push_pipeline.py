import pytest
from src.push_pipeline import *
from inspect import getgeneratorlocals


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


def test_gen_date_parser(dummy_target):
    date_parser1 = gen_date_parser(dummy_target)
    date_parser2 = gen_date_parser(dummy_target)

    date_parser1.send(date_keys)
    date_parser1.send('2017-10-07T00:14:42Z')

    datefunc = getgeneratorlocals(dummy_target)['ml']
    date = datefunc('2017-10-07T00:14:42Z')
    assert date.year == 2017
    assert date.day == 7
    assert date.month == 10

    # '2017-10-07T00:14:42Z'
    # '2017-10-07T00:14:42Z'
    # '12/12/2012'
    # '12/x2/2012'
    # '12/12/2012'

    # test_date1 = date_parser1('2017-10-07T00:14:42Z')
    # test_date_2 = date_parser2('12/12/2012')
    # test_date1 = date_parser1('2017-10-07T00:14:42Z')

    # assert next(gen_date_parser('12/x2/2012', date_keys)) is None
    #
    # assert type(test_date_2) == datetime
    # assert type(test_date1) == datetime
    #
    # assert test_date1.year == 2017
    # assert test_date1.month == 10
    # assert test_date1.day == int('07')


@pytest.mark.skip
def test_infer_data_type(dummy_target):  # from --> sample_data
    file_str1 = "Chevrolet Chevelle Malibu;18.0;8;307.0;130.0;3504.;12.0;70;US"
    file_str2 = "100-53-9824,2017-10-07T00:14:42Z,2016-01-24T21:19:30Z"

    data_row_2 = file_str1.split(';')
    data_row_1 = file_str2.split(';')

    infer_func = gen_row_parse_key(dummy_target)
    parsed_data = infer_func.send(data_row_1)
    assert parsed_data[0] == data_row_1[0]
    parsed_data = infer_func.send(data_row_1)
    assert parsed_data[0] == data_row_1[0]

    parsed_data = infer_func.send(data_row_2)
    assert parsed_data[0] == data_row_2[0]
    # print('87:', 'getgeneratorlocals(dummy) ''='' ', getgeneratorlocals(dummy_target))
    parsed_data = infer_func.send(data_row_2)
    assert parsed_data[0] == data_row_2[0]

