from src.push_pipeline import *


def test_func():
    pass


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


def test_parse_date():
    test_date = parse_date('2017-10-07T00:14:42Z', date_keys)
    assert test_date.year == 2017
    assert test_date.month == 10
    assert test_date.day == int('07')
    assert parse_date('12/x2/2012', date_keys) is None
    assert type(parse_date('12/12/2012', date_keys)) == datetime
    assert type(parse_date('2017-10-07T00:14:42Z', date_keys)) == datetime


def test_infer_data_type(dummy_target):  # from --> sample_data
    file_str = "Chevrolet Chevelle Malibu;18.0;8;307.0;130.0;3504.;12.0;70;US"
    data_row = file_str.split(';')
    infer_func = row_parse_key_gen(dummy_target)
    parsed_data = infer_func.send(data_row)
    assert parsed_data[0] == data_row[0]
    parsed_data = infer_func.send(data_row)
    assert parsed_data[0] == data_row[0]

