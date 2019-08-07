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
    pass


def test_parse_date():
    test_date = next(parse_date('2017-10-07T00:14:42Z', date_keys))
    assert test_date.year == 2017
    assert test_date.month == 10
    assert test_date.day == int('07')
    assert type(next(parse_date('12/x2/2012', date_keys))) == str
    assert type(next(parse_date('12/12/2012', date_keys))) == datetime
    assert type(next(parse_date('2017-10-07T00:14:42Z', date_keys))) == datetime
