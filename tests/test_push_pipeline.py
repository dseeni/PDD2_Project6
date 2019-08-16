import pytest
from src.push_pipeline import *
from inspect import getgeneratorlocals


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


def test_header_extract(test_sink):
    with file_readers(data_package) as readers:
        headers = header_extract(test_sink)
        row_cycler = cycle_rows(headers)
        row_cycler.send(readers)
        header_rows = getgeneratorlocals(test_sink)['ml']
        print('25:', *header_rows, sep='\n')
        assert header_rows[0][0] == 'car'
        assert header_rows[1][0] == 'employer'
        assert header_rows[2][0] == 'summons_number'


@pytest.mark.skip
def test_gen_field_names(test_sink):
    field_names = gen_field_names(test_sink)
    field_names.send(class_names[0])

    with file_readers(fnames[0]) as f:
        # header_row = next(f)
        headers = tuple(map(lambda l: l.lower(), next(f)))
        field_names.send(headers)
    dummy_nt = getgeneratorlocals(test_sink)['ml']
    obj_properties = ['acceleration', 'car', 'cylinders', 'displacement',
                      'horsepower', 'mpg', 'model', 'origin']
    assert all(getattr(dummy_nt, attr) for attr in obj_properties)


def test_file_readers(test_sink):
    with file_readers(data_package) as readers:
        # check the first element in each files header row:
        assert(next(readers[0])[0]) == 'Car'
        assert(next(readers[1])[0]) == 'employer'
        assert(next(readers[2])[0]) == 'Summons Number'
        assert(next(readers[3])[0]) == 'ssn'
        assert(next(readers[4])[0]) == 'ssn'
        data_rows = []
        for _ in range(len(readers)):
            try:
                while True:
                    data_rows.append(next(readers[_]))
            except StopIteration:
                continue
            except IndexError:
                break
        assert len(data_rows) == 4406


# @pytest.mark.skip
def test_date_key_gen(test_sink, test_file_reader):

    # cars.csv
    delimited_row1 = test_file_reader[0]
    # nyc_parking_tickets_extract.csv
    delimited_row2 = test_file_reader[1]
    # update_status.csv
    delimited_row3 = test_file_reader[2]

    date_parser = date_key_gen(test_sink)
    gen_row_key = row_key_gen(date_parser)

    date_parser.send(date_keys)  # <- normally sent by pipeline_coro()
    date_parser.send(delimited_row3)

    # date_parser.send(delimited_row3)
    gen_row_key.send(delimited_row3)

    row_key = getgeneratorlocals(test_sink)['ml']
    assert row_key[0] == str

    datefunc1 = getgeneratorlocals(test_sink)['ml'][1]
    datefunc2 = getgeneratorlocals(test_sink)['ml'][2]
    date1 = datefunc1('2017-10-07T00:14:42Z')
    date2 = datefunc2('2016-01-24T21:19:30Z')

    # def check_date(date_obj, year, month, day, hour, minute, second):
    def check_date(date_obj,*args):
        time = ('year', 'month', 'day', 'hour', 'minute', 'second')
        return list(getattr(date_obj, value) for value in time) == [*args]
    assert check_date(date1, 2017, 10, 7, 0, 14, 42)
    assert check_date(date2, 2016, 1, 24, 21, 19, 30)


# @pytest.mark.skip
def test_row_key_gen(test_sink, test_file_reader):
    # # cars.csv
    # delimited_row0 = test_data_rows[0]
    # # nyc_parking_tickets_extract.csv
    # delimited_row1 = test_data_rows[1]
    # # update_status.csv
    # delimited_row2 = test_data_rows[2]

    # reference keys
    # cars.csv
    test_key0 = (str, float, int, float, float, float, float, int, str)
    # nyc_parking_tickets_extract.csv
    test_key1 = (int, str, str, str, str, int, str, str, str)
    # update_status.csv
    test_key2 = (str, str, str)
    
    test_keys = zip(test_key0, test_key1, test_key2)

    def check_key(row_keys, ref_keys):
        for value, ref in list(zip(row_keys, ref_keys)):
            return value == ref

    gen_row_key = row_key_gen(test_sink)
    gen_row_key.send(test_file_reader)
    parsed_key0 = getgeneratorlocals(test_sink)['ml'][0]
    print('p0', parsed_key0)
    assert check_key(parsed_key0, test_key0)

    parsed_key1 = getgeneratorlocals(test_sink)['ml'][1]
    print('p1', parsed_key1)
    assert check_key(parsed_key1, test_key1)

    parsed_key2 = getgeneratorlocals(test_sink)['ml'][2]
    print('p2', parsed_key2)
    assert check_key(parsed_key2, test_key2)
