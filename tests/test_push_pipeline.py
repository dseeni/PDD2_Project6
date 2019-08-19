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
        assert (next(readers[0])[0]) == 'Car'
        assert (next(readers[1])[0]) == 'employer'
        assert (next(readers[2])[0]) == 'Summons Number'
        assert (next(readers[3])[0]) == 'ssn'
        assert (next(readers[4])[0]) == 'ssn'
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


# TODO: Verify that output and parsed_row can handle multi-format date parsing
# @pytest.mark.skip
def test_date_key(test_sink, test_file_reader, get_test_date, date_tester):
    f_idxs = (0, 2, 4)
    sink_keys = tuple('ml' for _ in range(3))
    date_key_idxs = (0, 1, 1)
    out_idxs = ((1, 4), (2, 2), (2, 1))
    raw_date_strs = ('10/5/2016', '2016-01-24T21:19:30Z',
                     '2017-10-07T00:14:42Z')

    date_tester(test_sink, test_file_reader(f_idxs), get_test_date,
                key_names=sink_keys, date_format_key_idxs=date_key_idxs,
                output_idxs=out_idxs, date_strs=raw_date_strs)

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
    f_idxs = (0, 2, 4)

    test_keys = zip(test_key0, test_key1, test_key2)

    def check_key(row_keys, ref_keys):
        for value, ref in list(zip(row_keys, ref_keys)):
            return value == ref

    gen_row_key = row_key_gen(test_sink)
    gen_row_key.send(test_file_reader(f_idxs))
    parsed_key0 = getgeneratorlocals(test_sink)['ml'][0]
    print('p0', parsed_key0)
    assert check_key(parsed_key0, test_key0)

    parsed_key1 = getgeneratorlocals(test_sink)['ml'][1]
    print('p1', parsed_key1)
    assert check_key(parsed_key1, test_key1)

    parsed_key2 = getgeneratorlocals(test_sink)['ml'][2]
    print('p2', parsed_key2)
    assert check_key(parsed_key2, test_key2)


def test_date_parser(test_sink):
    dk2 = '%Y-%m-%dT%H:%M:%SZ'
    dk1 = '%m/%d/%Y'
    date_str1 = '10/5/2016'
    date_str2 = '2016-01-24T21:19:30Z'
    dkeys = (dk1, dk2)
    date_func1 = (lambda v: datetime.strptime(v, dkeys[0]))
    date_func2 = (lambda v: datetime.strptime(v, dkeys[1]))

    assert date_func1(date_str1).day == 5
    assert date_func1(date_str1).month == 10
    assert date_func1(date_str1).year == 2016

    assert date_func2(date_str2).day == 24
    assert date_func2(date_str2).month == 1
    assert date_func2(date_str2).year == 2016
