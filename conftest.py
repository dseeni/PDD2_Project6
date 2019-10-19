from pytest import fixture
from src.push_pipeline import *
# from inspect import getgeneratorstate, getgeneratorlocals
from contextlib import ExitStack
from inspect import getgeneratorlocals


@fixture('session', autouse=True)
def set_test_directory():
    os.chdir('src/')


# we send to dummy via the coroutines we're testing
# so we can't view a return value
@fixture('function')
def test_sink():
    @coroutine
    def _test_sink():
        ml = []
        while True:
            row = yield ml
            if row == 'clear':
                ml.clear()
            else:
                if iter(row) and type(row) is not str:
                    ml.append(list(row))
                else:
                    ml.append(row)
    return _test_sink()


@fixture('function')
def get_test_date():
    def _get_test_date(gen_name, list_name, access_idxs):
        nested_list = getgeneratorlocals(gen_name)[list_name]
        print('37:', 'nested_list ''='' ', nested_list)
        idx = [arg for arg in access_idxs]
        print('35:', 'idx ''='' ', idx)
        current = list(nested_list)
        print('38:', 'current ''='' ', current)
        for i in range(len(idx)):
            try:
                if iter(current[idx[i]]):
                    current = current[idx[i]]
            except TypeError:
                continue
            finally:
                current = current[idx[-1]]
                return current
    return _get_test_date


@fixture('function')
def date_tester():
    def _date_tester(sink, reader_rows, date_getter, key_names,
                     date_format_key_idxs, access_idxs, date_strs):
        for s in range(len(date_strs)):
            date_parser = date_key_gen(sink)
            row_key_gen_targets = (sink, date_parser)
            gen_row_key = row_key_gen(row_key_gen_targets)
            # force the testing of only 1 date format key per iteration:
            date_parser.send((date_keys[date_format_key_idxs[s]],
                              date_keys[date_format_key_idxs[s]]))
            date_parser.send(reader_rows)
            gen_row_key.send(reader_rows)
            get_date_func = date_getter(sink, key_names[s], access_idxs[s])
            print('65:', 'get_date_func ''='' ', get_date_func)
            date1 = get_date_func(date_strs[s])
            print('68:', 'date1 ''='' ', date1)
            # raise
            assert date1 == datetime.strptime(date_strs[s], date_keys[
                date_format_key_idxs[s]])
            sink.send('clear')
    return _date_tester


@fixture('function')
def sample_reader_rows():
    def _sample_reader_rows(file_idxs, headers=False):
        raw_data_list = []
        partial_files = tuple(fnames[i] for i in file_idxs)
        # faking a data package with no name_tuple or tuple (outfile,predicates)
        fnames_only_package = tuple(((i, None), None) for i in partial_files)
        with file_readers(fnames_only_package) as readers:
            # print('38:', 'readers ''='' ', readers)
            for reader in readers:
                header_rows = next(reader)
                if headers:
                    raw_data_list.append(header_rows)
                else:
                    raw_data_list.append(next(reader))
        # print('40:', 'raw_data_list ''='' ', raw_data_list)
        # returns a list of 5 rows
        return raw_data_list
    return _sample_reader_rows
