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
            if type(row) == list:
                for i in row:
                    ml.append(i)
            else:
                ml.append(row)
    sink = _test_sink()
    return sink


@fixture('function')
def test_date():
    def _test_date(gen_name, list_name, *args):
        nested_list = getgeneratorlocals(gen_name)[list_name]
        print('35:', 'nested_list ''='' ', nested_list)
        idx = [arg for arg in args]
        current = list(nested_list)
        print('current',  current)
        for i in range(len(idx)):
            try:
                if iter(current[idx[i]]):
                    current = current[idx[i]]
            except TypeError:
                continue
            finally:
                print('current =', current)
                print('idx -1', idx[-1])
                current = current[idx[-1]]
                return current
    return _test_date


@fixture('function')
def test_file_reader():
    raw_data_list = []
    f_idxs = [0, 2, 4]
    # f_idxs = [2]
    partial_files = tuple(fnames[i] for i in f_idxs)
    fnames_only_package = tuple(((i, None), None) for i in partial_files)

    with file_readers(fnames_only_package) as readers:
        # print('38:', 'readers ''='' ', readers)
        for reader in readers:
            next(reader)
            raw_data_list.append(next(reader))
        # print('40:', 'raw_data_list ''='' ', raw_data_list)
    # returns a list of 5 rows
    return raw_data_list
