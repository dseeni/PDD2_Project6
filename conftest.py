from pytest import fixture
from src.push_pipeline import *
# from inspect import getgeneratorstate, getgeneratorlocals
from contextlib import ExitStack


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
def test_data_rows():
    raw_data_list = []
    f_idxs = [0, 2, 4]
    partial_files = tuple(fnames[i] for i in f_idxs)
    # _ = []
    # _.append((tuple((file, None), None)) for file in partial_files)
    # partial_package = tuple(i for i in _)
    # print(_)
    # print(partial_package)
    # partial_package = (('file', None), None),
    # (('file', None), None),
    # (('file', None), None),
    partial_package = (((fnames[0], None), None), ((fnames[0], None), None),
                       ((fnames[0], None), None))
    print(partial_package)

    with file_readers(partial_package) as readers:
        print('38:', 'readers ''='' ', readers)
        for reader in readers:
            next(reader)
            raw_data_list.append(next(reader))
        print('40:', 'raw_data_list ''='' ', raw_data_list)
    return raw_data_list
    # with ExitStack() as stack:
    #     readers = [stack.enter_context(open(file)) for file in partial_files]
    #     for reader in readers:
    #         next(reader)  # skip the header row
    #         row_line = next(reader).strip('\n')
    #         split_row = row_line.split(';')
    #         raw_data_list.append(row_line)
    # print('41:', *raw_data_list, sep='\n')
    # return raw_data_list

# @contextmanager
# def file_handler(file_name):
#     # send to: header_creator, type_generator
#     # pass in the dictionary of file/filter/name
#     print('pwd', os.getcwd())
#     # os.chdir('./input_data')
#     # open the file, sniff, and send rows
#     file_obj = open(file_name)
#     try:
#         dialect = csv.Sniffer().sniff(file_obj.read(2000))
#         file_obj.seek(0)
#         reader = csv.reader(file_obj, dialect)
#         # both header extractor and type_generator need row
#         yield reader
#     finally:
#         try:
#             next(file_obj)
#         except StopIteration:
#             pass
#         file_obj.close()


# @fixture('function', autouse=True)
# def dummy_sender():
#     @coroutine
#     def myvar():
#         while True:
#             var = yield
#             container.append(var)
#             return container
#     return myvar

# @coroutine
# def myvar():
#     while True:
#         var = yield
#         print(var)
