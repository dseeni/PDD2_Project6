from pytest import fixture
from src.push_pipeline import *
# from inspect import getgeneratorstate, getgeneratorlocals


@fixture('session', autouse=True)
def set_test_directory():
    os.chdir('src/')


# we send to dummy via the coroutines we're testing
# so we can't view a return value
@fixture('function')
def dummy_target():
    @coroutine
    def test_sink():
        ml = []
        while True:
            row = yield ml
            if type(row) == list:
                ml = [i for i in row]
            else:
                ml = row
    sink = test_sink()
    return sink


@fixture('function')
def dummy_reader():
    raw_data_list = []
    f_idxs = [0, 2, 4]
    partial_files = list(fnames[i] for i in f_idxs)
    for file in partial_files:
        with file_reader(file) as f:
            next(f)
            raw_data_list.append(next(f))
    return raw_data_list

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
