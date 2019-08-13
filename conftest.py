from pytest import fixture
from src.push_pipeline import *
# from inspect import getgeneratorstate, getgeneratorlocals


@fixture('session', autouse=True)
def set_test_directory():
    os.chdir('src/')


# remember that yield is trigger immediately upon sending!!!!!!
@fixture('function')
def dummy_target():
    @coroutine
    def test_sink():
        ml = []
        while True:
            # try:
            ml = yield ml
            if ml is not None:
                # print('sink got data')
                # print('ml I recieved', ml)
                if type(ml) == list:
                    ml = [i for i in ml]
                    # for element in row:
                    #     ml.append(element)
                    #     # print('sink yielding list')
                    #     # ml.append(row)
                    #     # print('28:', 'ml ''='' ', ml)
    sink = test_sink()
    return sink


@fixture('function')
def dummy_reader():
    raw_data_list = []
    f_idxs = [0, 2, 4]
    partial_files = list(fnames[i] for i in f_idxs)
    for file in partial_files:
        with file_handler(file) as f:
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
