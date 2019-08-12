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
            row = yield
            if row is not None:
                print('sink got data')
                print('row I recieved', row)
                if type(row) == list:
                    for element in row:
                        ml.append(element)
                        print('sink yielding list')
                        ml.append(row)
                        print('28:', 'ml ''='' ', ml)
                        yield ml
                ml = row
                yield ml
    sink = test_sink()
    return sink
# sink = test_sink()

# return sink


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
