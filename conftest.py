from pytest import fixture
import os
from src.push_pipeline import *


@fixture('session', autouse=True)
def set_test_directory():
    os.chdir('src/')


@fixture('function')
def dummy_target():
    @coroutine
    def test_sink():
        ml = []
        while True:
            row = yield ml
            print('sink got data')
            for element in row:
                ml.append(element)
            print('sink yielding list')
            # row = yield ml
    return test_sink


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

