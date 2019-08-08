from pytest import fixture
import os
from src.push_pipeline import *


@fixture('session', autouse=True)
def set_test_directory():
    os.chdir('src/')


@fixture('function', autouse=True)
@coroutine
def dummy_target():
    def myvar():
        container = []
        while True:
            var = yield
            container.append(var)
            return container
    return myvar


# @coroutine
# def myvar():
#     while True:
#         var = yield
#         print(var)

