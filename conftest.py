from pytest import fixture
import os
from src.constants import *


@fixture('session', autouse=True)
def set_test_directory():
    os.chdir('src/')

