from src.constants import *
from src.push_pipeline import *
from collections import namedtuple
from contextlib import contextmanager
import csv
from itertools import islice

cars_header = header_extract(fcars)
cars = data_reader(fcars, cars_parser, cars_header, cars_class_name)

with cars as c:
    # for row in c:
    #     print(row)
    print('20:', *list(islice(c, 100)), sep='\n')


# for row in cars:
#     print('17:', *list(row), sep='\n')


