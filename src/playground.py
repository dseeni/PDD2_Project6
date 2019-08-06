from src.constants import *
from src.push_pipeline import *
from collections import namedtuple
from contextlib import contextmanager
import csv
from itertools import islice

# cars_header = header_extract(fcars)
# cars = data_reader(fcars, cars_parser, cars_header, cars_class_name)
#
# with cars as c:
#     # for row in c:
#     #     print(row)
#     print('20:', *list(islice(c, 100)), sep='\n')


# for row in cars:
#     print('17:', *list(row), sep='\n')


# file open context manager, coroutine context manager, test within it thats
# the first thing that you need to right

# extract headers as variable
# determine filter names that correspond to these:
#  Car Cheverlot
#  MPG
#  Cylinders
#  Displacement
#  Horsepower
#  Weight
#  Acceleration
#  Model Monte Carlo Landau
#  Origin

# Constants:
# Headers --> NamedTuple
# idx_*headers --> Headers.horsepower etc?
# Input_file --> cars.csv
# Types str in etc
# Output files based on filter names {}


# def infer_data_type(self):
#     for value in self.data_key:
#         if value is None:
#             self.data_key[self.data_key.index(value)] = None
#         elif all(c.isdigit() for c in value):
#             self.data_key[self.data_key.index(value)] = int(value)
#
#         elif value.count('.') == 1:
#             try:
#                 self.data_key[self.data_key.index(value)] = float(value)
#             except ValueError:
#                 self.data_key[self.data_key.index(value)] = str(value)
#
#         else:
#             self.data_key[self.data_key.index(value)] = str(value)
with pipeline() as pipe:
        data = data_parser()
        for row in data:
                pipe.send(row)
