from src.constants import *
from src.push_pipeline import *
from collections import namedtuple
from contextlib import contextmanager
import csv
from itertools import islice

cars = data_reader(fcars, cars_parser, cars_class_name)

with cars as c:
    # for row in c:
    #     print(row)
    print('20:', *list(islice(c, 20)), sep='\n')

# @contextmanager
# def gen_file_context_manager(file_name, single_parser, single_class_name):
#     file_obj = open(file_name)
#     try:
#         dialect = csv.Sniffer().sniff(file_obj.read(2000))
#         file_obj.seek(0)
#         reader = csv.reader(file_obj, dialect)
#         headers = map(lambda l: l.lower(), next(reader))
#         DataTuple = namedtuple(single_class_name, headers)
#         yield (DataTuple(*(fn(value) for value, fn
#                            in zip(row, single_parser))) for row in reader)
#
#     finally:
#         try:
#             next(file_obj)
#         except StopIteration:
#             pass
#         # print('closing file')
#         file_obj.close()
