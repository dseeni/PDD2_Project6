# from src.constants import *
# from src.push_pipeline import *
# from collections import namedtuple
# from contextlib import contextmanager
# import csv
# from itertools import islice
# from datetime import datetime
import os
from inspect import getgeneratorstate, getgeneratorlocals
from collections import namedtuple
from src.constants import *
from itertools import zip_longest


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
# with pipeline() as pipe:
#         data = data_parser()
#         for row in data:
#                 pipe.send(row)


# def infer_data_type():
#     data_key = yield
#     for value in data_key:
#         if value is None:
#             data_key[data_key.index(value)] = None
#         elif all(c.isdigit() for c in value):
#             data_key[data_key.index(value)] = int(value)
#
#         elif value.count('.') == 1:
#             try:
#                 data_key[data_key.index(value)] = float(value)
#             except ValueError:
#                 data_key[data_key.index(value)] = str(value)
#
#         else:
#             data_key[data_key.index(value)] = str(value)

# try date first, then literal, then str


# print(type(ast.literal_eval('6.2')))
#
# print(type(ast.literal_eval('6.')))
# print(type(ast.literal_eval('6')))
# try:
#     print(type(ast.literal_eval('6-2')))
# except ValueError:
#     print(str)
# try:
#     print(type(ast.literal_eval('6A2')))
# except Exception:
#     print(str)


# def parse_date(value, *, fmt='%Y-%m-%dT%H:%M:%SZ'):
#     return datetime.strptime(value, fmt)


# def date_getter():
#     # for datekey in date_keys:
#     #     try:
#     #         parse_date('12/12/12', fmt=datekey)
#     #     except Exception:
#     #         return str('none')
#     for datekey in date_keys:
#         try:
#             print(datetime.stiptime('12/12/12', datekey))
#         except Exception:
#             print('cant do it')


# test_date = next(parse_date('2017-10-07T00:14:42Z', date_keys))
# print(test_date.year)
# print(test_date.month)
# print(test_date.day)
# print(test_date)
# print(type(test_date))
# print(type(next(parse_date('12/x2/2012', date_keys))) == str)
# print(type(next(parse_date('2017-10-07T00:14:42Z', date_keys))))
# print(type(next(parse_date('12/12/2012', date_keys))) == datetime)
# print(type(next(parse_date('2017-10-07T00:14:42Z', date_keys))) == datetime)
# print(type(next(parse_date('12/x2/2012', date_keys))) == str)


# @coroutine
# def myvar():
#     container = []
#     while True:
#         var = yield
#         container.append(var)
#         yield container
#
#
# cvars = myvar()
#
# for i in range(1000):
#     print(cvars.send('hi'))

# h = header_extract(cvars)
# file_obj = open(fnames[0])
# h.send(file_obj)

# sample_row = "Chevrolet Chevelle Malibu;18.0;8;307.0;130.0;3504.;12.0;70;US"
# ml = sample_row.split(';')
# print(ml)
# print(type(ml))


# @coroutine
# def test_sink():
#     ml = []
#     while True:
#         # try:
#         row = yield
#         if row is not None:
#             print('sink got data')
#             for element in row:
#                 ml.append(element)
#             print('sink yielding list')
#         yield ml
#
# sink = test_sink()
# print(sink.send('hi sink'))

# working on constants.. shape them into UserDict

# a dictionary of filters:
# {tuple(file_name_1, data row name): 'function_name': lambda_func
# tuple(file_name_2, data row name): 'function_name': lambda_func
# tuple(file_name_3, data row name): 'function_name': lambda_func}

# file_and_row_tup = tuple(zip(fnames, class_names))
# print(*file_and_row_tup, sep='\n')


# example:...                               |output_file
# where in d is data_row:
# lambda d: d[idx_color].lower() == 'pink', out_pink_cars


# example:...                               |output_file
# def pred_ford_green(data_row):
#     return (data_row[idx_make].lower() == 'ford'
#             and data_row[idx_color].lower() == 'green')

# lambda d where d is data_row...
# example predicate: 'lambda d: d[idx_color].lower()'

# filter_ford_green = filter_data(pred_ford_green, out_ford_green)
# filter_older = filter_data(lambda d: d[idx_year] <= 2010, out_older)

# filters = (filter_pink_cars, filter_ford_green, filter_older)

# cars:
# 'Car;MPG;Cylinders;Displacement;Horsepower;Weight;Acceleration;Model;Origin'
# 'Chevrolet Chevelle Malibu;18.0;8;307.0;130.0;3504.;12.0;70;US'

# employment.csv
# 'employer,department,employee_id,ssn'
# 'Stiedemann-Bailey,Research and Development,29-0890771,100-53-9824'

# nyc_parking_tickets_extract.csv
# 'employer,department,employee_id,ssn'
# '4006478550,VAD7274,VA,PAS,10/5/2016,5,4D,BMW,BUS LANE VIOLATION'

# personal_info.csv
# 'ssn,first_name,last_name,gender,language'
# '100-53-9824,Sebastiano,Tester,Male,Icelandic'

# update_status.csv
# 'ssn,last_updated,created'
# '100-53-9824,2017-10-07T00:14:42Z,2016-01-24T21:19:30Z'

ml1 = [i for i in range(10)]

ml2 = [i for i in range(15)]

print(list(zip_longest(ml1, ml2, fillvalue=None)))

print(list(zip_longest(fnames, class_names, vehicle_output, emp_output,
                       fillvalue=None)))

# files and named tuples can be zipped
# predicates and output files can be zipped
