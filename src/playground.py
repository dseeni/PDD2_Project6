# from src.constants import *
# from src.push_pipeline import *
# from collections import namedtuple
# from contextlib import contextmanager
# import csv
# from itertools import islice
# from datetime import datetime
# import os
# from inspect import getgeneratorstate, getgeneratorlocals
# from collections import namedtuple
# # from src.constants import *
# from itertools import chain
# from src.constants import *
# from datetime import datetime
# # from src.push_pipeline import *
# from pytest import yield_fixture
# from pytest import fixture
from itertools import compress
from src.constants import fnames

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

# files and named tuples can be zipped
# predicates and output files can be zipped

# print(*input_package, sep='\n')
# print(list(output_package))
# print('230:', *output_package, sep='\n')

# with file_handler(fnames[0]) as f:
#     for row in f:
#         print(next(f))

# @coroutine
# def dummy_test():
#     def test_sink():
#         ml = []
#         while True:
#             # try:
#             row = yield
#             if row is not None:
#                 print('sink got data')
#                 print('row I recieved', row)
#                 if type(row) == list:
#                     for element in row:
#                         ml.append(element)
#                         print('sink yielding list')
#             ml = row
#             yield ml
#     return test_sink()
#
#

# @yield_fixture
# @coroutine
# def dummy_target():
#     def test_sink():
#         ml = []
#         while True:
#             # try:
#             row = yield
#             if row is not None:
#                 print('sink got data')
#                 print('row I recieved', row)
#                 if type(row) == list:
#                     for element in row:
#                         ml.append(element)
#                         print('sink yielding list')
#                 ml.append(row)
#             else:
#                 ml.append(row)
#             print('28:', 'ml ''='' ', ml)
#             yield ml
#     return test_sink()
# dummy = dummy_target()
#
# print(dummy.send('hello'))
# print(getgeneratorlocals(dummy))
#
#
# print(class_names[0])
# @fixture

# @coroutine
# def dummy_target():
#     # @coroutine
#     def test_sink():
#         ml = []
#         while True:
#             # try:
#             row = yield
#             if row is not None:
#                 print('sink got data')
#                 print('row I recieved', row)
#                 if type(row) == list:
#                     for element in row:
#                         ml.append(element)
#                         print('sink yielding list')
#                 ml.append(row)
#                 print('28:', 'ml ''='' ', ml)
#                 yield row
#     return test_sink()
# dummy = dummy_target()
# dummy.send('hello')
# print(getgeneratorlocals(dummy))

# you can use functions without instancing them
# @coroutine
# def mgen(target):
#     while True:
#         value = yield
#         target.send(value)
#
# @coroutine
# def agen():
#     while True:
#         value = yield
#         print(value)
#
# mgen(agen()).send('hi from agen')

# g = 'hi'
# d = 'bye'
#
# ml = [None, None, None, g]
# al = [d, d, d, d]
# gl = [i for i in range(len(al))]

# print(list(compress(al, ml)))

# first pass row to data parser, which returns dates as strings, then date
# parser will go over the row again, only evaluting entires that are
# strings.. thereby reducing the work that date_parser needs to do.
# because right now you're checking dates len(row) * 3 or more keys--> 24 calls
# you can automatically reduce the work if you just use date parser last,
# only on those entries that are of type(str)
# parse_guide = list(zip(ml, al, gl))
#
# for j, k, l in parse_guide:
#     print(j, k, l)
#
# ids = [i for i in range(len(parse_guide))]
# print(ids)

# f_idxs = [0, 2, 4]
# partial_files = list(fnames[i] for i in f_idxs)
# print(partial_files)
#
# row_parse_key = [1, 2, 3, 1, 'hi']
# # value = 1
# for value in row_parse_key:
#     if value == 1:
#         print('found 1')
#         row_parse_key[row_parse_key.index(value)] = 'hahah'
# print(row_parse_key)

# from src.constants import *
# from datetime import datetime
# date_keys_tuple = date_keys
# from copy import deepcopy
#
#
# key = [(str, '100-53-9824', 0), (str, '2017-10-07T00:14:42Z', 1),
#        (str, '2016-01-24T21:19:30; Z', 2)]
#
# key_copy = deepcopy(key)
#
# for data_type, item, idx in key:
#     # try to cast any str_key as potential date
#     if data_type == str:
#         for _ in range(len(date_keys_tuple)):
#             try:
#                 if datetime.strptime(item, date_keys_tuple[_]):
#                     date_func = (lambda v: datetime.strptime
#                     (v, date_keys_tuple[_]))
#                     key_copy[idx] = date_func
#                     continue
#             except ValueError:
#                 # _ += 1
#                 continue
#             except IndexError:
#                 # print('Unrecognizable Date Format: cast as str')
#                 # row_key_gen.send(None)
#                 break
# print(key_copy)

#
# for data_type, item, idx in key:
#        print(data_type, item, idx)
#
#
# date_func = (lambda v: datetime.strptime(v, date_keys_tuple[_]))


# from copy import deepcopy
# from src.push_pipeline import coroutine
# from inspect import getgeneratorlocals
#
#
# delimited_row3 = ['4006478550', 'VAD7274', 'VA', 'PAS', '10/5/2016', '5', '4D',
#                   'BMW', 'BUS LANE VIOLATION']
#
# @coroutine
# def row_key_gen(target):  # from coro to date parser:-->
#     while True:
#         data_row = yield  # from pipeline_coro
#         row_parse_key = deepcopy(data_row)
#         for value in row_parse_key:
#             if value is None:
#                 row_parse_key[row_parse_key.index(value)] = None
#             elif all(c.isdigit() for c in value):
#                 row_parse_key[row_parse_key.index(value)] = int
#             elif value.count('.') == 1:
#                 try:
#                     float(value)
#                     row_parse_key[row_parse_key.index(value)] = float
#                 except ValueError:
#                     row_parse_key[row_parse_key.index(value)] = str
#             else:
#                 row_parse_key[row_parse_key.index(value)] = str
#         target.send(row_parse_key)
#
#
#
# @coroutine
# def sink():
#     while True:
#         incoming = yield
#         # yield incoming
#
# s = sink()
# row_gen = row_key_gen(s)
# row_gen.send(delimited_row3)
# print(getgeneratorlocals(s))

# from contextlib import contextmanager, ExitStack
# import csv
#
# @contextmanager
# def file_handler(filenames):
#     # # send to: header_creator, type_generator
#     # # pass in the dictionary of file/filter/name
#     # # print('pwd', os.getcwd())
#     # # os.chdir('./input_data')
#     # # open the file, sniff, and send rows
#     # file_obj = open(file_names)
#     # try:
#     #     dialect = csv.Sniffer().sniff(file_obj.read(2000))
#     #     file_obj.seek(0)
#     #     reader = csv.reader(file_obj, dialect)
#     #     # both header extractor and type_generator need row
#     #     yield reader
#     # finally:
#     #     try:
#     #         next(file_obj)
#     #     except StopIteration:
#     #         pass
#     #     file_obj.close()
#     try:
#         with ExitStack() as stack:
#             files = [stack.enter_context(open(fname)) for fname in
#                      filenames]
#             for file in files:
#                 dialect = csv.Sniffer().sniff(file.read(2000))
#                 file.seek(0)
#                 readers = [csv.reader(file_obj, dialect)
#                            for file_obj in files]
#                 # both header extractor and type_generator need row
#             yield readers
#
#     finally:
#         return
#         # for file in files:
#         #     # file.close()
#         #     # print(file.closed)
#
#
# with file_handler(fnames) as fh:
#     readers = list(reader for reader in fh)
#     print(readers)
#     headers = (next(reader) for reader in fh)
#     for header in headers:
#         print(header)
#     print(list(next(reader) for reader in fh), sep='\n')
#
# # input_pack = {inputfile1: {filters:outputfile}
# #               inputfile2: {filters:outputfile}
# #               inputfile3: {filters:outputfile}
# #               inputfile4: {filters:outputfile}}
#
#
#
#
# # print(list(file.closed for file in files))
#
# # with ExitStack() as stack:
# #     files = [stack.enter_context(open(fname)) for fname in fnames]
# #     for file in files:
# #         print('495:', next(file), sep='\n')


from src.constants import *
# print('512:', *input_packages, sep='\n')
# print('513:', *output_packages, sep='\n')
# print('513:', data_package_dict, sep='\n')
# print(data_package_dict)

# print(len(list(input_packages)), len(list(output_packages)))
# for input_data, output_data in data_package:
#     print(input_data, '===========>', output_data, sep='\n')

for input_data, output_data in data_package:
    print(*input_data, sep='\n')
    print(*output_data, sep='\n')
    print('')


# for output_package in output_packages:
#     print('217:', *output_package, sep='\n')

# # filesnames and classnames
# input_dict = {k: v for k,v in input_packages, output_packages}
#
# print(*input_dict, sep='\n')
