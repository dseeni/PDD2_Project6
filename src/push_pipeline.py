from src.constants import *
from collections import namedtuple
from contextlib import contextmanager
from datetime import datetime
import csv
import os
from copy import deepcopy


# TODO: Look at the pulling example and rewrite it all as a push pipeline
# We can do this be making the reader yield and row and send it to parser that
# sends it to broadcaster

# TODO: Parse the file with correct types, via type sniffing function
# TODO: Make it as generic as possible to handle N filters and N files and N
# destination files
# And N source files!!!

# TODO: Solution must take in arbitrary amount of filters
# Any 3 filters will result with 2 records on the screen only
# if you did it all right

# TODO: Implement function to average length of lines in file
# # TODO: Lookup the print_file_row() function in Fred's notes on GitHub.

# headers = ('make', 'model', 'year', 'vin', 'color')

# TODO: Make to lower case headers as well as replace whitespace with _
# TODO: Output File name = File Name + Filter Name?

@contextmanager
def file_handler(file_name):
    # send to: header_creator, type_generator
    # pass in the dictionary of file/filter/name
    print('pwd', os.getcwd())
    # os.chdir('./input_data')
    # open the file, sniff, and send rows
    file_obj = open(file_name)
    try:
        dialect = csv.Sniffer().sniff(file_obj.read(2000))
        file_obj.seek(0)
        reader = csv.reader(file_obj, dialect)
        # both header extractor and type_genertor need row

        yield reader
    finally:
        try:
            next(file_obj)
        except StopIteration:
            pass
        file_obj.close()
#    def get_dialect(file_obj):
#    sample = file_obj.read(2000)
#    dialect = csv.Sniffer().sniff(sample)
#    file_obj.seek(0)
#    return dialect


# this coroutine decorator will prime your sub generators
def coroutine(fn):
    def inner(*args, **kwargs):
        g = fn(*args, **kwargs)
        next(g)
        return g
    return inner
# input_data parser needs headers and data_key sent to it
@coroutine
def header_extract(target):  # --> send to row_parse_key_gen
    while True:
        reader = yield
        class_name = yield
        # file_obj = open(file_name)
        headers = tuple(map(lambda l: l.lower(), next(reader)))
        target.send(headers)


@coroutine
def pipeline_coro(**data_packages):
    # for data_package in data_packages:
    #    for inputfile, classname, outputfile, predicate in datapackage:
    #    do stuff:

    # instantiate functions parameters..
    # can you instantiate without symbol binding?

    # instance save data writers:
    out_pink_cars = save_data('pink_cars.csv', header_extract(fcars))
    out_ford_green = save_data('ford_green.csv', header_extract(fcars))
    out_older = save_data('older.csv', header_extract(fcars))

    # filter instances with predicates
    filter_pink_cars = filter_data(lambda d: d[idx_color].lower() == 'pink',
                                   out_pink_cars)
    # predicates can be defined as filters..
    def pred_ford_green(data_row):
        return (data_row[idx_make].lower() == 'ford'
                and data_row[idx_color].lower() == 'green')

    filter_ford_green = filter_data(pred_ford_green, out_ford_green)
    filter_older = filter_data(lambda d: d[idx_year] <= 2010, out_older)

    filters = (filter_pink_cars, filter_ford_green, filter_older)

    # your brodcaster must send data from row
    broadcaster = broadcast(filters)

    while True:
        data_row = yield
        broadcaster.send(data_row)


@coroutine
def row_parse_key_gen(target):  # from --> sample_data to:--> parse_data
    row_copy = None
    while True:
        data_row = yield row_copy
        row_copy = deepcopy(data_row)
        for value in row_copy:
            # try:
            if parse_date(value, date_keys) is None:

                if value is None:
                    row_copy[row_copy.index(value)] = None
                elif all(c.isdigit() for c in value):
                    row_copy[row_copy.index(value)] = int(value)
                elif value.count('.') == 1:
                    try:
                        row_copy[row_copy.index(value)] = float(value)
                    except ValueError:
                        row_copy[row_copy.index(value)] = str(value)
                else:
                    row_copy[row_copy.index(value)] = str(value)

            else:
                row_copy[row_copy.index(value)] = parse_date(value, date_keys)
            # finally:
        # else:


# TODO: Refactor out Data_Tuple, let header_extract take care of is
@coroutine
def data_caster(file_name, single_parser, headers, single_class_name):
    file_obj = open(file_name)
    try:
        dialect = csv.Sniffer().sniff(file_obj.read(2000))
        file_obj.seek(0)
        reader = csv.reader(file_obj, dialect)
        # skip the header row
        next(reader)
        # headers = header_extract(file_name, file_obj)
        print(headers)
        DataTuple = namedtuple(single_class_name, headers)
        yield (DataTuple(*(fn(value) for value, fn
                           in zip(row, single_parser))) for row in reader)
    finally:
        try:
            next(file_obj)
        except StopIteration:
            pass
        print('closing file')
        file_obj.close()


# data reader --> sends out header, and sends out sample data row
@coroutine
def data_reader(file_name, header_targert, row_sample_target):
    while True:
        file_obj = open(file_name)
        # TODO: just yield to header
        #   yield to sample data
        #   then keep yielding removing the converter code below
        data = data_caster(file_name)
        next(data)  # skip header row
        for row in data:
            parsed_row = [converter(item)
                          for converter, item in zip(converters, row)]
            yield parsed_row


# @coroutine
def parse_date(value, date_keys_tuple):
    valid_date = None
    while True:
        for _ in range(len(date_keys_tuple)):
            # while valid_date is None:
            # while True:
                try:
                    # print('try:', _)
                    valid_date = datetime.strptime(value, date_keys_tuple[_])
                except ValueError:
                    _ += 1
                    continue
                except IndexError:
                    print('Unrecognizable Date Format: cast as str')
                    valid_date = None
                    break
        return valid_date


@coroutine
def broadcast(targets):
    while True:
        data_row = yield
        for target in targets:
            target.send(data_row)


@coroutine
def filter_data(filter_predicate, target):
    # sent the input_data tuple from reader
    while True:
        data_tuple = yield
        if filter_predicate(data_tuple):
            target.send(target)


@coroutine
def save_data(f_name, headers, dir_name):
    try:
        # Create target Directory
        os.mkdir(dir_name)
        print("Directory ", dir_name, " Created ")
    except FileExistsError:
        print("Directory ", dir_name, " already exists")
    finally:
        os.chdir(dir_name)
        try:
            with open(f_name, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                while True:
                    data_row = yield
                    writer.writerow(data_row)
        finally:
            os.chdir('..')
