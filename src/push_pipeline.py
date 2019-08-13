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
    # print('pwd', os.getcwd())
    # os.chdir('./input_data')
    # open the file, sniff, and send rows
    file_obj = open(file_name)
    try:
        dialect = csv.Sniffer().sniff(file_obj.read(2000))
        file_obj.seek(0)
        reader = csv.reader(file_obj, dialect)
        # both header extractor and type_generator need row
        yield reader
    finally:
        try:
            next(file_obj)
        except StopIteration:
            pass
        file_obj.close()


# this coroutine decorator will prime your sub generators
def coroutine(fn):
    def inner(*args, **kwargs):
        g = fn(*args, **kwargs)
        next(g)
        return g

    return inner


@coroutine
def pipeline_coro():
    for file_name, class_name in input_package:
        with file_handler(file_name) as f:
            # DECLARE --> From the bottom up stack
            broadcaster = broadcast(filter_names)
            row_parser = data_parser(broadcaster)
            date_key = date_key_gen(row_parser)
            row_key = row_key_gen(date_key_gen)
            field_name_gen = gen_field_names(data_parser)  # send class_names
            header_row = header_extract(field_name_gen)

            # pipeline sends gen_date_key the date_keys_tuple
            # SEND DATA
            # once for parse_key generation and once for processing
            # send the first data row twice
            # read header and send to data_fields gen
            # right away to field_name_generator

            # send class_names and header_row
            field_name_gen.send(class_name)
            header_row.send(f)  # --> send to gen_field_names
            # sample row for row_key:
            first_raw_data_row = next(f)
            row_key.send(first_raw_data_row)
            date_key.send(date_keys)
            date_key.send(first_raw_data_row)

            # TODO: working on date_parser as a sub-pipe off shoot from
            #   gen_row_parse_key, it sends to it and it sends back

            # send next row to gen_row_parse_key

            # named_tuple_gen sends to data_caster for parsing
            # parser needs named tupel and data type key

            # send the first row of the file to the header function
            # send first row to gen_field_names

            # header function sends to gen_field_name

            # gen_parse key sends key to caster
            sample_row = row_key_gen(data_parser)
            # header_extract.send(next(f))  # --> send row for header extract

            # row_parse_key_gen.send(next(f))

            # date_parse gen

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
    # filter_pink_cars = filter_data(lambda d: d[idx_color].lower() == 'pink',
    #                                out_pink_cars)

    # predicates can be defined as filters..
    # def pred_ford_green(data_row):
    #     return (data_row[idx_make].lower() == 'ford'
    #             and data_row[idx_color].lower() == 'green')

    # filter_ford_green = filter_data(pred_ford_green, out_ford_green)
    # filter_older = filter_data(lambda d: d[idx_year] <= 2010, out_older)

    # filters = (filter_pink_cars, filter_ford_green, filter_older)

    # your brodcaster must send data from row
    # broadcaster = broadcast(filters)

    # while True:
    #     data_row = yield
    #     broadcaster.send(data_row)


# input_data parser needs headers and data_key sent to it
@coroutine
def header_extract(target):  # --> send to gen_field_names
    while True:
        reader = yield
        headers = tuple(map(lambda l: l.lower(), next(reader)))
        target.send(headers)


# TODO: Refactor to output a data_type-key
@coroutine
def row_key_gen(target):  # from coro to date parser:-->
    while True:
        data_row = yield  # from pipeline_coro
        row_parse_key = deepcopy(data_row)
        for value in row_parse_key:
            if value is None:
                row_parse_key[row_parse_key.index(value)] = None
            elif all(c.isdigit() for c in value):
                row_parse_key[row_parse_key.index(value)] = int
            elif value.count('.') == 1:
                try:
                    float(value)
                    row_parse_key[row_parse_key.index(value)] = float
                except ValueError:
                    row_parse_key[row_parse_key.index(value)] = str
            else:
                row_parse_key[row_parse_key.index(value)] = str
        target.send(row_parse_key)


@coroutine
def date_key_gen(target):
    date_keys_tuple = yield  # <-sent by pipeline_coro ONCE per file run
    delimited_row = yield  # <-sent by pipeline coro ONCE per file run
    while True:
        partial_key = yield  # <-sent by gen_row_parse_key
        key_copy = deepcopy(partial_key)
        key_idx = [i for i in range(len(key_copy))]
        parse_guide = list(zip(key_copy, delimited_row, key_idx))
        date_func = None
        for data_type, item, idx in parse_guide:
            if data_type == str:
                for _ in range(len(date_keys_tuple)):
                    try:
                        if datetime.strptime(item, date_keys_tuple[_]):
                            date_func = (lambda v: datetime.strptime
                            (v, date_keys_tuple[_]))
                            key_copy[idx] = date_func
                            continue
                    except ValueError:
                        # _ += 1
                        continue
                    except IndexError:
                        break
        target.send(key_copy)


@coroutine
def gen_field_names(target):  # sends to data_caster
    while True:
        class_name = yield  # from pipeline_coro
        header_row = yield  # from header_extract
        data_field = namedtuple(class_name, header_row)
        target.send(data_field)


# TODO: Refactor out Data_Tuple, let header_extract take care of is
# TODO: make sure data_caster can handle None values
@coroutine
def data_parser(file_name, parse_key, headers, single_class_name):
    # handled by gen_field_names # file_name = yield  # <-- from pipeline_coro
    # single_class_name = yield  # <-- from pipe_line_coro
    field_names_tuple = yield  # <-- from gen_field_names ONCE per file fun
    parse_key = yield  # <-- from gen_row_parse_key
    while True:
        raw_data_row = yield
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
                               in zip(row, parse_key))) for row in reader)
        finally:
            try:
                next(file_obj)
            except StopIteration:
                pass
            print('closing file')
            file_obj.close()


# # data reader --> sends out header, and sends out sample data row
# @coroutine
# def data_reader(file_name, header_targert, row_sample_target):
#     while True:
#         file_obj = open(file_name)
#         # TODO: just yield to header
#         #   yield to sample data
#         #   then keep yielding removing the converter code below
#         data = data_caster(file_name)
#         next(data)  # skip header row
#         # for row in data:
#         #     parsed_row = [converter(item)
#         #                   for converter, item in zip(converters, row)]
#         #     yield parsed_row
#

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
    str = 'ff_name, headers, dir_name'
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
