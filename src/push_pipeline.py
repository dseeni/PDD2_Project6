from src.constants import *
from collections import namedtuple
from contextlib import contextmanager, ExitStack
from datetime import datetime
import csv
import os
from copy import deepcopy
from itertools import islice, cycle, count
from inspect import getgeneratorlocals, getgeneratorstate


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
# TODO: Lookup the print_file_row() function in Freds notes on GitHub.
# TODO: Output File name = File Name + Filter Name?

@contextmanager
def file_readers(packaged_data):
    readers = []
    input_file_objs = []
    try:
        for data_in, data_out in packaged_data:
            input_files = [data_in[0]]
            input_file_objs = [open(input_file) for input_file in input_files]
            # print(input_file_objs)
            for file_obj in input_file_objs:
                dialect = csv.Sniffer().sniff(file_obj.read(2000))
                file_obj.seek(0)
                readers.append(csv.reader(file_obj, dialect))
        yield readers
    finally:
        for reader in readers:
            try:
                next(reader)
            except StopIteration:
                pass
        for file in input_file_objs:
            file.close()


def coroutine(fn):
    def inner(*args, **kwargs):
        g = fn(*args, **kwargs)
        next(g)
        return g
    return inner


@coroutine
def pipeline_coro():
    with file_readers(data_package) as readers:
        # for input_data, output_data in data_package:

        # CONSTANTS:
        nt_class_names = [data[0][1] for data in data_package]
        filters = [data[1][1][1] for data in data_package]
        output_files = [data[1][1][0] for data in data_package]

        # DECLARE --> From the bottom up stack
        broadcaster = broadcast(filters)
        parse_data = data_parser(broadcaster)
        date_key = date_key_gen(parse_data)
        row_key = row_key_gen(parse_data, date_key)
        field_name_gen = gen_field_names(parse_data)
        headers = header_extract(field_name_gen)
        row_cycler = cycle_rows(headers)

        # pipeline sends gen_date_key the date_keys_tuple
        # once for parse_key generation and once for processing
        # send the first data row twice
        # read header and send to data_fields gen
        # right away to field_name_generator

        # SEND PREREQUISITES FIRST
        field_name_gen.send(nt_class_names)
        date_key.send(date_keys)
        row_cycler.send(readers)

        # SEND DATA:
        # this will engage row cyclers while loop
        row_cycler.send((row_key, parse_data))

        # send next row to gen_row_parse_key
        # named_tuple_gen sends to data_caster for parsing
        # parser needs named tupel and data type key
        # send the first row of the file to the header function
        # send first row to gen_field_names
        # header function sends to gen_field_name
        # gen_parse key sends key to caster
        # header_extract.send(next(f))  # --> send row for header extract
        # row_parse_key_gen.send(next(f))
        # date_parse gen

    # for data_package in data_packages:
    #    for inputfile, classname, outputfile, predicate in datapackage:
    #    do stuff:
    # instantiate functions parameters..
    # can you instantiate without symbol binding?
    # # instance save data writers:
    # out_pink_cars = save_data('pink_cars.csv', header_extract(fcars))
    # out_ford_green = save_data('ford_green.csv', header_extract(fcars))
    # out_older = save_data('older.csv', header_extract(fcars))

    # filter instances with predicates
    # filter_pink_cars = filter_data(lambda d: d[idx_color].lower() ==
    # 'pink',
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


@coroutine
def cycle_rows(target):
    readers = yield
    reader_idx_tracker = list(range(len(readers)))
    cycler = cycle(reader_idx_tracker)
    headers = [next(reader) for reader in readers]
    target.send(headers)
    # after headers are sent once, new targets (date_gen, row_gen, parser)
    targets = yield
    row_package = []
    while True:
        # sent out 5 lines at a time
        if len(row_package) == len(readers):
            # break out of function if all files are exhausted
            if all(row == [None] for row in row_package):
                break
            # check if single or multiple targets and send every 5 rows
            if isinstance(targets, tuple) and len(targets) > 1:
                for target in targets:
                    target.send(row_package)
                row_package.clear()
            else:
                targets.send(row_package)
                row_package.clear()
        # keep track of what file is being read with reader_idx
        reader_idx = next(cycler)
        # mark exhausted file readers as [None]
        if reader_idx_tracker[reader_idx] is None:
            row_package.append([None])
            continue
        try:
            # print('168:', 'row ''='' ', row)
            row_package.append(next(readers[reader_idx]))
        except StopIteration:  # skip over exhausted readers
            # print('finished', idx_tracker[reader_idx])
            reader_idx_tracker[reader_idx] = None
            row_package.append([None])
            continue


@coroutine
def header_extract(target):  # --> send to gen_field_names
    while True:
        row_package = yield  # --> from row_cycle
        headers = []
        for row in row_package:
            headers.append(tuple(map(lambda l: l.replace(" ", "_"),
                                 tuple(map(lambda l: l.lower(),
                                           (item for item in row))))))
        target.send(headers)  # --> sending [list of tuples of headers]


def gen_sub_key_ranges(package):
    sub_key_lens = [0, *[len(sub_key) for sub_key in package]]
    range_start = 0
    sub_key_ranges = []
    for i in range(len(sub_key_lens)):
        sub_key_ranges.append(sub_key_lens[i] + range_start)
        range_start += sub_key_lens[i]
    return sub_key_ranges


def pack(unpacked, sub_key_ranges):
    packed_package = [unpacked[sub_key_ranges[i]: sub_key_ranges[i + 1]]
                      for i in range(len(sub_key_ranges) - 1)]
    return packed_package


@coroutine
def row_key_gen(targets):
    while True:
        data_rows = yield
        row_parse_keys = deepcopy(data_rows)
        sub_key_ranges = gen_sub_key_ranges(row_parse_keys)
        # target0, 1 --> send to parser sub_key_ranges, send to date_key_gen
        target0, target1 = targets
        target0.send(sub_key_ranges)
        parse_keys = list(chain.from_iterable((value for value in parse_keys)
                                              for parse_keys in row_parse_keys))
        flat_raw_data = deepcopy(parse_keys)
        target0.send(flat_raw_data)
        for value in parse_keys:
            if value is None:
                parse_keys[parse_keys.index(value)] = None
            elif all(c.isdigit() for c in value):
                parse_keys[parse_keys.index(value)] = int
            elif value.count('.') == 1:
                try:
                    float(value)
                    parse_keys[parse_keys.index(value)] = float
                except ValueError:
                    parse_keys[parse_keys.index(value)] = str
            else:
                parse_keys[parse_keys.index(value)] = str
        # send to date_key_gen:
        target1.send(parse_keys)


@coroutine
def date_key_gen(target):
    date_keys_tuple = yield  # <-sent by pipeline_coro ONCE per file run
    while True:
        delimited_rows = yield  # <-sent by row_cycler
        partial_keys = yield  # <-sent by row_key_gen flattened
        # print('235:', 'partial_keys ''='' ', partial_keys)
        flat_keys = [*chain(deepcopy(partial_keys))]
        flat_rows = list(chain.from_iterable(deepcopy(delimited_rows)))
        keys_idxs = [i for i in range(len(flat_keys))]
        parse_guide = [*zip(flat_keys, flat_rows, keys_idxs)]
        for data_type, item, idx in parse_guide:
            if data_type == str:
                for _ in range(len(date_keys_tuple)):
                    try:
                        datetime.strptime(item, date_keys_tuple[_])
                        key = date_keys_tuple[_]
                        flat_keys[idx] = (lambda v: datetime.strptime(v, key))
                    except ValueError:
                        continue
                    except IndexError:
                        break
        target.send(flat_keys)


@coroutine
def gen_field_names(target):  # sends to data_caster
    while True:
        nt_class_names = yield  # from pipeline_coro a list of lists
        raw_header_rows = yield  # from header_extract a list of lists
        # print('headers', header_row_package)
        data_fields = [namedtuple(nt_class_names[i], raw_header_rows[i])
                       for i in range(len(nt_class_names))]
        target.send(data_fields)


# TODO: Refactor out Data_Tuple, let header_extract take care of is
# TODO: make sure data_caster can handle None values
@coroutine
def data_parser(target):
    # needs file_name, parse_keys, headers, single_class_name:
    nt_classes = yield  # <-- from gen_field_names list of field names

    # parse then pack data:
    while True:
        sub_key_ranges = yield  # <-- from row_key_gen
        flat_raw_data = yield
        parse_keys = yield  # <-- from gen_date_parse_key list of lists
        packed = pack(parse_keys, sub_key_ranges)
        target.send(packed)
        # use pack() here to pack unpacked data into named_tuples based on

        # parsers = [tuple(zip(parse_keys[i], raw_data_rows[i]))
        #            for i in range(len(parse_keys))]
        # parsed = [tuple(fn(item) for fn, item in parsers[i])
        #           for i in range(len(parsers))]
        # named_tuple_row = [(data_row_tuples[i](*parsed[i]))
        #                    for i in range(len(data_row_tuples))]
        # # print('297:', *named_tuple_row, sep='\n')
        # target.send(named_tuple_row)
        #

        # print(*parsers, sep='\n')
        # print('300:', 'len(parsers) ''='' ', len(parsers))

        # parsed = []
        # for parser_key in parsers:
        #     print('301:', 'parser_key ''='' ', parser_key)
        #     for parser in parser_key:
        #         print('parser', parser)
        #         # parsed = [fn(item) for fn, item in parser]

        # parsed = tuple(fn(item) for zip_row in zipped for fn, item in zip_row)
        # print('300:', 'parsed ''='' ', parsed)
        # print('300:', *zipped, sep='\n')
        # print('297:', 'len(zipped) ''='' ', len(zipped))
        # result.append([fn(item) for key in zipped for fn, item in key])
        # print('**********************')
        # print('295:', 'result ''='' ', result)
        # print('**********************')
        # target.send(zipped)
        # try:
        #     DataTuple = namedtuple(single_class_name, headers)
        #     yield (DataTuple(*(fn(value) for value, fn
        #                        in zip(row, parse_keys))) for row in reader)
        # finally:
        #     try:
        #         next(file_obj)
        #     except StopIteration:
        #         pass
        #     print('closing file')
        #     file_obj.close()


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
def save_data(targets):
    # 'ff_name, headers, dir_name'
    dir_name = yield
    output_name = yield
    header_row = yield
    try:
        output = yield
        # Create target Directory
        os.mkdir(dir_name)
        print("Directory ", dir_name, " Created ")
    except FileExistsError:
        print("Directory ", dir_name, " already exists")
    finally:
        os.chdir(dir_name)
        try:
            with open(dir_name, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(header_row)
                while True:
                    data_row = yield
                    writer.writerow(data_row)
        finally:
            os.chdir('..')
