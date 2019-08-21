from src.constants import *
from collections import namedtuple
from contextlib import contextmanager, ExitStack
from datetime import datetime
import csv
import os
from copy import deepcopy
from itertools import islice, cycle, count


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
# # TODO: Lookup the print_file_row() function in Freds notes on GitHub.

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


# TODO: cycle_rows has to send packets of up to 5 rows at once, as files are
#  exhausted they will be marked as None, down the pipe if its None skip that
#  row when parsing, filtering, and writing to disk
@coroutine
def cycle_rows(target):
    readers = yield
    reader_idx_list = list(range(len(readers)))  # 5 in our case
    idx_tracker = readers_idx_list = list(range(len(readers)))
    cycler = cycle(readers_idx_list)
    counter = count(0)
    row_package = []
    while True:
        # # yield every 5 rows
        reader_idx = next(cycler)
        if (next(counter) >= (len(readers) - 1)
                # and reader_idx_list[reader_idx] is not None
                and reader_idx % len(readers) == 0): # do you need this?
            target.send(row_package)
            row_package.clear()
            yield
        next(counter)
        try:
            # go until all readers are exhausted
            if all(idx is None for idx in reader_idx_list):
                break
            # skip exhausted file readers
            if idx_tracker[reader_idx] is None:
                next(counter)
                continue
            else:
                row_package.append(next(readers[reader_idx]))
                next(counter)
        except StopIteration:  # skip over exhausted readers
            reader_idx_list[reader_idx] = None
            next(counter)
            continue


@coroutine
def pipeline_coro():
    with file_readers(data_package) as readers:
        for input_data, output_data in data_package:

            # CONSTANTS:
            nt_classes = [input_data[1]]
            output_files = [output_data[0]]
            filters = [output_data[1]]

            # DECLARE --> From the bottom up stack
            broadcaster = broadcast(filter_names)
            row_parser = data_parser(broadcaster)
            date_key = date_key_gen(row_parser)
            row_key = row_key_gen(date_key_gen)
            field_name_gen = gen_field_names(data_parser)  # send class_names
            header_row = header_extract(field_name_gen)
            row_cycler = cycle_rows(header_extract)

            # pipeline sends gen_date_key the date_keys_tuple
            # once for parse_key generation and once for processing
            # send the first data row twice
            # read header and send to data_fields gen
            # right away to field_name_generator
            # SEND DATA:
            row_cycler.send(readers)
            # send class_names and header_row
            field_name_gen.send(nt_classes)
            header_row.send(next(row_cycler))  # --> send to gen_field_names
            # sample row for row_key:
            first_delimited_row = next(row_cycler)
            row_key.send(first_delimited_row)
            date_key.send(date_keys)  # <-- y1 only happens ONCE
            date_key.send(first_delimited_row)  # <-- y2 await row_key_gen

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


# input_data parser needs headers and data_key sent to it
@coroutine
def header_extract(target):  # --> send to gen_field_names
    while True:
        row_package = yield  # --> from row_cycle
        headers = []
        for row in row_package:
            headers.append(tuple(map(lambda l: l.replace(" ", "_"),
                                 tuple(map(lambda l: l.lower(),
                                           (item for item in row))))))
        target.send(headers)


@coroutine
def row_key_gen(target):  # from coro to date parser:-->
    while True:
        data_rows = yield  # from cycle_rows
        row_parse_keys = deepcopy(data_rows)  # list of lists
        # print('200:', 'row_parse_keys ''='' ', row_parse_keys)
        for parse_keys in row_parse_keys:  # for each sublists in list
            for value in parse_keys:  # for values in each sublist
                if value is None:
                    (row_parse_keys[row_parse_keys.index(parse_keys)]
                     [parse_keys.index(value)]) = None
                elif all(c.isdigit() for c in value):
                    (row_parse_keys[row_parse_keys.index(parse_keys)]
                     [parse_keys.index(value)]) = int
                elif value.count('.') == 1:
                    try:
                        float(value)
                        (row_parse_keys[row_parse_keys.index(parse_keys)]
                         [parse_keys.index(value)]) = float
                    except ValueError:
                        (row_parse_keys[row_parse_keys.index(parse_keys)]
                         [parse_keys.index(value)]) = str
                else:
                    (row_parse_keys[row_parse_keys.index(parse_keys)]
                     [parse_keys.index(value)]) = str
        target.send(row_parse_keys)


@coroutine
def date_key_gen(target):
    date_keys_tuple = yield  # <-sent by pipeline_coro ONCE per file run
    delimited_rows = yield  # <-sent by row_cycler ONCE per file
    while True:
        partial_keys = yield
        keys_copy = deepcopy(partial_keys)  # list of lists
        # print('230:', 'type(keys_copy) ''='' ', type(keys_copy))
        # print('230:', 'keys_copy ''='' ', keys_copy)
        keys_idxs = [tuple(i for i in range(len(sub_key))) for sub_key
                     in keys_copy]
        parse_guide = [list(zip(keys_copy[i], delimited_rows[i], keys_idxs[i]))
                       for i in range(len(keys_copy))]
        # print('243:', *parse_guide, sep='\n')
        date_func = None
        for parser in parse_guide:
            for data_type, item, idx in parser:
                if data_type == str:
                    for _ in range(len(date_keys_tuple)):
                        # print('inspect:', keys_copy[parse_guide.index(
                        #     parser)])
                        try:
                            datetime.strptime(item, date_keys_tuple[_])
                            # if date_keys_tuple[_] == '%m/%d/%Y':
                            #     key = '%m/%d/%YT%H:%M:%SZ'
                            #     # key = '%m/%d/%YT%H:%M:%SZ'
                            # # "%Y-%m-%dT%H:%M:%S.%fZ"
                            # else:
                            key = date_keys_tuple[_]
                            date_func = lambda v: datetime.strptime(v, key)
                            (keys_copy[parse_guide.index(parser)]
                             [idx]) = date_func
                            print('dfid', date_keys_tuple[_], id(date_func))
                            print(parse_guide.index(parser), idx,
                                  date_keys_tuple[_])
                            print('date found')
                            print(item, 'item')
                            continue
                        except ValueError:
                            continue
                        # except ValueError:
                        #     continue
                        except IndexError:
                            break
        # print('257:', *keys_copy, sep='\n')
        target.send(keys_copy)


@coroutine
def gen_field_names(target):  # sends to data_caster
    while True:
        class_names = yield  # from pipeline_coro a list of lists
        header_rows = yield  # from header_extract a list of lists
        assert len(class_names) == len(header_rows)
        print(class_names)
        print(header_rows)
        for i in range(len(class_names)):
            data_fields = [namedtuple(class_names, header_rows[i])]
        assert len(data_fields) == len(class_names)*2
        target.send(data_fields)


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
            with open(f_name, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                while True:
                    data_row = yield
                    writer.writerow(data_row)
        finally:
            os.chdir('..')
