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


def pipeline_coro():
    with file_readers(data_package) as readers:

        # CONSTANTS:
        nt_class_names = [data[0][1] for data in data_package]
        output_package = [data[1] for data in data_package]

        # DECLARE --> From the bottom up stack
        writer = save_data()
        data_filter = filter_data(writer)
        broadcaster = broadcast(data_filter)
        parse_data = data_parser(broadcaster)
        date_key = date_key_gen(parse_data)
        row_key = row_key_gen((parse_data, date_key))
        field_name_gen = gen_field_names(parse_data)
        headers = header_extract((field_name_gen, writer))
        row_cycler = cycle_rows(headers)

        # SEND PREREQUISITES FIRST
        writer.send(output_dir)
        field_name_gen.send(nt_class_names)
        date_key.send(date_keys)
        row_cycler.send(readers)
        broadcaster.send(output_package)

        # SEND DATA:
        try:
            row_cycler.send((date_key, row_key))
        except StopIteration:
            pass


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
def header_extract(targets):  # --> send to gen_field_names
    while True:
        row_package = yield  # --> from row_cycle
        headers = []
        for row in row_package:
            headers.append(tuple(map(lambda l: l.replace(" ", "_"),
                                 tuple(map(lambda l: l.lower(),
                                           (item for item in row))))))
        for target in targets:
            target.send(headers)  # --> sending [list of tuples of headers]


@coroutine
def gen_field_names(target):  # sends to data_caster
    while True:
        nt_class_names = yield  # from pipeline_coro a list of lists
        raw_header_rows = yield  # from header_extract a list of lists
        # print('headers', header_row_package)
        data_fields = [namedtuple(nt_class_names[i], raw_header_rows[i])
                       for i in range(len(nt_class_names))]
        target.send(data_fields)


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
            elif value is None:
                parse_keys[parse_keys.index(value)] = None
            # check if len(value) > 0 to guard against empty string
            elif all(c.isdigit() for c in value) and len(value) > 0:
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
def data_parser(target):
    # needs file_name, parse_keys, headers, single_class_name:
    nt_classes = yield  # <-- from gen_field_names list of field names
    # parse then pack data:
    while True:
        sub_key_ranges = yield  # <-- from row_key_gen
        flat_raw_data = yield
        parse_keys = yield  # <-- from gen_date_parse_key list of lists
        zip_func_data = [*zip(parse_keys, flat_raw_data)]
        # print(zip_func_data)
        casted = []
        for unparsed_data in zip_func_data:
            func = unparsed_data[0]
            # print('290:', 'func ''='' ', func)
            data = unparsed_data[1]
            # print('292:', 'data ''='' ', data)
            # print('294:', 'type(data) ''='' ', type(data))
            if func is None:
                casted.append(None)
            else:
                casted.append(func(data))
        packed = pack(casted, sub_key_ranges)
        packets = []
        # for packet in packed:
        for i in range(len(packed)):
            if packed[i] == [None]:
                packets.append(None)
            else:
                packets.append(nt_classes[i](*packed[i]))
        target.send(packets)


@coroutine
def broadcast(target):
    output_data_package = yield  # sent ONCE from pipeline_coro()
    while True:
        packed_rows = yield
        assert len(packed_rows) == 5
        for row in packed_rows:
            if row is None:
                continue
            else:
                output_data = output_data_package[packed_rows.index(row)]
                target.send(output_data)
                target.send(row)


@coroutine
def filter_data(target):
    while True:
        output_data = yield
        row = yield
        for output in output_data:
            output_file_name = output[0]
            predicate = output[1]
            if predicate(row) is not None:
                target.send(output_file_name)
                target.send(row)


@coroutine
def save_data():
    # 'ff_name, headers, dir_name'
    output_dir_name = yield
    header_rows = yield
    path = os.getcwd()
    print('297:', 'path ''='' ', path)
    while True:
        output_file_name = yield
        data_row = yield
        try:
            # Create target Directory
            os.mkdir(output_dir_name)
        except OSError:
            pass
        finally:
            print('307:', 'output_dir_name ''='' ', output_dir_name)
            os.chdir(path + "/" + output_dir_name)
            path = os.getcwd()
            if os.path.isfile(output_file_name):
                with open(output_file_name, 'a', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(data_row)
            else:
                with open(output_file_name, 'w+', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(header_rows)
                    writer.writerow(data_row)
            os.chdir('..')


pipeline_coro()

