from src.constants import *
from collections import namedtuple
from contextlib import contextmanager
import csv
from itertools import islice


def coroutine(fn):
    def inner(*args, **kwargs):
        g = fn(*args, **kwargs)
        next(g)
        return g
    return inner

@contextmanager
def data_reader(file_name, single_parser, single_class_name):
    file_obj = open(file_name)
    try:
        dialect = csv.Sniffer().sniff(file_obj.read(2000))
        file_obj.seek(0)
        reader = csv.reader(file_obj, dialect)
        headers = map(lambda l: l.lower(), next(reader))
        DataTuple = namedtuple(single_class_name, headers)
        yield (DataTuple(*(fn(value) for value, fn
                           in zip(row, single_parser))) for row in reader)

    finally:
        try:
            next(file_obj)
        except StopIteration:
            pass
        # print('closing file')
        file_obj.close()

@coroutine
def save_data(f_name, headers):
    with open(f_name, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        while True:
            data_row = yield
            writer.writerow(data_row)


@coroutine
def filter_data(filter_predicate, target):
    while True:
        data_row = yield
        if filter_predicate(data_row):
            target.send(data_row)


@coroutine
def broadcast(targets):
    while True:
        data_row = yield
        for target in targets:
            target.send(data_row)


@coroutine
def pipeline_coro():
    out_pink_cars = save_data('pink_cars.csv', headers)
    out_ford_green = save_data('ford_green.csv', headers)
    out_older = save_data('older.csv', headers)

    filter_pink_cars = filter_data(lambda d: d[idx_color].lower() == 'pink',
                                   out_pink_cars)

    def pred_ford_green(data_row):
        return (data_row[idx_make].lower() == 'ford'
                and data_row[idx_color].lower() == 'green')

    filter_ford_green = filter_data(pred_ford_green, out_ford_green)
    filter_older = filter_data(lambda d: d[idx_year] <= 2010, out_older)

    filters = (filter_pink_cars, filter_ford_green, filter_older)

    broadcaster = broadcast(filters)

    while True:
        data_row = yield
        broadcaster.send(data_row)
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

# TODO: Implement averager for average length of lines in file
# # TODO: Lookup the print_file_row() function in Fred's notes on GitHub.


# headers = ('make', 'model', 'year', 'vin', 'color')

# def get_dialect(file_obj):
#     sample = file_obj.read(2000)
#     dialect = csv.Sniffer().sniff(sample)
#     file_obj.seek(0)
#     return dialect



