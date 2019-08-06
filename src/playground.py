from src.constants import *
import csv
from itertools import islice


def data_reader(f_name):
    f = open(f_name)
    try:
        dialect = csv.Sniffer().sniff(f.read(2000))
        f.seek(0)
        reader = csv.reader(f, dialect=dialect)
        yield from reader
    finally:
        f.close()


cars = data_reader(fcars)


# print('20:', *list(islice(cars, 10)), sep='\n')
# for row in cars:
#     print(row)
