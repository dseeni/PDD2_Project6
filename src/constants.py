
# cars.csv = 407 rows
# employment.csv = 1001 rows
# nyc_parking_ticket_extract.csv = 1001 rows
# personal_info.csv = 1001 rows
# update_status.csv = 1001 row


# Files
fcars = 'input_data/cars.csv'
femployment = 'input_data/employment.csv'
fticket = 'input_data/nyc_parking_tickets_extract.csv'
fpersonal = 'input_data/personal_info.csv'
fupdate = 'input_data/update_status.csv'

fnames = fcars, femployment, fticket, fpersonal, fupdate


# Named Tuple Class Names
cars_class_name = 'Vehicle_Info'
employment_class_name = 'Employment_Info'
ticket_class_name = 'Ticket_Info'
personal_class_name = 'Personal_Info'
update_class_name = 'Update_Status'

class_names = (cars_class_name, employment_class_name,
               ticket_class_name, personal_class_name, update_class_name)

# (Filter Predicate Name, Filer Predicate) --> (Name, Func)

# Filter Names


# Filer Predicate


# Output File name = File Name + Filter Name?


# Date format keys
date_key1 = '%d/%m/%Y'
date_key2 = '%Y-%m-%dT%H:%M:%SZ'
date_keys = (date_key1, date_key2)

# get rid of these you don't need them probably...
idx_make = 0
idx_model = 1
idx_year = 2
idx_vin = 3
idx_color = 4


