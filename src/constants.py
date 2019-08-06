
# cars.csv = 407 rows
# employment.csv = 1001 rows
# nyc_parking_ticket_extract.csv = 1001 rows
# personal_info.csv = 1001 rows

# Files
fcars = 'input_data/cars.csv'
femployment = 'input_data/employment.csv'
fticket = 'input_data/nyc_parking_tickets_extract.csv'
fpersonal = 'input_data/personal_info.csv'

fnames = fcars, femployment, fticket, fpersonal


# Named Tuple Class Names
cars_class_name = 'Vehicle_Info'
employment_class_name = 'Employment_Info'
ticket_class_name = 'Ticket_Info'
personal_class_name = 'Personal_Info'

class_names = (cars_class_name, employment_class_name,
               ticket_class_name, personal_class_name)


# idx_make = 0
# idx_model = 1
# idx_year = 2
# idx_vin = 3
# idx_color = 4


