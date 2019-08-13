import src.push_pipeline
from itertools import chain

# cars.csv = 407 rows
# employment.csv = 1001 rows
# nyc_parking_ticket_extract.csv = 1001 rows
# personal_info.csv = 1001 rows
# update_status.csv = 1001 row

# Date format keys
# ------------------------------------------------------------------------------
date_key1 = '%d/%m/%Y'
date_key2 = '%Y-%m-%dT%H:%M:%SZ'
date_keys = (date_key1, date_key2)

# Input Files
# ------------------------------------------------------------------------------
fcars = 'input_data/cars.csv'
femployment = 'input_data/employment.csv'
fticket = 'input_data/nyc_parking_tickets_extract.csv'
fpersonal = 'input_data/personal_info.csv'
fupdate = 'input_data/update_status.csv'

fnames = fcars, femployment, fticket, fpersonal, fupdate

# Named Tuple Class Names
# ------------------------------------------------------------------------------
cars_class_name = 'Vehicle_Info'
employment_class_name = 'Employment_Info'
ticket_class_name = 'Ticket_Info'
personal_class_name = 'Personal_Info'
update_class_name = 'Update_Status'

class_names = (cars_class_name, employment_class_name,
               ticket_class_name, personal_class_name, update_class_name)

# Output File Names for Vehicle_Info
# ------------------------------------------------------------------------------
muscle_cars = 'American_Muscle_Cars'
japenese_fuel = 'Fuel_Efficent_Japanese_Cars'
heavy_cars = 'Heavy_Cars'
chevy_monte_carlo = 'American_Chevy_Monte_Carlo'

vehicle_output = (muscle_cars, japenese_fuel,
                  heavy_cars, chevy_monte_carlo)

# Output File Names for Employment_Info
# ------------------------------------------------------------------------------
kohler_engineers = 'Kohler_Engineering_Dept_Employees'
sales_employees = 'All_Sales_Depts_Employees'
rd_employees = 'All_Research_and_Development_Employees'
carroll_employees = 'All_Employees_at_Carroll_Company'

emp_output = (kohler_engineers, sales_employees,
              rd_employees, carroll_employees)

# Output File Names for Ticket_Info
# ------------------------------------------------------------------------------
nyc_bmw_school_zone = 'Bmw_Nyc_School_Zone_Tickets'
honda_no_parking = 'Honda_No_Parking_Tickets'

ticket_output = (nyc_bmw_school_zone, honda_no_parking)

# Output File Names for Personal_Info
# ------------------------------------------------------------------------------
icelandic_female_speakers = 'Iceland_Speaking_Woman'
telugu_speakers = 'All_Telugu_Speakers'
korean_male_speakers = 'Korean_Speaking_Men'

personal_output = (icelandic_female_speakers, telugu_speakers,
                   korean_male_speakers)

# Output File Names for Update_Status
# ------------------------------------------------------------------------------
new_updates_march18 = 'Newest_Updates'
old_updates_april17 = 'Oldest_Updates'

update_status_output = (new_updates_march18, old_updates_april17)


# Filter Predicates for Vehicle_Info
# ------------------------------------------------------------------------------
def pred_muscle_cars(data_row):
    if all(v is True for v in (data_row.cylinders > 4,
                               data_row.horsepower > 200,
                               data_row.origin == 'US',
                               data_row.acceleration > 15)):
        return data_row


def pred_japanese_fuel(data_row):
    if all(v is True for v in (data_row.mpg > 35,
                               data_row.origin == 'Japan')):
        return data_row


def pred_heavy_cars(data_row):
    if data_row.weight > 3500:
        return data_row


def pred_chevy_monte_carlo(data_row):
    if data_row.car == 'Chevrolet Monte Carlo':
        return data_row


vehicle_predicates = (pred_muscle_cars, pred_japanese_fuel,
                      pred_heavy_cars, pred_chevy_monte_carlo)


# Filter Predicates for'Employment_Info'
# ------------------------------------------------------------------------------
def pred_kohler_engineering_dept(data_row):
    if 'Kohler' in data_row.employer():
        return data_row


def pred_sales_employees(data_row):
    if data_row.department == 'Sales':
        return data_row


def pred_rd_employees(data_row):
    if data_row.department == 'Research and Development':
        return data_row


def pred_carroll_all_depts(data_row):
    if 'Carroll' in data_row.employer():
        return data_row


emp_predicates = (pred_kohler_engineering_dept, pred_sales_employees,
                  pred_rd_employees, pred_carroll_all_depts)


# Filter Predicates for Ticket_Info
# ------------------------------------------------------------------------------
def pred_nyc_bmw_school_zone(data_row):
    if (data_row.violation_description == "PHTO SCHOOL ZN SPEED VIOLATION"
            and data_row.vehicle_make == "BMW"):
        return data_row


def pred_honda_no_parking(data_row):
    if (data_row.violation_description == "21-No Parking (street clean)"
            and data_row.vehicle_make == "HONDA"):
        return data_row


ticket_predicates = (pred_nyc_bmw_school_zone, pred_honda_no_parking)


# Filter Predicates for Personal_Info
# ------------------------------------------------------------------------------
def pred_icelandic_women(data_row):
    if (data_row.language == 'Icelandic'
            and data_row.gender == 'Female'):
        return data_row


def pred_telugu_speakers(data_row):
    if data_row.language == 'Telugu':
        return data_row


def pred_korean_men(data_row):
    if (data_row.language == 'Korean'
            and data_row.gener == 'Male'):
        return data_row


personal_predicates = (pred_icelandic_women, pred_telugu_speakers,
                       pred_kohler_engineering_dept)


# Filter Predicates for Update_Status
# ------------------------------------------------------------------------------
def pred_new_updates_march18(data_row):
    if (data_row.something >
            src.push_pipeline.date_key_gen('2018-03-01T00:00:00ZZ')):
        return data_row


def pred_old_updates_april17(data_row):
    if (data_row.last_updated <
            src.push_pipeline.date_key_gen('2017-04-01T00:00:00Z')):
        return data_row


update_status_predicates = (pred_new_updates_march18,
                            pred_old_updates_april17)

input_package = (_ for _ in (zip(fnames, class_names)))

predicates = (_ for _ in
              (chain(vehicle_predicates, emp_predicates, ticket_predicates,
                     personal_predicates, update_status_predicates)))

output_files = (_ for _ in (chain(vehicle_output, emp_output, ticket_output,
                                  personal_output, update_status_output)))

output_package = (_ for _ in (zip(predicates, output_files)))

output_dir = 'output_data'
