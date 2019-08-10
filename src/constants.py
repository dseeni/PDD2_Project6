from src.push_pipeline import parse_date

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

# Files
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


# Filter Names for Vehicle_Info:
# ------------------------------------------------------------------------------
muscle_cars = 'American_Muscle_Cars'
japenese_fuel = 'Fuel_Efficent_Japanese_Cars'
heavy_cars = 'Heavy_Cars'
chevy_monte_carlo = 'American_Chevy_Monte_Carlo_Cars'


# Filter Names for Employment_Info:
# ------------------------------------------------------------------------------
kohler_engineering_dept = 'Kohler_Engineering_Dept_Employees'
all_sales_depts = 'All_Sales_Depts_Employees'
rd_employees = 'All_Research_and_Development_Employees'
carroll_all_depts = 'All_Employees_at_Carroll_Company'


# Filter Names for Ticket_Info:
# ------------------------------------------------------------------------------
nyc_bmw_school_zone = 'Bmw_Nyc_School_Zone_Tickets'
honda_no_parking = 'Honda_No_Parking_Tickets'

# Filter Names for Personal_Info:
# ------------------------------------------------------------------------------
icelandic_women = 'Iceland_Speaking_Woman'
telugu_speakers = 'All_Telugu_Speakers'
korean_men = 'Korean_Speaking_Men'

# Filter Names for Update_Status:
# ------------------------------------------------------------------------------
new_updates_march18 = 'Newest_Updates'
old_updates_april17 = 'Oldest_Updates'


# Filter Predicate for Vehicle_Info:
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


# Filter Predicates for'Employment_Info':
# ------------------------------------------------------------------------------
def pred_kohler_engineering_dept(data_row):
    if 'Kohler' in data_row.employer():
        return data_row


def pred_all_sales_depts(data_row):
    if data_row.department == 'Sales':
        return data_row


def pred_rd_employees(data_row):
    if data_row.department == 'Research and Development':
        return data_row


def pred_carroll_all_depts(data_row):
    if 'Carroll' in data_row.employer():
        return data_row


# Filter Predicate for Ticket_Info:
# ------------------------------------------------------------------------------
def pred_nyc_bmw_school_zone(data_row):
    if (data_row.violation_description == "PHTO SCHOOL ZN SPEED VIOLATION"
            and data_row.vehicle_make == "BMW"):
        return data_row


def pred_honda_no_parking(data_row):
    if (data_row.violation_description == "21-No Parking (street clean)"
            and data_row.vehicle_make == "HONDA"):
        return data_row


# Filter Predicate for Personal_Info:
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


# Filter Predicate for Update_Status:
# ------------------------------------------------------------------------------
def pred_new_updates_march18(data_row):
    if (data_row.something >
            parse_date('2018-03-01T00:00:00ZZ', date_keys)):
        return data_row


def pred_old_updates_april17(data_row):
    if (data_row.last_updated <
            parse_date('2017-04-01T00:00:00Z', date_keys)):
        return data_row

