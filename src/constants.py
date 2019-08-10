# cars.csv = 407 rows
# employment.csv = 1001 rows
# nyc_parking_ticket_extract.csv = 1001 rows
# personal_info.csv = 1001 rows
# update_status.csv = 1001 row


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

# Date format keys
# ------------------------------------------------------------------------------
date_key1 = '%d/%m/%Y'
date_key2 = '%Y-%m-%dT%H:%M:%SZ'
date_keys = (date_key1, date_key2)

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
all_r_and_d_employees = 'All_Research_and_Development_Employees'
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
new_updates_march18 = 'Newest_Updates' # March 2018
old_updates_april17 = 'Oldest_Updates' # April 2017


# Filter Predicate for Vehicle_Info:
# ------------------------------------------------------------------------------
# American Muscle Cars:
def pred_muscle_cars(data_row):
    if all(v is True for v in (data_row.cylinders > 4,
                               data_row.horsepower > 200,
                               data_row.origin == 'US',
                               data_row.acceleration > 15)):
        return data_row


# Japanese Fuel Efficient Cars:
def pred_japanese_fuel(data_row):
    if all(v is True for v in (data_row.mpg > 35,
                               data_row.origin == 'Japan')):
        return data_row


# Heavy Cars:
def pred_heavy_cars(data_row):
    if data_row.mpg > 3500:
        return data_row


# Chevy Monte Carlos:
def pred_chevy_monte_carlo(data_row):
    if data_row.car == 'Chevrolet Monte Carlo':
        return data_row



# Filter Predicates for'Employment_Info':
# ------------------------------------------------------------------------------
def pred_kohler_engineering_dept(data_row):
    pass



def pred_all_sales_depts(data_row):
    pass



def pred_all_r_and_d_employees(data_row):
    pass



def pred_carroll_all_depts(data_row):
    pass



# Filter Predicate for Ticket_Info:
# ------------------------------------------------------------------------------
# nyc_bmw_school_zone
# honda_no_parking


# Filter Names for Ticket_Info:
# nyc_bmw_school_zone
# honda_no_parking



# Filter Predicate for Personal_Info:
# ------------------------------------------------------------------------------

# Filter Predicate for Update_Status:
# ------------------------------------------------------------------------------





# Output File name = File Name + Filter Name?




# filter_older = filter_data(lambda d: d[idx_year] <= 2010, out_older)
# (filter_name, F(n))                     \s
# power_cars_filter:
# cylinder > 4,  Horsepower > 200,  and Origin:USA

# def pred_ford_green(data_row):
#     return (data_row[idx_make].lower() == 'ford'
#             and data_row[idx_color].lower() == 'green')

# predicate = lambda d: getattr(d, 'What') > 100
# all(v is True for v in (x<10, x>100, x==100))

# cylinder > 4,  horsepower > 200,  and Origin:Europe
# cylinder > 4,  Horsepower > 200,  and Origin:Japan

# filter names
# muscle_cars


# filter_older = filter_data(lambda d: d[idx_year] <= 2010, out_older)

