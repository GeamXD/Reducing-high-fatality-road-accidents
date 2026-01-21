import pandas as pd
import numpy as np
from calendar import day_name
from utilites import *

# Load datasets
accidents = pd.read_csv('data/accident-data.csv')
accidents_lookups = pd.read_csv('data/road-safety-lookups.csv')

# FIll labels for each field names
field_names = ['urban_or_rural_area', 'carriageway_hazards', 'special_conditions_at_site', 'road_surface_conditions',
               'weather_conditions', 'light_conditions', 'pedestrian_crossing_human_control', 'pedestrian_crossing_physical_facilities',
               'second_road_class', 'junction_control', 'road_type', 'first_road_class', 'junction_detail',
               'day_of_week', 'accident_severity']

# Loops through fields and replaces with correct labals
for field in field_names:
    field_labels = get_labels_easy(accidents_lookups, field)
    if 0 in field_labels.keys() and isinstance(field_labels[0], float):
        field_labels[0] = 'None'
    accidents[field] = accidents[field].replace(field_labels)

# Create timestamp column
accidents['timestamp'] = pd.to_datetime(
    accidents['date'] + ' ' + accidents['time']
    , format='mixed'
)

# Drop duplicates if any
accidents.drop_duplicates(inplace=True)

# Print dataset info
accidents.info()

# Save dataset
accidents.to_csv('data/accidents_cleaned.csv', index=False)