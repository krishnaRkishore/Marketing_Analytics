
# Note:

## What does this function do?:

### There are various types of city names in the leas that should be taken into account while summarising for each city

# This function grabs all the city names that go-together and groups them.

# Example: "Bangalore" city takes "online" and "Mysuru" city leads. This function maps both to "Bangalore"

# To do that, there is a dataset being created mapping all the possible city names present till date.



import pandas as pd
import numpy as np

Leads_raw_data_raw = pd.read_csv(r"D:\Python\Test\Leads_data\Leads.csv", engine = "python")
Leads_cities_mapping = pd.read_csv(r"D:\Python\Test\Mapping\Cities mapping\Leads_cities_mapping.csv", engine = "python")


def Leads_city_mapping():
    Leads_raw_data = pd.merge(Leads_raw_data_raw, Leads_cities_mapping, left_on = "City", right_on = "All_cities", how = "left")
    Leads_raw_data = Leads_raw_data.drop(columns = ["City_x","All_cities"])
    Leads_raw_data.columns = ["Date","Lead_id","Lead_src","Service","Campaign","Call_src","Hour","City"]
    Leads_raw_data.City = Leads_raw_data.City.replace(np.nan, "No_data")

    return Leads_raw_data

# data = Leads_city_mapping()
# print(data.head())
# print(data.tail())