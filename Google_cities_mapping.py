
# Note:

## What does this function do?:

### Changes the region in Google campaign data from multiple cities to required 4 cities and Others.

# For example: Region in Google Campaign gets takes as "Secunderabad". This code maps it to "Hyderabad" and returns the resultant dataframe

#              Region in Google Campaign gets takes as "New Delhi". This code maps it to "Others" and returns the resultant dataframe




import pandas as pd

def Google_cities_mapping():

    Google_raw_data_raw = pd.read_csv(r"D:\Python\Test\Google_campaign_data\Google_campaigns.csv", engine = "python")
    State_city_mapping = pd.read_csv(r"D:\Python\Test\Mapping\Cities mapping\Google_cities.csv", engine = "python")

    Google_raw_data = pd.merge(Google_raw_data_raw, State_city_mapping, left_on = "City", right_on = "Google_city", how = "left")
    Google_raw_data.columns = ["Day","Campaign","Region","Campaign_type","Ad_group","Cost","Impressions","Clicks","Conversions","Avg_CPC","Google_city","City"]
    Google_raw_data = Google_raw_data.drop(columns = ["Region","Google_city"])

    return Google_raw_data

# Google_raw_data_raw = pd.read_csv(r"D:\Python\Test\Google_campaign_data\Google_campaigns.csv", engine = "python")
# print(Google_raw_data_raw[Google_raw_data_raw.Day=="2021-03-01"].sum().Cost)

# data = Google_cities_mapping()
# print(data.tail())
