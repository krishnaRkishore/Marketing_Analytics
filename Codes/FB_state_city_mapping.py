
# Note:

## What does this function do?:

### Changes the region in facebook adset data from state to city.

# For example: Region in Facebook adset gets takes as "Karnataka". This code maps it to "Bangalore" and returns the resultant dataframe




import pandas as pd



FB_raw_data_raw = pd.read_csv(r"D:\Python\Test\FB_campaign_data\FB_data.csv", engine = "python")
State_city_mapping = pd.read_csv(r"D:\Python\Test\Mapping\Cities mapping\State_City_mapping.csv", engine = "python")


def FB_state_city_map():
    FB_raw_data = pd.merge(FB_raw_data_raw, State_city_mapping, left_on = "Region", right_on = "State", how = "left")
    return FB_raw_data

# data = FB_state_city_map()
# print(data[data["Reporting ends"]=="2021-06-17"].sum())