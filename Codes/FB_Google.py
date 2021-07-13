 # Notes: 

# What does this function do?: 
#   1. Returns 2 dataframes - Facebook and Google as sources in each
#   2. Removes MRI service from the dataframe

# How this work?
#   1. Make sure the services have "Service" as the column name
#   2. Input the dataframe 


import pandas as pd 
import numpy as np

### Lets first remove the MRI data from the dataframe

def MRI_filter(input_df):

    service_ids_data = pd.read_csv(r"D:\Python\Test\Services\Service_list.csv",engine="python")
    service_ids_data["sm_parent_id"][0]="0"
    service_ids_data.columns = ["Dept_id","Service_id","Service","Dept"]

    input_df = input_df.replace(r"\N", "Service NA")
    merged_data = pd.merge(input_df,service_ids_data, on="Service",how = "left")
    non_MRI_leads = merged_data[merged_data["Dept_id"]!="55"]
    return non_MRI_leads

## Facebook lead sources

#facebook_formfill_chat = ["Facebook","Facebook - Chat","Facebook-RM","FB","lptest","lptest-rm","Instagram","Instagram - Chat","Fcebook"]
#facebook_calls = ["inbound","Inbound-instagram"]
facebook_formfill_chat = ["facebook","facebook - chat","facebook-rm","fb","lptest","lptest-rm","instagram","instagram - Chat","fcebook","intstagram"]
facebook_calls = ["inbound","inbound-instagram"]


## Google Lead sources
google_formfill_chat = ["google","google - chat","gdn","google-display","google-display-chat","googlesearch_npd","youtube","youtube - chat","google_npd"]
google_calls = ["inbound-goog","inbound-gdn","inbound-youtube"]

def channels(input_dataframe):

### Lets take the non-MRI data and split Facebook and Google 

    non_MRI = MRI_filter(input_dataframe)
    #non_MRI_df = non_MRI.dropna()
    non_MRI_df1 = non_MRI.fillna("Not available")
    non_MRI_df = non_MRI_df1.replace("\\N","Not available")
    if "Lead_src" in non_MRI_df.columns and "Call_src" in non_MRI_df.columns:
        non_MRI_df["Lead_src"] = [i.lower() for i in non_MRI_df["Lead_src"].to_list()]
        non_MRI_df["Call_src"] = [j.lower() for j in non_MRI_df["Call_src"].to_list()]
        facebook_data = non_MRI_df[(non_MRI_df["Lead_src"].isin(facebook_formfill_chat)) | (non_MRI_df["Call_src"].isin(facebook_calls)) ]
        google_data = non_MRI_df[(non_MRI_df["Lead_src"].isin(google_formfill_chat)) | (non_MRI_df["Call_src"].isin(google_calls)) ]
        return facebook_data, google_data
    else:
        return ("invalid","check column names")



# surgery_data = pd.read_csv(r"D:\Python\Test\Surgery\Surgery_completed.csv",parse_dates = ["f2f_sch","f2f_comp","surgery"])
# surgery_data_edited = surgery_data.drop(columns = ["f2f_Doctor","f2f_hospital","surg_hospital","the_owner","surgery_date","doctor","f2f_comp","f2f_sch"])
# columns = ["Lead_id","City","Service","Lead_src","Dept","Call_src"]
# surgery_data_edited.columns = columns



# a,b  = channels(surgery_data_edited)
# print(a.head())
# print("***************")
# print(b.head())

   
