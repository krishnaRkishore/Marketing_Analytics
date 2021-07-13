
import pandas as pd

import Google_cities_mapping

def Google_service_map():
    
    service_ids_data = pd.read_csv(r"D:/Python/Test/Services/Service_list.csv",engine="python")
    service_ids_data["sm_parent_id"][0]="0"
    service_ids_data.columns = ["Dept_id","Service_id","Service","Dept"]
    
    Google_data_raw = Google_cities_mapping.Google_cities_mapping()
    Service_mapping = pd.read_csv(r"D:\Python\Test\Mapping\Service mapping\Service_mapping.csv", engine = "python")
    
    Campaign_array = Google_data_raw["Campaign"].unique()
    
    Extracted_service_array = Service_mapping["Extracted_service"].unique()
    
    Campaign_set = {}

    for i in Campaign_array:
        for j in Extracted_service_array:
                if i.lower().find(j)>=0:
                    Campaign_set[i]=j
                    break
                else:
                    Campaign_set[i]="others"
    
    Google_campaign_service_extracted = pd.DataFrame.from_dict(Campaign_set, orient = "index").reset_index()
    Google_campaign_service_extracted.columns = ["Campaign", "Service"]
    
    Google_mapping = pd.merge(Google_campaign_service_extracted, Service_mapping, left_on = "Service", right_on = "Extracted_service",how = "left" )
    
    Google_service_map = pd.merge(Google_mapping, service_ids_data, left_on = "Service ID", right_on = "Service_id", how = "left")
    
    Google_data = pd.merge(Google_data_raw, Google_service_map, left_on = "Campaign", right_on = "Campaign", how = "left")

    Google_data = Google_data[Google_data.Service_id!=55]
    
    Google_data = Google_data.drop(columns = ["Service_x", "Service_y", "Service ID", "Dept ID", "Dept_id", "Service_id"])
    
    return Google_data


# data = Google_service_map()
# print(data.head())

# print(data[data.Day=="2021-03-01"].sum().Cost)