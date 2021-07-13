
import pandas as pd

import FB_state_city_mapping

def FB_service_map():
    
    service_ids_data = pd.read_csv(r"D:/Python/Test/Services/Service_list.csv",engine="python")
    service_ids_data["sm_parent_id"][0]="0"
    service_ids_data.columns = ["Dept_id","Service_id","Service","Dept"]
    
    FB_data_raw = FB_state_city_mapping.FB_state_city_map()
    Service_mapping = pd.read_csv(r"D:\Python\Test\Mapping\Service mapping\Service_mapping.csv", engine = "python")
    
    Ad_set_array = FB_data_raw["Ad set name"].unique()
    
    Extracted_service_array = Service_mapping["Extracted_service"].unique()
    
    Ad_set_set = {}

    for i in Ad_set_array:
        for j in Extracted_service_array:
                if i.lower().find(j)>=0:
                    Ad_set_set[i]=j
                    break
                else:
                    Ad_set_set[i]="others"
    
    FB_adset_service_extracted = pd.DataFrame.from_dict(Ad_set_set, orient = "index").reset_index()
    FB_adset_service_extracted.columns = ["Ad_set", "Service"]
    
    FB_mapping = pd.merge(FB_adset_service_extracted, Service_mapping, left_on = "Service", right_on = "Extracted_service",how = "left" )
    
    FB_service_map = pd.merge(FB_mapping, service_ids_data, left_on = "Service ID", right_on = "Service_id", how = "left")
    
    FB_data = pd.merge(FB_data_raw, FB_service_map, left_on = "Ad set name", right_on = "Ad_set", how = "left")
    
    FB_data = FB_data.drop(columns = ["Service_x", "Service_y", "Service ID", "Dept ID", "Dept_id", "Service_id"])
    
    return FB_data


# data = FB_service_map()
# print(data.head())