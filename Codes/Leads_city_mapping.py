from numpy.core.fromnumeric import sort
import pandas as pd
import numpy as np
import Leads_apts_condition_map

def sorting_nulls():
    Leads = pd.read_csv(r"D:\Python\Test\Leads_data\Leads.csv", engine="python",encoding = "ISO-8859-1")
    Leads.replace(np.nan, "None")
    Leads.replace(r"\N", "None")

    # Mapping all Surgeries to respective Condition

    Service = pd.read_csv(r"D:\Python\Test\Services\Service_list.csv", engine = "python")

    Leads_2 = pd.merge(Leads, Service, left_on = "Service", right_on = "Service", how = "left")

    Leads_2 = Leads_2[["Date", "Lead_id", "Lead_src", "Condition_map", "Campaign", "City", "Call_src", "Hour"]]
    Leads_2.columns = ["Date", "Lead_id", "Lead_src", "Service", "Campaign", "City", "Call_src", "Hour"]


    # Mapping a few temporary sources

    Leads_2.City = np.where(((Leads_2.Call_src=="inbound-Pune-Facebook") | (Leads_2.Call_src=="inbound-Pune-Goog")), "Pune", Leads_2.City)
    Leads_2.Call_src = np.where((Leads_2.Call_src=="inbound-Pune-Facebook"),"inbound",Leads_2.Call_src)
    Leads_2.Call_src = np.where((Leads_2.Call_src=="inbound-Pune-Goog"),"inbound-Goog",Leads_2.Call_src)
    Leads_2.Service = np.where((Leads_2.Call_src=="inbound-Circum"),"Circumcision",Leads_2.Service)

    Leads_2.Lead_src = np.where(((Leads_2.Date>="2021-10-15") & (Leads_2.Lead_src=="Chat") & (Leads_2.Service=="Varicose Veins")), "Google - Chat", Leads_2.Lead_src)

    return Leads_2

def Leads_city_map():

    Leads_cities_mapping = pd.read_csv(r"D:\Python\Test\Mapping\Cities mapping\Leads_cities_mapping.csv", engine = "python")

    Leads_raw_data_raw = sorting_nulls()
    Leads_raw_data = pd.merge(Leads_raw_data_raw, Leads_cities_mapping, left_on = "City", right_on = "All_cities", how = "left")
    Leads_raw_data = Leads_raw_data.drop(columns = ["City_x","All_cities"])

    Leads_raw_data.columns = ["Date","Lead_id","Lead_src","Service","Campaign","Call_src","Hour", "SID", "Message","City"]
    Leads_raw_data = Leads_raw_data[["Date","Lead_id","Lead_src","Service","Campaign","Call_src","Hour","City"]]

    Leads_raw_data.City = Leads_raw_data.City.replace(np.nan, "No_data")
    Leads_raw_data.City = Leads_raw_data.City.replace(r'\N', "No_data")

    Leads_condition_mapped  = Leads_apts_condition_map.Leads_condition_mapping(Leads_raw_data)

    Leads_condition_mapped = Leads_condition_mapped.loc[(Leads_condition_mapped.Lead_src.str.lower()!="doctor referal") 
    & (Leads_condition_mapped.Lead_src.str.lower()!="patient referal") & (Leads_condition_mapped.Lead_src.str.lower()!="b2b sales")
    & (Leads_condition_mapped.Lead_src.str.lower()!="walk in")]

    return Leads_condition_mapped







    
    
