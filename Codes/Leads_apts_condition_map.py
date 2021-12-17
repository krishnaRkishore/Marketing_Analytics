import pandas as pd

Condition_map = pd.read_csv(r"D:\Python\Test\Services\Condition_map.csv")

def Leads_condition_mapping(input_df):

    merged = pd.merge(input_df, Condition_map, on = "Service", how = "left")
    merged = merged[['Date', 'Lead_id', 'Lead_src', 'Condition', 'Campaign', 'Call_src',
       'Hour', 'City']]
    merged.rename(columns={"Condition": "Service"}, inplace = True)
    
    return merged

def Apts_condition_mapping(input_df):

    apts_merged = pd.merge(input_df, Condition_map, on = "Service", how = "left")
    apts_merged = apts_merged[['Lead_id', 'f2f_doctor', 'f2f_hospital', 'f2f_sch_date', 'f2f_sch_time',
       'f2f_comp_date', 'f2f_comp_time',"City",'Condition',
       'the_Owner', 'Lead_src','Dept', 'Call_src']]
    apts_merged.rename(columns={"Condition": "Service"}, inplace = True)
    
    return  apts_merged

