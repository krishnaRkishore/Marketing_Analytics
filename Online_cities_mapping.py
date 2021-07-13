### Note:

# What does this function do?: 

## Every Online consultation happening in Bangalore, Chennai, Hyderabad and Pune are not being recorded with City names. 
## But rather they are being recorded with City name called "Online"
## In this Code we try to map these "Online" with corresponding city names based on the online consulation location.

## Assumption: Person is from the same place as the Online consultion location suggests.

## Example: A person living in Madurai can do online consultation with a doctor from Chennai. This gets recorded as "Online consultation-Chennai"
# We assume the person is not from Madurai

import pandas as pd
import Apts_cities_mapping

def Online_cities_mapping():

    Apts = Apts_cities_mapping.Apts_cities_mapping()

    Apts_half1 = Apts[Apts.City!="Online"]
    Apts_half2 = Apts[Apts.City=="Online"]

    Online_mapping = pd.read_csv(r"D:\Python\Test\Mapping\Cities mapping\Online_hospitals_mapping.csv")

    Online_mapped = pd.merge(Apts_half2, Online_mapping, left_on = "f2f_hospital", right_on = "Online_hospitals", how = "left")

    Online_mapped = Online_mapped.drop(columns = ["City_x", "Online_hospitals"])

    Online_mapped.columns = ['Lead_id', 'f2f_doctor', 'f2f_hospital', 'f2f_sch_date', 'f2f_sch_time',
        'f2f_comp_date', 'f2f_comp_time', 'Surgery_doctor', 'Surgery_hospital',
        'Surgery_sch_date', 'Surgery_sch_time', 'Surgery_comp_date',
        'Surgery_comp_time', 'Doctor_id', 'Hospital_id', 'Service', 'Owner',
        'Lead_src', 'Status', 'Surgeon_id', 'Surg_hospital_id',
        'Surgery_amount', 'Insurance_amount', 'Copay_amount', 'Cash_amount',
        'Discount', 'Final_amount', 'Dept', 'Surgery_required_date',
        'Surgery_required_time', 'Call_src','City']
    
    Apts_online_mapped =  pd.concat([Apts_half1, Online_mapped], axis = 0, ignore_index = True)

    return Apts_online_mapped




