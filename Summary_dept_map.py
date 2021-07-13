### Notes: 

### What does this function return ?

#   1. A dataframe with daywise summary for the spends, leads, appointments and surgeries
#   2. Added columns of CPL, CPA and CPS
#   3. Split of Total leads, appointment and surgeries from Facebook and Google

### What does this function take as input?:

#   1. Nothing. The function automatically reads the required csv files and returns the dataframes mentioned above.


import pandas as pd
import datetime
import FB_Google
import non_mri
import numpy as np
import time
import FB_service_map
import Google_service_map
import Leads_cities_mapping
import Apts_cities_mapping
import Online_cities_mapping


class summary_cities:

    def __init__(self):
        
        self.FB_data_raw = FB_service_map.FB_service_map()
        self.Google_data_raw = Google_service_map.Google_service_map()
        self.leads_data_raw = Leads_cities_mapping.Leads_city_mapping()
        self.apts_data_raw = Online_cities_mapping.Online_cities_mapping()

    def summary(self, city):

        FB_raw_data_raw = self.FB_data_raw
        Google_spends_data_raw = self.Google_data_raw
        leads_raw = self.leads_data_raw
        apts_raw = self.apts_data_raw

        FB_raw_data = FB_raw_data_raw[FB_raw_data_raw.City==city]
        Google_spends_data = Google_spends_data_raw[Google_spends_data_raw.City==city]
        leads_data = leads_raw[leads_raw.City==city]
        apts_data = apts_raw[apts_raw.City==city]

        #FB_raw_data = FB_state_city_mapping.FB_state_city_mapping()

        
        ### Lets define a daterange dataframe to be joined to every dataframe in this notebook

        

        #FB_raw_data = FB_state_city_mapping.FB_state_city_mapping()


        ### Lets define a daterange dataframe to be joined to every dataframe in this notebook

        dates = pd.date_range('2019-01-01', time.strftime("%Y-%m-%d"), freq='D').to_list()
        dates_df = pd.DataFrame({"Date":dates})

        #### Lets process Facebook Spend here

        FB_data_dropped = FB_raw_data[["Reporting ends", "Ad set name", "Results",
        "Amount spent (INR)", "Impressions", "Link clicks",
        "CTR (link click-through rate)", "City", "Extracted_service", "Service", "Dept"]]

        new_columns = ["Date", "FB_Ad_set", "FB_Results",
        "Spend", "FB_Impressions", "FB_Link_clicks",
        "FB_CTR", "City", "FB_Extracted_service", "Service", "Dept"]
        FB_data_dropped.columns = new_columns
        FB_data_dropped["FB_spend"] = FB_data_dropped["Spend"]*1.18

        ### Now lets summarise the FB spend data daywise to be later attached with the leads, appointments and surgery

        FB_datewise_spends = FB_data_dropped.groupby(["Dept", "Service", "Date"]).sum().drop(columns = ["Spend"])

        FB_daily_spends = FB_datewise_spends.reset_index()
        FB_daily_spends.FB_spend = FB_daily_spends.FB_spend.replace(np.nan, 0)
        FB_daily_spends["Date"] = pd.to_datetime(FB_daily_spends["Date"], format = "%Y-%m-%d")
        FB_spends = pd.merge(dates_df,FB_daily_spends, on = "Date", how = "left")
        #FB_spends.head()


        ### Lets process Google Spend here

        Google_spends_data = Google_spends_data[["Day","Campaign", "Cost", "Impressions", "Clicks", "Conversions", "City", 
                                        "Extracted_service", "Service", "Dept"]]
        Google_spends_data.columns = ["Date","Google_Campaign", "Spend", "Google_Impressions", "Google_Clicks",
                            "Google_Conversions", "City", "Extracted_service", "Service", "Dept"]
        Google_spends_data["Google_spend"] = Google_spends_data["Spend"]*1.18

        ### Now lets summarise the Google spend data daywise to be later attached with the leads, appointments and surgery

        Google_datewise_spends = Google_spends_data.groupby(["Dept", "Service","Date"]).sum().drop(columns = ["Spend"])

        Google_daily_spends = Google_datewise_spends.reset_index()
        Google_daily_spends.Google_spend = Google_daily_spends.Google_spend.replace(np.nan, 0)
        Google_daily_spends["Date"] = pd.to_datetime(Google_daily_spends["Date"])
        Google_spends = pd.merge(dates_df,Google_daily_spends, on = "Date", how = "left")
        #Google_spends.head()

        Spends_data = pd.merge(FB_spends, Google_spends, on = ["Date","Dept","Service"], how = "outer")


        ### Lets process Leads data here

        non_mri_leads = non_mri.MRI_filter(leads_data)

        non_mri_leads.City = non_mri_leads.City.replace(np.nan, "No_data")


        # Lets read overall leads and split it to daily leads for facebook and google

        overall_leads = non_mri_leads.groupby(["Date", "Dept", "Service"]).count()
        overall_leads = overall_leads.reset_index()

        daywise_leads = overall_leads[["Date", "Dept", "Service", "Lead_id"]]
        daywise_leads.columns = ["Date", "Dept", "Service","Total_leads"]
        daywise_leads.Date = pd.to_datetime(daywise_leads.Date)

        FB_leads_df, Google_leads_df = FB_Google.channels(leads_data)

        FB_leads = FB_leads_df[['Date',"Dept","Service",'Lead_id']]
        FB_leads.columns = ["Date","Dept","Service","FB_leads"]

        FB_daywise_leads = FB_leads.groupby(["Date","Dept","Service"]).count()
        FB_daily_leads = FB_daywise_leads.reset_index()

        FB_daily_leads.Date = pd.to_datetime(FB_daily_leads.Date, format = "%Y-%m-%d")


        Google_leads = Google_leads_df[['Date',"Dept","Service",'Lead_id']]
        Google_leads.columns = ["Date","Dept","Service","Google_leads"] 

        Google_daywise_leads = Google_leads.groupby(["Date","Dept","Service"]).count()
        Google_daily_leads = Google_daywise_leads.reset_index()

        Google_daily_leads.Date = pd.to_datetime(Google_daily_leads.Date, format = "%Y-%m-%d")


        dummy1 = pd.merge(dates_df, FB_daily_leads, on = "Date", how = "left")
        dummy2 = pd.merge(dummy1, Google_daily_leads, on = "Date", how = "left")

        #daywise_leads.Date = pd.to_datetime(daywise_leads.Date, format = "%Y-%m-%d")

        leads_df = pd.merge(FB_daily_leads, Google_daily_leads, on = ["Date", "Dept", "Service"], how = "outer")
        #leads_df.head()

        ### Lets combine Spends and Leads dataframes

        Spends_leads_df = pd.merge(Spends_data, leads_df, on = ["Date", "Dept", "Service"], how = "outer")

        Spends_leads_data = pd.merge(Spends_leads_df, daywise_leads, on = ["Date", "Dept", "Service"], how = "outer")

        

        apts_data_dropped = apts_data[["Lead_id","f2f_sch_date", "f2f_comp_date","Service","Dept"]]
        apts_data_dropped.columns = ["Lead_id","f2f_sch","f2f_comp","Service", "Dept"]

        apts = non_mri.MRI_filter(apts_data_dropped)
        apts = apts[["Lead_id", "f2f_sch", "f2f_comp","Service","Dept_x"]]
        apts.columns = ["Lead_id", "f2f_sch", "f2f_comp","Service","Dept"]

        f2f_sch = apts[["Lead_id","f2f_sch", "Service", "Dept"]]
        f2f_sch.columns = ["Total_f2f_sch","Date", "Service", "Dept"]
        f2f_sch_df = f2f_sch.groupby(["Date", "Dept","Service"]).count()
        f2f_sch_df = f2f_sch_df.reset_index()

        f2f_sch_df.Date = pd.to_datetime(f2f_sch_df.Date, format = "%Y-%m-%d")

        #f2f_sch_daily = pd.merge(dates_df, f2f_sch_df, on = "Date", how = "left")
        f2f_sch_daily = f2f_sch_df

        # Lets take daily Total f2f completed

        f2f_comp = apts[["Lead_id","f2f_comp", "Service", "Dept"]]
        f2f_comp.columns = ["Total_f2f_comp","Date", "Service", "Dept"]
        f2f_comp_df = f2f_comp.groupby(["Date", "Dept","Service"]).count()
        f2f_comp_df = f2f_comp_df.reset_index()

        f2f_comp_df = f2f_comp_df[f2f_comp_df.Date>="2019-01-01"]
        f2f_comp_df = f2f_comp_df.reset_index()
        f2f_comp_df = f2f_comp_df[["Date","Dept", "Service","Total_f2f_comp"]]

        f2f_comp_df = f2f_comp_df[f2f_comp_df.Date!="Service NA"]
        f2f_comp_df.Date = pd.to_datetime(f2f_comp_df.Date, format = "%Y-%m-%d")

        #f2f_comp_df = f2f_comp_df.replace(r"\\N",0,regex = True)
        f2f_comp_df.Date = pd.to_datetime(f2f_comp_df.Date)


        #f2f_comp_daily = pd.merge(dates_df, f2f_comp_df, on = "Date", how = "left")
        f2f_comp_daily = f2f_comp_df

        # Combining Total daily f2f_sch and f2f_comp

        Apts_df = pd.merge(f2f_sch_daily, f2f_comp_daily, on = ["Date", "Dept", "Service"], how = "outer")

        just = pd.merge(Spends_leads_data, Apts_df, on = ["Date", "Dept","Service"], how = "outer" )

        ### Lets Split Apts Scheduled and Completed into Facebook and Google

        FB_apts_df, Google_apts_df = FB_Google.channels(apts_data)

        ### Splitting Scheduled and Completed between Facebook and Google

        FB_apts_data_dropped = FB_apts_df[["Lead_id","f2f_sch_date", "f2f_comp_date","Service", "Dept_x"]]
        FB_apts_data_dropped.columns = ["Lead_id","f2f_sch","f2f_comp","Service", "Dept"]

        FB_apts = FB_apts_data_dropped
        #FB_apts = non_mri.MRI_filter(FB_apts_data_dropped)
        #FB_apts = FB_apts.drop(columns = ["Service","Dept_id","Service_id","Dept"])

        # Lets take daily total f2f_sch

        FB_f2f_sch = FB_apts[["Lead_id","f2f_sch", "Service", "Dept"]]
        FB_f2f_sch.columns = ["FB_f2f_sch","Date", "Service", "Dept"]
        FB_f2f_sch_df = FB_f2f_sch.groupby(["Date", "Dept", "Service"]).count()
        FB_f2f_sch_df = FB_f2f_sch_df.reset_index()

        FB_f2f_sch_df.Date = pd.to_datetime(FB_f2f_sch_df.Date, format = "%Y-%m-%d")

        FB_f2f_sch_daily = pd.merge(dates_df, FB_f2f_sch_df, on = "Date", how = "left")


        # Lets take daily Total f2f completed

        FB_f2f_comp = FB_apts[["Lead_id","f2f_comp","Service", "Dept"]]
        FB_f2f_comp.columns = ["FB_f2f_comp","Date","Service", "Dept"]
        FB_f2f_comp_df = FB_f2f_comp.groupby(["Date", "Dept", "Service"]).count()
        FB_f2f_comp_df = FB_f2f_comp_df.reset_index()

        FB_f2f_comp_df = FB_f2f_comp_df[FB_f2f_comp_df.Date>="2019-01-01"]
        FB_f2f_comp_df = FB_f2f_comp_df.reset_index()
        FB_f2f_comp_df = FB_f2f_comp_df[["Date", "Dept", "Service", "FB_f2f_comp"]]

        FB_f2f_comp_df = FB_f2f_comp_df.replace(r"\\N",0,regex = True)
        FB_f2f_comp_df.Date = pd.to_datetime(FB_f2f_comp_df.Date, errors ='coerce')

        FB_f2f_comp_daily = pd.merge(dates_df, FB_f2f_comp_df, on = "Date", how = "left")

        # Combining Total daily f2f_sch and f2f_comp

        FB_apts_daily = pd.merge(FB_f2f_sch_daily, FB_f2f_comp_daily, on = ["Date", "Dept", "Service"], how = "outer")



        Google_apts_data_dropped = Google_apts_df[["Lead_id","f2f_sch_date", "f2f_comp_date","Service", "Dept_x"]]
        Google_apts_data_dropped.columns = ["Lead_id","f2f_sch","f2f_comp","Service", "Dept"]

        #Google_apts = non_mri.MRI_filter(Google_apts_data_dropped)
        #Google_apts = Google_apts.drop(columns = ["Service","Dept_id","Service_id","Dept"])
        Google_apts = Google_apts_data_dropped
        # Lets take daily total f2f_sch

        Google_f2f_sch = Google_apts[["Lead_id","f2f_sch", "Service", "Dept"]]
        Google_f2f_sch.columns = ["Google_f2f_sch","Date", "Service", "Dept"]
        Google_f2f_sch_df = Google_f2f_sch.groupby(["Date", "Dept", "Service"]).count()
        Google_f2f_sch_df = Google_f2f_sch_df.reset_index()

        Google_f2f_sch_df.Date = pd.to_datetime(Google_f2f_sch_df.Date, format = "%Y-%m-%d")

        Google_f2f_sch_daily = pd.merge(dates_df, Google_f2f_sch_df, on = "Date", how = "left")


        # Lets take daily Total f2f completed

        Google_f2f_comp = Google_apts[["Lead_id","f2f_comp","Service", "Dept" ]]
        Google_f2f_comp.columns = ["Google_f2f_comp","Date", "Service", "Dept"]
        Google_f2f_comp_df = Google_f2f_comp.groupby(["Date", "Dept", "Service"]).count()
        Google_f2f_comp_df = Google_f2f_comp_df.reset_index()

        Google_f2f_comp_df = Google_f2f_comp_df[Google_f2f_comp_df.Date>="2019-01-01"]
        Google_f2f_comp_df = Google_f2f_comp_df.reset_index()
        Google_f2f_comp_df = Google_f2f_comp_df[["Date", "Dept", "Service", "Google_f2f_comp"]]

        Google_f2f_comp_df = Google_f2f_comp_df.replace(r"\\N",0,regex = True)
        Google_f2f_comp_df.Date = pd.to_datetime(Google_f2f_comp_df.Date, errors ='coerce')

        Google_f2f_comp_daily = pd.merge(dates_df, Google_f2f_comp_df, on = "Date", how = "left")

        # Combining Total daily f2f_sch and f2f_comp

        Google_apts_daily = pd.merge(Google_f2f_sch_daily, Google_f2f_comp_daily, on = ["Date", "Dept", "Service"], how = "outer")


        FB_Google_daily_apts = pd.merge(FB_apts_daily, Google_apts_daily, on = ["Date", "Dept", "Service"], how = "outer")

        Apts_daily = pd.merge(Apts_df, FB_Google_daily_apts, on = ["Date", "Dept", "Service"], how = "outer")

        Daily_spend_leads_apts = pd.merge(Spends_leads_data, Apts_daily, on = ["Date", "Dept", "Service"], how = "outer")


        ### Lets add more columns 

        Daily_spend_leads_apts = Daily_spend_leads_apts.replace(np.nan, 0)

        Daily_spend_leads_apts["Total_spend"] = Daily_spend_leads_apts.FB_spend + Daily_spend_leads_apts.Google_spend

        Daily_spend_leads_apts["Total_cpl"] = Daily_spend_leads_apts.Total_spend/Daily_spend_leads_apts.Total_leads

        Daily_spend_leads_apts["Total_cpa"] = Daily_spend_leads_apts.Total_spend/Daily_spend_leads_apts.Total_f2f_comp

        Daily_spend_leads_apts["FB_cpl"] = Daily_spend_leads_apts.FB_spend/Daily_spend_leads_apts.FB_leads
        Daily_spend_leads_apts["Google_cpl"] = Daily_spend_leads_apts.Google_spend/Daily_spend_leads_apts.Google_leads

        Daily_spend_leads_apts["FB_cpa"] = Daily_spend_leads_apts.FB_spend/Daily_spend_leads_apts.FB_f2f_comp
        Daily_spend_leads_apts["Google_cpa"] = Daily_spend_leads_apts.Google_spend/Daily_spend_leads_apts.Google_f2f_comp

        Daily_spend_leads_apts["Total_Comp/Sch"] = (Daily_spend_leads_apts.Total_f2f_comp/Daily_spend_leads_apts.Total_f2f_sch)*100
        Daily_spend_leads_apts["Total_Apt/Lead"] = (Daily_spend_leads_apts.Total_f2f_comp/Daily_spend_leads_apts.Total_leads)*100

        Daily_spend_leads_apts["FB_Comp/Sch"] = (Daily_spend_leads_apts.FB_f2f_comp/Daily_spend_leads_apts.FB_f2f_sch)*100
        Daily_spend_leads_apts["FB_Apt/Lead"] = (Daily_spend_leads_apts.FB_f2f_comp/Daily_spend_leads_apts.FB_leads)*100

        Daily_spend_leads_apts["Google_Comp/Sch"] = (Daily_spend_leads_apts.Google_f2f_comp/Daily_spend_leads_apts.Google_f2f_sch)*100
        Daily_spend_leads_apts["Google_Apt/Lead"] = (Daily_spend_leads_apts.Google_f2f_comp/Daily_spend_leads_apts.Google_leads)*100


        #Daily_spend_leads_apts["Google_spend/budget"] = (Daily_spend_leads_apts.Google_spend/Daily_spend_leads_apts.G_budget)*100

        Daily_spend_leads_apts["FB_leads_share"] = (Daily_spend_leads_apts.FB_leads/Daily_spend_leads_apts.Total_leads)*100
        Daily_spend_leads_apts["Google_leads_share"] = (Daily_spend_leads_apts.Google_leads/Daily_spend_leads_apts.Total_leads)*100

        Daily_spend_leads_apts["FB_apts_share"] = (Daily_spend_leads_apts.FB_f2f_comp/Daily_spend_leads_apts.Total_f2f_comp)*100
        Daily_spend_leads_apts["Google_apts_share"] = (Daily_spend_leads_apts.Google_f2f_comp/Daily_spend_leads_apts.Total_f2f_comp)*100


        cols = Daily_spend_leads_apts.columns.tolist()
        cols.sort()
        Daily_spend_leads_apts = Daily_spend_leads_apts[cols]



        return Daily_spend_leads_apts


# testing = summary_cities()

# print(summary_cities.summary(testing,"Bangalore").tail)