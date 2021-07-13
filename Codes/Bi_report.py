import pandas as pd
import datetime
import numpy as np

import sys
sys.path.insert(0,"D:\Python\Test\Codes\My_modules")

import FB_Google
import non_mri
from Summary_dept_map import summary_cities
import FB_adset_type_mapper

def bi_report():

    test = summary_cities()

    Bang = summary_cities.summary(test, "Bangalore")
    Chen = summary_cities.summary(test, "Chennai")
    Hyd = summary_cities.summary(test, "Hyderabad")
    Pune = summary_cities.summary(test, "Pune")
    Others = summary_cities.summary(test, "Others")
    No_data= summary_cities.summary(test, "No_data")

    dfs = [Bang, Chen, Hyd, Pune, Others, No_data]
    df = pd.concat(dfs, keys = ["Bangalore", "Chennai", "Hyderabad", "Pune", "Others", "No_data"])


    df = df.replace(np.inf, 0)
    df.to_csv(r"D:\Python\Test\PowerBi\report.csv")

    test2 = FB_adset_type_mapper.FB_adset_type_mapper()

    return 0

report = bi_report()