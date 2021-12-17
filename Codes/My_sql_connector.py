

''' Note: 

This is a Function to return the MySql Connector '''


import os
import mysql.connector

def sql_connection():
       connection = mysql.connector.connect(
              user= os.environ.get("db_user"),
              host = os.environ.get("db_host"),
              password = os.environ.get("db_pw")
              )
       return connection