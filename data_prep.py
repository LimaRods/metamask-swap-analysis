import pandas as pd
from api_key import sdk_api_key
from shroomdk import ShroomDK
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
#Creating a class
class Query:

    def __init__(self, script, api_key = sdk_api_key, data = None):
        self.api_key = api_key
        self.script = script
        self.data = data

    def query_data(self, groupby = None, start_date = None, end_date = None):
        try:
            if groupby is None and start_date  is None:
                sdk = ShroomDK(self.api_key)
                sql = self.script

                query_result =  sdk.query(sql)
                self.data = pd.DataFrame.from_records(query_result.records)
                return self
            
            else : 
                sdk = ShroomDK(self.api_key)
                sql = self.script.format(groupby,start_date,end_date)
                query_result =  sdk.query(sql)
                self.data = pd.DataFrame.from_records(query_result.records)
                return self
        
        except BaseException as e:
            print(e)

    def load_gsheet(self,sheet_name, index_tab):
        try:
            # Connecting with Google Client
            scope = ['https://www.googleapis.com/auth/spreadsheets',
                    'https://www.googleapis.com/auth/drive']
            creds = Credentials.from_service_account_file("gpe_projects_key.json", scopes=scope)
            client = gspread.authorize(creds)  

            # Opening google sheet
            google_sh = client.open(sheet_name)
            sheet = google_sh.get_worksheet(index_tab)

            # write to dataframe
            sheet.clear()
            set_with_dataframe(worksheet=sheet, dataframe=self.data, include_index=False,
            include_column_header=True, resize=True)

            print(f'Loading data into {sheet_name} index {index_tab} sucessfully completed')

        except BaseException as e:
            print(e)

# Additional function to clean google sheets
def clean_sheet(sheet_name, tab_index):
    # Connecting with Google Client
    scope = ['https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file("gpe_projects_key.json", scopes=scope)
    client = gspread.authorize(creds)                                            

    # Opening google sheet
    google_sh = client.open(sheet_name)
    sheet = google_sh.get_worksheet(tab_index)
    sheet.clear()
   
    return print(f'Cleaning data from sheet {sheet_name} index {index_tab} sucessfully finished')