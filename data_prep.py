import pandas as pd
from api_key import sdk_api_key
from shroomdk import ShroomDK

#Creating a class
class Query:

    def __init__(self, script, api_key = sdk_api_key, data = None):
        self.api_key = api_key
        self.script = script
        self.data = data

    def query_data(self, groupby = None, start_date = None):
        try:
            if groupby is None and start_date  is None:
                sdk = ShroomDK(self.api_key)
                sql = self.script
                
                query_result =  sdk.query(sql)
                self.data = pd.DataFrame.from_records(query_result.records)
                return self
            
            else : 
                sdk = ShroomDK(self.api_key)
                sql = self.script.format(groupby,start_date)
                query_result =  sdk.query(sql)
                self.data = pd.DataFrame.from_records(query_result.records)
                return self
        
        except BaseException as e:
            print(e)
