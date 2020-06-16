import requests
import pandas as pd

import config as cfg



class ServerConnection:
    def __init__(self, url, authorization_token):
        self.url = url
        self.authorization_token = authorization_token
        self.headers = {'Authorization': 'Token ' + self.authorization_token}

    def post(self, sub_path, data):
       return requests.post(self.url + sub_path, data=data, headers=self.headers)

    def get(self, sub_path):
       return requests.get(self.url + sub_path, headers=self.headers)


old_df = pd.read_pickle('raw_data.df')
print("old shape: " + str(old_df.shape))
timezone = 'Europe/Zurich'
df = pd.read_json(cfg.url + 'measurements/', orient='records')
print("new shape: " + str(df.shape))
df['when'] = pd.to_datetime(df['when'])
df.when.dt.tz_convert(tz=timezone)
df = df.sort_values(by='when')

combined = pd.concat([old_df,df])
combined.drop_duplicates(inplace=True)
combined = combined.sort_values(by='when')
print("combined shape: " + str(combined.shape))
combined.to_pickle('raw_data.df')


connection = ServerConnection(cfg.url,cfg.authorization_token)
response = connection.get('clean/')
print(response.content)
