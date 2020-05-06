import requests
import pandas as pd

url = 'https://aqueous-depths-78223.herokuapp.com/measurements/'

df = pd.read_json(url, orient='records')
df.to_pickle('raw_data.df')