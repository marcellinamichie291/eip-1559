from duneanalytics import DuneAnalytics
import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv
import os

# load env variables for dune
load_dotenv()

# initialize client
dune = DuneAnalytics(os.getenv('DUNE_USER'), os.getenv('DUNE_PASSWORD'))

# try to login
dune.login()

# fetch token
dune.fetch_auth_token()


##### BLOCKS #####
result_id = dune.query_result_id(query_id=1447663)
# fetch query result
data = dune.query_result(result_id)
data = [d['data'] for d in [{k: v for k, v in d.items() if k == 'data'} for d in data['data']['get_result_by_result_id']]]
df_blocks = pd.DataFrame(data)

print(df_blocks)

##### TRANSACTIONS #####
result_id = dune.query_result_id(query_id=1447664)
# fetch query result
data = dune.query_result(result_id)
data = [d['data'] for d in [{k: v for k, v in d.items() if k == 'data'} for d in data['data']['get_result_by_result_id']]]
df_tx = pd.DataFrame(data)

print(df_tx)

df_total=pd.merge(df_blocks,df_tx, how='left', left_on='number', right_on='block_number')
print(df_total)

df_total.to_csv('dataset_validation.csv')