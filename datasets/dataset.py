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
result_id = dune.query_result_id(query_id=1316043)
# fetch query result
data = dune.query_result(result_id)
data = [d['data'] for d in [{k: v for k, v in d.items() if k == 'data'} for d in data['data']['get_result_by_result_id']]]
df_blocks = pd.DataFrame(data)

result_id = dune.query_result_id(query_id=1316465)
# fetch query result
data = dune.query_result(result_id)
data = [d['data'] for d in [{k: v for k, v in d.items() if k == 'data'} for d in data['data']['get_result_by_result_id']]]
df_blocks2 = pd.DataFrame(data)

result_id = dune.query_result_id(query_id=1317337)
# fetch query result
data = dune.query_result(result_id)
data = [d['data'] for d in [{k: v for k, v in d.items() if k == 'data'} for d in data['data']['get_result_by_result_id']]]
df_blocks3 = pd.DataFrame(data)

result_id = dune.query_result_id(query_id=1317362)
# fetch query result
data = dune.query_result(result_id)
data = [d['data'] for d in [{k: v for k, v in d.items() if k == 'data'} for d in data['data']['get_result_by_result_id']]]
df_blocks4 = pd.DataFrame(data)

result_id = dune.query_result_id(query_id=1317364)
# fetch query result
data = dune.query_result(result_id)
data = [d['data'] for d in [{k: v for k, v in d.items() if k == 'data'} for d in data['data']['get_result_by_result_id']]]
df_blocks5 = pd.DataFrame(data)

df_blocks = df_blocks.append(df_blocks2, ignore_index=True)
df_blocks = df_blocks.append(df_blocks3, ignore_index=True)
df_blocks = df_blocks.append(df_blocks4, ignore_index=True)
df_blocks = df_blocks.append(df_blocks5, ignore_index=True)

print(df_blocks)

##### TRANSACTIONS #####
result_id = dune.query_result_id(query_id=1316443)
# fetch query result
data = dune.query_result(result_id)
data = [d['data'] for d in [{k: v for k, v in d.items() if k == 'data'} for d in data['data']['get_result_by_result_id']]]
df_tx = pd.DataFrame(data)

result_id = dune.query_result_id(query_id=1488114)
# fetch query result
data = dune.query_result(result_id)
data = [d['data'] for d in [{k: v for k, v in d.items() if k == 'data'} for d in data['data']['get_result_by_result_id']]]
df_tx2 = pd.DataFrame(data)

result_id = dune.query_result_id(query_id=1317334)
# fetch query result
data = dune.query_result(result_id)
data = [d['data'] for d in [{k: v for k, v in d.items() if k == 'data'} for d in data['data']['get_result_by_result_id']]]
df_tx3 = pd.DataFrame(data)

result_id = dune.query_result_id(query_id=1316044)
# fetch query result
data = dune.query_result(result_id)
data = [d['data'] for d in [{k: v for k, v in d.items() if k == 'data'} for d in data['data']['get_result_by_result_id']]]
df_tx4 = pd.DataFrame(data)

result_id = dune.query_result_id(query_id=1488136)
# fetch query result
data = dune.query_result(result_id)
data = [d['data'] for d in [{k: v for k, v in d.items() if k == 'data'} for d in data['data']['get_result_by_result_id']]]
df_tx5 = pd.DataFrame(data)

result_id = dune.query_result_id(query_id=1316045)
# fetch query result
data = dune.query_result(result_id)
data = [d['data'] for d in [{k: v for k, v in d.items() if k == 'data'} for d in data['data']['get_result_by_result_id']]]
df_tx6 = pd.DataFrame(data)

result_id = dune.query_result_id(query_id=1488154)
# fetch query result
data = dune.query_result(result_id)
data = [d['data'] for d in [{k: v for k, v in d.items() if k == 'data'} for d in data['data']['get_result_by_result_id']]]
df_tx7 = pd.DataFrame(data)

result_id = dune.query_result_id(query_id=1316456)
# fetch query result
data = dune.query_result(result_id)
data = [d['data'] for d in [{k: v for k, v in d.items() if k == 'data'} for d in data['data']['get_result_by_result_id']]]
df_tx8 = pd.DataFrame(data)

df_tx = df_tx.append(df_tx2, ignore_index=True)
df_tx = df_tx.append(df_tx3, ignore_index=True)
df_tx = df_tx.append(df_tx4, ignore_index=True)
df_tx = df_tx.append(df_tx5, ignore_index=True)
df_tx = df_tx.append(df_tx6, ignore_index=True)
df_tx = df_tx.append(df_tx7, ignore_index=True)
df_tx = df_tx.append(df_tx8, ignore_index=True)



print(df_tx)

df_total=pd.merge(df_blocks,df_tx, how='left', left_on='number', right_on='block_number')
print(df_total)

df_total.to_csv('dataset.csv')