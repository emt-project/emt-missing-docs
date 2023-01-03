import os
import pandas as pd
from tqdm import tqdm
from transkribus_utils.transkribus_utils import ACDHTranskribusUtils

user = os.environ.get('TR_USER')
pw = os.environ.get('TR_PW')
os.makedirs('./mets', exist_ok=True)

transkribus_client = ACDHTranskribusUtils(
    user=user,
    password=pw,
    transkribus_base_url="https://transkribus.eu/TrpServer/rest"
)

df = pd.read_csv("./col_ids.csv")
for i, row in tqdm(df.iterrows(), total=len(df)):
    col_id = row["coll_id"]
    mpr_docs = transkribus_client.collection_to_mets(col_id, file_path='./mets')
