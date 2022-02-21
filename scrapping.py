import pandas as pd
import logging 
import re
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)

# EXTRACTING DATA

url = "https://id.wikipedia.org/wiki/Daftar_miliarder_Forbes"

def scrape(url):
    logging.info(f"Scrapping website with url: '{url}' ...")
    return pd.read_html(url, header=None)

dfs = scrape(url)[1]

print(dfs)

# TRANSFORMING DATA
def is_money_miliar(string_money):
    return string_money.lower().endswith("miliar")

def transform_money_format(string_money):
    half_clean_string = string_money.lower().replace(",", ".").replace(" ", "").replace("$","")
    return re.sub(r"[?\[M\]miliar]", "", half_clean_string)

def transform(df, tahun, perusahaan):
    logging.info("Transforming DataFrame ...")

    columns_mapping = {
        "Nama": "nama",
        "No.": "nomor_urut",
        "Kekayaan bersih (USD)": "kekayaan_bersih_usd",
        "Usia": "usia",
        "Kebangsaan":"kebangsaan",
        "Sumber kekayaan":"sumber_kekayaan"
    }

    renamed_df = df.rename(columns=columns_mapping)

    renamed_df["tahun"] = tahun
    renamed_df["perusahaan"] = renamed_df["sumber_kekayaan"]
    x = len(renamed_df.index)
    renamed_df["nomor_urut"] = list(range(1,x+1))
    
    renamed_df["kekayaan_bersih_usd_juta"] = renamed_df["kekayaan_bersih_usd"].apply(
        lambda value: float(transform_money_format(value)) * 1000 if is_money_miliar(value) else float(transform_money_format(value))
    )

    return renamed_df[["nama", "nomor_urut", "kekayaan_bersih_usd_juta", "perusahaan", "usia", "kebangsaan", "sumber_kekayaan","tahun"]]

df_2020 = transform(dfs, 2021, 'tes')

print(df_2020)

# LOADING DATA TO DATABASE
DB_NAME = "postgres"
DB_USER = "user1"
DB_PASSWORD = "user1"
DB_HOST = "104.197.148.144"
DB_PORT = "5432"
CONNECTION_STRING = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
TABLE_NAME = "muhammad_bachtiar_amin_orang_terkaya_forbes"

def write_to_postgres(df, db_name, table_name, connection_string):
    engine = create_engine(connection_string)
    logging.info(f"Writing dataframe to database: '{db_name}', table: '{table_name}' ...")
    df.to_sql(name = table_name, con=engine, if_exists="replace", index=False)

write_to_postgres(df=df_2020, db_name=DB_NAME, table_name=TABLE_NAME, connection_string=CONNECTION_STRING)