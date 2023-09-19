import pymysql
import pandas as pd
from pymysql import MySQLError
import sqlalchemy
from sqlalchemy import create_engine
import re
import os
from SPARQLWrapper import SPARQLWrapper, JSON
from tqdm import tqdm
import time
from sparql_queries import getDesc_from_ID, get_all_aliases, getDesc_from_ID_old
from sparql_queries import parsewiki, get_results, sqlengine
from sqlalchemy_utils import database_exists, create_database

endpoint_url = "https://query.wikidata.org/sparql"

dirname = os.getcwd()
os.chdir(dirname)

arch = pd.read_excel('../kbbase/archigos_allwiki.xlsx',engine='openpyxl')
arch = arch.dropna(subset=['WikidataID'])
arch=arch.reset_index(drop=True)
arch.rename(columns={'leader':'itemLabel'}, inplace = True)
arch = arch[['WikidataID','itemLabel']]

sen = pd.read_csv('../kbbase/wiki_senate.csv', encoding='latin1')
sen[['url','item_id']]  = sen['item'].str.split('Q', expand=True)
sen['WikidataID'] = "Q"+sen['item_id'].astype(str)
sen = sen[['WikidataID','itemLabel']]

house = pd.read_csv('../kbbase/wiki_house.csv', encoding='latin1')
house[['url','item_id']]  = house['item'].str.split('Q', expand=True)
house['WikidataID'] = "Q"+house['item_id'].astype(str)

house = house[['WikidataID','itemLabel']]

fm = pd.read_csv('../kbbase/formin_done.csv',encoding='utf-8')
fm = fm.dropna(subset=['WikidataID'])
fm = fm.reset_index(drop=True)
fm.rename(columns={'formin1':'itemLabel'}, inplace = True)
fm = fm[['WikidataID','itemLabel']]

dpi1 = pd.concat([arch, fm, sen, house], ignore_index=True)
dpi1 = dpi1.drop_duplicates()

entitiesdf = pd.DataFrame(columns=['WikidataID','itemLabel', 'en_description','instanceof'])
aliasesdf = pd.DataFrame(columns=['WikidataID','itemLabel'])
dpi1['en_description'] = None
t = dpi1.loc[dpi1.en_description.isna(),].WikidataID.values.tolist()
s = ' '.join('wd:'+w+' ' for w in t)[:-1]
entity = getDesc_from_ID(s)
entitiesdf = pd.concat([entitiesdf,pd.DataFrame(entity, columns=['WikidataID', 'itemLabel','en_description','instanceof'])], ignore_index=True)
time.sleep(3)
alias = get_all_aliases(s)
aliasdf = pd.DataFrame(alias, columns=['WikidataID','itemLabel','sitelinks'])
aliasesdf = aliasdf.merge(entitiesdf.loc[:,['WikidataID','itemLabel']], on = ['WikidataID','itemLabel'], how = 'outer')
print("The number of entities is "+ str(len(entitiesdf)))
print("The number of aliases is "+ str(len(aliasesdf)))
db = 'dpikb_new'
engine = sqlengine(db)
entitiesdf.to_csv('../kbbase/entities.csv', encoding='utf-8')
aliasesdf.to_csv('../kbbase/aliases.csv', encoding='utf-8')
entitiesdf.loc[:,['WikidataID','en_description']].to_sql('entities',con=engine,  if_exists='replace', chunksize=1000,dtype={'WikidataID': sqlalchemy.types.TEXT(length=255),'en_description': sqlalchemy.types.TEXT(length=255000)})
entitiesdf.loc[:,['WikidataID','itemLabel','instanceof']].to_sql('std_name',con=engine, if_exists='replace',dtype={'WikidataID': sqlalchemy.types.TEXT(length=255),'itemLabel': sqlalchemy.types.TEXT(length=2550),'instanceof': sqlalchemy.types.TEXT(length=255)})
aliasesdf.to_sql('aliases',con=engine, if_exists='replace',dtype={'WikidataID': sqlalchemy.types.TEXT(length=255),'itemLabel': sqlalchemy.types.TEXT(length=2550, collation = 'utf8mb4_unicode_520_ci')})
