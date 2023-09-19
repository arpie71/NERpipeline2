description = """Script to create KB from SQL DB"""
import argparse
from argparse import RawTextHelpFormatter

def cli():
    parser = argparse.ArgumentParser(description=description, formatter_class=RawTextHelpFormatter)
    parser.add_argument(
        "--sqldb",
        required=True,
        type=str,
        default='',
        help="Name of existing DB")
    parser.add_argument(
        "--entfiles",
		nargs = "*",
        required=True,
        type=str,
        default=[],
        help="Name of variables in existing entities file")
    parser.add_argument(
        "--aliasfiles",
		nargs = "*",
        required=False,
        type=str,
        default=[],
        help="Name of variables in existing aliases file")
    parser.add_argument(
        "--newdb",
        required=True,
        type=str,
        default='',
        help="Named of new DB")
    args = parser.parse_args()
    entfiles = args.entfiles
    aliasfiles = args.aliasfiles
    sqldb = args.sqldb
    newdb = args.newdb
    return (entfiles, aliasfiles, sqldb, newdb)


def main(entfiles, aliasfiles, sqldb, newdb):
    import pymysql
    import time
    import os
    import pandas as pd
    from pymysql import MySQLError
    from sqlalchemy import create_engine
    import re
    from sparql_queries import get_all_aliases, sqlengine, getDesc_from_ID
    from sqlalchemy_utils import database_exists, create_database
    from sqlalchemy import create_engine, text as sql_text
    dirname = os.getcwd()
    os.chdir(dirname)
    engine = sqlengine(sqldb)

    entities=pd.read_sql(sql_text('SELECT * from entities;'), con=engine.connect())
    aliases = pd.read_sql(sql_text('SELECT * from aliases;'), con=engine.connect())
    std_name = pd.read_sql(sql_text('SELECT * from std_name;'), con=engine.connect())

    print("The length of entities is", len(entities))
    print("The length of aliases is", len(aliases))
    print("The length of std_name is", len(std_name))
    for f in entfiles:
        try:
            newdf = pd.read_csv(f, encoding='utf8')
        except:
            newdf = pd.read_csv(f, encoding='latin1')
        
        entities = entities.merge(newdf.loc[:,['WikidataID','en_description']].drop_duplicates(), on = ['WikidataID','en_description'], how = 'outer')
        aliases = aliases.merge(newdf.loc[:,['WikidataID','itemLabel']].drop_duplicates(), how = 'outer')
        std_name = std_name.merge(newdf.loc[:,['WikidataID','itemLabel']].drop_duplicates(), how = 'outer')
    for af in aliasfiles:
        newdf_a = pd.read_csv(af, encoding='utf8')
        aliases = aliases.merge(newdf_a.loc[:,['WikidataID','itemLabel']].drop_duplicates(), how = 'outer')
        
    entities = entities.drop_duplicates(subset=['WikidataID'], keep='last')
    std_name = std_name.drop_duplicates(subset=['WikidataID'], keep='last')
    print(len(std_name))
    std_name = std_name[std_name.WikidataID.str.contains("Q")]
    print(len(std_name))
    t = std_name.WikidataID.values.tolist()
    s = ' '.join('wd:'+w+' ' for w in t)[:-1]
    instancesof = getDesc_from_ID(s)
    instancesdf = pd.DataFrame(instancesof, columns=['WikidataID', 'itemLabel','en_description','instancesof'])
    std_name = pd.merge(std_name,instancesdf[['WikidataID','instancesof']], on='WikidataID',how='outer')
    #entitiesdf = pd.concat([entitiesdf,pd.DataFrame(entity, columns=['WikidataID', 'itemLabel','en_description','instanceof'])], ignore_index=True)
    print("The new length of entities is", len(entities))
    print("The new length of aliases is", len(aliases))
    print("The new length of std_name is", len(std_name))
    print(aliases.loc[899])
    engine2 = sqlengine(newdb)
    entities.to_sql('entities',con=engine2,  if_exists='replace', chunksize=1000, index = False)
    std_name.to_sql('std_name',con=engine2, if_exists='replace', index = False)
    aliases.to_sql('aliases',con=engine2, if_exists='replace', chunksize=1000, index = False)

#ALTER TABLE `covidkb2`.`aliases` 
#CHARACTER SET = utf8mb4 , COLLATE = utf8mb4_unicode_520_ci ;

if __name__ == '__main__':
    entfiles, aliasfiles, sqldb, newdb = cli()
    main(entfiles, aliasfiles, sqldb, newdb)	
    
    
    