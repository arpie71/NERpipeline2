
description = """Script to aggregate NER results"""
import argparse
from argparse import RawTextHelpFormatter

def cli():
    parser = argparse.ArgumentParser(description=description, formatter_class=RawTextHelpFormatter)
    parser.add_argument(
        "--db",
        required=True,
        type=str,
        default='',
        help="Name of db to aggregate")
    parser.add_argument(
        "--newfile",
        required=True,
        type=str,
        default='',
        help="Name of new file")
    parser.add_argument(
        "--stdname",
        required=False,
        type=str,
        default=None,
        help="Name of KB")
    parser.add_argument(
        "--filetype",
        required=True,
        type=str,
        default=None,
        help="File type for NER results (json or csv)")		
    args = parser.parse_args()
    db = args.db
    newfile = args.newfile
    stdname = args.stdname
    filetype = args.filetype
    return (db, newfile, stdname,filetype)

def main(db,newfile, stdname, filetype):
    import pandas as pd
    import json 
    import os
    import configparser
    import sqlalchemy
    from sqlalchemy import create_engine, text as sql_text
    from tqdm import tqdm

    #os.chdir(db)
    files = os.listdir(db)
    nerout = []
    if filetype=='json':
        with tqdm(total=len(files)) as pbar:
            for f in files:
                pbar.update(1)
                with open(db+'/'+f, 'r', encoding='utf8') as nf:
                    data = json.load(nf)
                    nerout.extend([[item['entity_name'], item['span_start'], item['span_end'], item['entity_type'],item['wikipedia'],f] for item in data])
    elif filetype=='csv':
        with tqdm(total=len(files)) as pbar:
            for f in files:
                pbar.update(1)
                with open(os.path.join(db, f), 'r', encoding='utf8') as nf:
                    rows = nf.readlines()
                    nerout.extend([row.strip().split('\t') + [f] for row in rows])
    else:
        print("File type not supported")
        sys.exit
    df = pd.DataFrame(nerout,columns=['entity','estart','eend','enttype','wikipedia','file'])
    sql = "local"
    config = configparser.ConfigParser()
    config.read('config.ini')
    if ((sql=="HL") | (sql=="local") | (sql=="covid"))  :
        sqlhost = config[sql]['host']
        sqluser = config[sql]['user']
        sqlpwd = config[sql]['pwd']
    else:
        print("Unknown SQL server")
        sys.exit()
    engine = sqlalchemy.create_engine('mysql+pymysql://'+sqluser[1:-1]+':'+sqlpwd[1:-1]+'@'+sqlhost[1:-1]+'/'+stdname )
    query = 'SELECT * from std_name'
    std_name = pd.read_sql(sql_text(query), con=engine.connect())
    std_name=std_name.rename(columns={"WikidataID":"wikipedia"})
    df = df.merge(std_name[['wikipedia','itemLabel']],on='wikipedia', how='left')
    df.to_csv(newfile)


if __name__ == '__main__':
    db,newfile, stdname, filetype = cli()
    main(db,newfile, stdname, filetype)
