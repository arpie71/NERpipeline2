# python3 kbfile_format2.py --entfile covid_items.xlsx --entvars item_id  en_label en_desc  --entnf kbfiles/covid_ent.csv --aliasnf kbfiles/covid_alias.csv --aliasfile covid_aliases.xlsx --aliasvars item_id en_alias
description = """Script to create KB from SQL DB"""
import argparse
from argparse import RawTextHelpFormatter

def cli():
    parser = argparse.ArgumentParser(description=description, formatter_class=RawTextHelpFormatter)
    parser.add_argument(
        "--entfile",
        required=True,
        type=str,
        default='',
        help="Name of existing entities file")
    parser.add_argument(
        "--aliasfile",
        required=False,
        type=str,
        default='',
        help="Name of existing aliases file (if any)")
    parser.add_argument(
        "--entvars",
		nargs = "*",
        required=True,
        type=str,
        default=[],
        help="Name of variables in existing entities file")
    parser.add_argument(
        "--aliasvars",
		nargs = "*",
        required=False,
        type=str,
        default=[],
        help="Name of variables in existing aliases file")
    parser.add_argument(
        "--entnf",
        required=True,
        type=str,
        default='',
        help="New entities path and file")
    parser.add_argument(
        "--aliasnf",
        required=True,
        type=str,
        default='',
        help="New aliases path and file")
    args = parser.parse_args()
    entfile = args.entfile
    aliasfile = args.aliasfile
    entvars = args.entvars
    aliasvars = args.aliasvars
    entnf = args.entnf
    aliasnf = args.aliasnf
    return (entfile, aliasfile, entvars, aliasvars, entnf, aliasnf)

def main(entfile, aliasfile, entvars, aliasvars, entnf, aliasnf):
    import os, sys
    import re
    from SPARQLWrapper import SPARQLWrapper, JSON
    import pandas as pd
    import time
    from sparql_queries import getDesc_from_ID, get_all_aliases
    import pathlib
    dirname = os.getcwd()

    os.chdir(dirname)
    file_extension = pathlib.Path(entfile).suffix
    if file_extension==".xlsx":
        df = pd.read_excel(entfile, engine='openpyxl')
    else:
        df = pd.read_csv(entfile, encoding='utf-8')
    if entvars[0] != 'WikidataID':
        df.rename(columns={entvars[0]:'WikidataID'}, inplace = True)
    if aliasfile!='':
        file_ext_a = pathlib.Path(aliasfile).suffix
        if file_ext_a==".xlsx":
            df_a = pd.read_excel(aliasfile, engine='openpyxl')
        else:
            df_a = pd.read_csv(aliasfile, encoding='utf-8')
        if len(aliasvars)!=2:
            sys.exit("Two variables required for aliasvars")
        if aliasvars[1] != 'itemLabel' :
            df_a.rename(columns={aliasvars[1]:'itemLabel'}, inplace = True)
        if aliasvars[0] != 'WikidataID':
            df_a.rename(columns={aliasvars[0]:'WikidataID'}, inplace = True)
    print(len(df))
    if ((entvars[1] != 'itemLabel') & (entvars[1]!='')):
        df2.rename(columns={entvars[1]:'itemLabel'}, inplace = True)
    df2 = df.drop_duplicates(subset=['WikidataID'])
    df2['en_description'] = None
    if ((entvars[2]=='') | (len(entvars)<3)):
        df2['en_description'] = None
    elif entvars[2] != 'en_description':
        df2.rename(columns={entvars[2]:'en_description'}, inplace = True)

    t = df2.loc[df2.en_description.isna(),].WikidataID.values.tolist()
    s = ' '.join('wd:'+t1+' ' for t1 in t if t1[0]=="Q")[:-1]
    print(len(t))
    print(len(df))
    wid = getDesc_from_ID(s)
    entities = pd.concat([df2.dropna(subset=['en_description']),pd.DataFrame(wid,columns=['WikidataID', 'itemLabel','en_description','instanceof'])], ignore_index=True)

    print(len(entities))
    if ((entvars[1] =='') | (aliasfile=='')):
        t = entities.WikidataID.values.tolist()
        s = ' '.join('wd:'+t1+' ' for t1 in t if t1[0]=="Q")[:-1]
        alias = get_all_aliases(s)
        aliasdf = pd.DataFrame(alias, columns=['WikidataID','itemLabel','sitelinks'])
        aliases = aliasdf.merge(entities.loc[:,['WikidataID','itemLabel']], on = ['WikidataID','itemLabel'], how = 'outer')
        aliases = aliases.merge(df.loc[:,['WikidataID','itemLabel']], on = ['WikidataID','itemLabel'], how = 'outer')
    else:
        aliases = entities.loc[:,['WikidataID','itemLabel']]
        aliases = df_a.merge(df.loc[:,['WikidataID','itemLabel']], on = ['WikidataID','itemLabel'], how = 'outer')

    entities.to_csv(entnf, index = False)
    aliases.to_csv(aliasnf, index = False)

if __name__ == '__main__':
    entfile, aliasfile, entvars, aliasvars, entnf, aliasnf = cli()
    main(entfile, aliasfile, entvars, aliasvars, entnf, aliasnf)