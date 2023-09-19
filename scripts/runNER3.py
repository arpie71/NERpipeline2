from spacy.kb import KnowledgeBase
from spacy.kb import InMemoryLookupKB
import spacy
import os
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import json
import pandas as pd
import sqlalchemy
import pymysql
import sys
import psycopg2
import configparser	
import pymysql.cursors
import random

description = """Script to run NER"""
import argparse
from argparse import RawTextHelpFormatter

def cli():
    parser = argparse.ArgumentParser(description=description, formatter_class=RawTextHelpFormatter)
    parser.add_argument(
        "--db",
        required=True,
        type=str,
        default='',
        help="Name of database")
    parser.add_argument(
        "--ner",
        required=True,
        type=str,
        default='',
        help="Entity linker to use (vanilla or saved)")
    parser.add_argument(
        "--kb",
        required=True,
        type=str,
        default='',
        help="Name of KnowledgeBase")
    parser.add_argument(
        "--sql",
        required=True,
        type=str,
        default='',
        help="Which SQL DB to use: local or HL")
    parser.add_argument(
        "--outdir",
        required=True,
        type=str,
        default='',
        help="NER output directory")
    parser.add_argument(
        "--sample",
        required=False,
        type=int,
        help="Number of documents to run NER model on")
    args = parser.parse_args()
    db = args.db
    ner = args.ner
    newkb = args.kb
    sql = args.sql
    outdir = args.outdir
    n = args.sample
    return (db,ner, newkb, sql, outdir,n)


def wikisearch(entity):
    import requests
    import json
    query = 'https://en.wikipedia.org/w/api.php?action=query&prop=pageprops&ppprop=wikibase_item&redirects=1&format=json&titles={0}'.format(entity)
    r = requests.get(query)
    j = json.loads(r.content)
    try: 
        j2 = j['query']['pages']
        for a in j2:
            if a=="-1":
                wiki='NIL'
            else:
                wiki = j2[a]['pageprops']['wikibase_item']
    except:
        wiki='NIL'
    return wiki

def run_ner_wiki(doc, nlp, perslist,fid):
	exclude_list = ('ADDRESS', 'CARDINAL', 'DATE', 'MONEY', 'ORDINAL', 'PERCENT', 'QUANTITY', 'TIME')
	s = nlp(doc)
	nerlist = []
	nowiki=[]
	for ent in s.ents:
		if ent.label_ not in exclude_list:
			entdict = {
                'file':fid,
				'entity_type': ent.label_,
				'entity_name': ent.text,
				'span_start': ent.start_char,
				'span_end': ent.end_char
			}
			if ent.label_ == "PERSON" and ent.kb_id_ == "NIL" and ent.text not in perslist:
				wiki = wikisearch(ent.text)
				#print("Searching wikipedia API:", ent.text, " ",wiki)
				perslist[ent.text]= wiki
				entdict['wikipedia'] = wiki
			else:
				entdict['wikipedia'] = ent.kb_id_
			if entdict['wikipedia']=="NIL" and entdict['entity_type']=="PERSON":
				nowiki.append(ent.text)
			nerlist.append(entdict)
	return nerlist, perslist, nowiki

def run_ner(doc, nlp,fid):
    exclude_list=('ADDRESS','CARDINAL','DATE','MONEY','ORDINAL','PERCENT','QUANTITY','TIME')
    s = nlp(doc)
    nerlist = []
    for ent in s.ents: 
        if ent.label_ not in exclude_list:
            entdict = {
            'file':fid,
            'entity_type': ent.label_,
            'entity_name': ent.text,
            'span_start': ent.start_char,
            'span_end': ent.end_char,
            'wikipedia': ent.kb_id_
            }
            nerlist.append(entdict)
    return nerlist    

def get_sql(conn, query):
	cursor = conn.cursor()
	cursor.execute(query)
	docs = cursor.fetchall()
	cursor.close() 
	return docs

def load_sql(sql,db):
    config = configparser.ConfigParser()
    config.read('config.ini')
    if sql in ["HL", "local", "covid"]:
        sqlhost = config[sql]['host']
        sqluser = config[sql]['user']
        sqlpwd = config[sql]['pwd']
    else:
        print("Unknown SQL server")
        sys.exit()
    # Load data
    if db in ['covid', 'un', 'nato']:
        conn = psycopg2.connect(host = sqlhost[1:-1] , user = sqluser[1:-1], password = sqlpwd[1:-1], dbname='postgres')
        if db=='covid':
            query = """
            SELECT body, doc_id, pg from covid19_muckrock.docpages
            ;"""
        elif db =='un':
            query = """
            select doc_id, body
            from foiarchive.un_archives_docs
            ;"""
        elif db =='nato':
            query = """
            select doc_id, body
            from foiarchive.nato_archives_docs
            ;"""
    else:
        if db == "frus" or db=="frus_update":
            query = 'select id, raw_body from docs;'
        elif db=="cibs":
            query = 'select doc_no, body from docs;'
        else:
            query = 'select id, body from docs;'
        conn=pymysql.connect(host=sqlhost[1:-1], user=sqluser[1:-1], password=sqlpwd[1:-1], db='declassification_'+db)
    docs = get_sql(conn,query)
    conn.close()
    return docs
def main(db,ner, newkb, sql, outdir,n):
    nlp = spacy.load(ner)
    kb = InMemoryLookupKB(vocab=nlp.vocab, entity_vector_length=300)
    kb.from_disk(newkb)
    df = load_sql(sql,db)
    print("DONE with SQL")
    print(n)
    # NER/NEL
    if n is not None:
        tt = random.sample(df,n)
    else:
        tt = df

    home = outdir
    if not os.path.exists(home):
        os.mkdir(home)

    perslist = {}
    enttype = ['GPE','NORP','ORG','PERSON','LOC','FAC','MED','EMAIL','TITLE','URL']
    with tqdm(total=len(tt)) as pbar:
        with ThreadPoolExecutor(max_workers=50):
            for body, did, pg in tt:
                t = str(did)+'_'+str(pg)
                fileloc=home+'/'+t+'.json'
                if os.path.isfile(fileloc):
                    continue
                body = body.replace(r"\n"," ")
                if len(body)<1000000:
                    #nl, pl, nw = run_ner_wiki(str(body),nlp, perslist, t)
                    nl = run_ner(str(body), nlp,t)
                    with open(fileloc, 'w') as outfile:
                        json.dump(nl, outfile)
                pbar.update(1)		
				
if __name__ == '__main__':
    db,ner,newkb, sql, outdir, n = cli()
    main(db,ner,newkb,sql, outdir,n )    				