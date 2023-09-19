
description = """Script to create KB from SQL DB"""
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
        help="Name of new KnowledgeBase")
    parser.add_argument(
        "--sql",
        required=True,
        type=str,
        default='',
        help="Which SQL DB to use: local or HL")
    args = parser.parse_args()
    db = args.db
    ner = args.ner
    newkb = args.kb
    sql = args.sql
    return (db,ner, newkb, sql)

def add_to_kb(db, kb):
    import spacy
    nlp = spacy.load(ner)
    vocab = nlp.vocab
    for qid, desc in db:
        try:
            desc_doc = nlp(desc)
        except:
            desc_doc = nlp("No description")
        desc_enc = desc_doc.vector
        kb.add_entity(entity=qid, entity_vector=desc_enc, freq=342)  # 342 is an arbitrary value here

def alias_to_kb(db, kb):
    from collections import Counter, defaultdict
    d = db.itemLabel.values.tolist()
    l = Counter(d)
    l1 = Counter(el for el in l.elements() if l[el]==1)
    adb = db.loc[db.itemLabel.isin(l1),['WikidataID','itemLabel']].values.tolist()
    ents = db.loc[db.itemLabel.isin(l1),['itemLabel']].values.tolist()
    print(ents.count('ACBJ'))
    for i, a in adb:
        #try:
            kb.add_alias(alias=a, entities=[i], probabilities=[int(.99)])  # 100% prior probability P(entity|alias)
            #if a.upper not in ents:
            #    kb.add_alias(alias=a.upper(), entities=[i], probabilities=[1])  # 100% prior P(entity|alias)
        #except:
        #    print(i+ " not in knowledge base")
    l=Counter(el for el in l.elements() if l[el]>1)
    for a in l:
        tDF = db.loc[db.itemLabel==a,].copy()
        prob = [1/(len(tDF))]*(len(tDF))
        alist = tDF.WikidataID.values.tolist()
        #if a=="OAS":
        #    print(alist, ".51")
        #    prob = [.51,.49]
        try:
            kb.add_alias(alias=a, entities=alist, probabilities=prob)  # 100% prior 
        except:
            print(a," not found in KnowledgeBase")
        #if a.upper!=a:
        #    kb.add_alias(alias=a.upper(), entities=alist, probabilities=prob)  # 100% prior 

def main(db,ner, newkb, sql):
    import pymysql
    import time
    import os
    import pandas as pd
    from pymysql import MySQLError
    import sqlalchemy
    from sqlalchemy import create_engine, text as sql_text
    import re
    import spacy
    from spacy.kb import KnowledgeBase
    from spacy.kb import InMemoryLookupKB
    import configparser
    import json
    from collections import Counter, defaultdict
    from spacy.util import minibatch, compounding
    import psycopg2
    import sys
    dirname = os.getcwd()
    os.chdir(dirname)
    nlp = spacy.load(ner)
    vocab = nlp.vocab
    #kb = KnowledgeBase(vocab=nlp.vocab, entity_vector_length=300)
    kb = InMemoryLookupKB(vocab=nlp.vocab, entity_vector_length=300)
    config = configparser.ConfigParser()
    config.read('config.ini')
    if ((sql=="HL") | (sql=="local") | (sql=="covid"))  :
        sqlhost = config[sql]['host']
        sqluser = config[sql]['user']
        sqlpwd = config[sql]['pwd']
    else:
        print("Unknown SQL server")
        sys.exit()
    if db=='covid':
        conn = psycopg2.connect(host = sqlhost[1:-1] , user = sqluser[1:-1], password = sqlpwd[1:-1], dbname='postgres')
        query = """
        select email_id, file_id, body
        from covid19.emails where file_id!=1000
        ;"""

        df = pd.read_sql(query, conn)
        df = df.rename(columns={'email_id':'id'})
    else:
        if sql=="HL":
            engine = sqlalchemy.create_engine('mysql+pymysql://'+sqluser[1:-1]+':'+sqlpwd[1:-1]+'@'+sqlhost[1:-1]+'/declassification_'+db , encoding='utf8')
        else:
            #engine = sqlalchemy.create_engine('mysql+pymysql://'+sqluser[1:-1]+':'+sqlpwd[1:-1]+'@'+sqlhost[1:-1]+'/'+db , encoding='utf8')
            engine = sqlalchemy.create_engine('mysql+pymysql://'+sqluser[1:-1]+':'+sqlpwd[1:-1]+'@'+sqlhost[1:-1]+'/'+db)
    sqlhost = config['local']['host']
    sqluser = config['local']['user']
    sqlpwd = config['local']['pwd']    
    #engine = sqlalchemy.create_engine('mysql+pymysql://'+sqluser[1:-1]+':'+sqlpwd[1:-1]+'@'+sqlhost[1:-1]+'/'+db , encoding='utf8')
    engine = sqlalchemy.create_engine('mysql+pymysql://'+sqluser[1:-1]+':'+sqlpwd[1:-1]+'@'+sqlhost[1:-1]+'/'+db)
    entities=pd.read_sql(sql_text('SELECT * from entities;'), con=engine.connect())
    aliases = pd.read_sql(sql_text('SELECT * from aliases;'), con=engine.connect())
    add_to_kb(entities[['WikidataID', 'en_description']].values.tolist(),kb)
    print("Number of KB entries: ", len(kb))
    aliases = aliases.dropna(subset=['itemLabel'])
    alias_to_kb(aliases, kb)
    #print("Number of KB entries: ", len(kb))
    print("Number of KB aliases: ", kb.get_size_aliases())
    kb.to_disk(newkb)
    
    dataset = []
    nam = []
    json_loc = "NEL/frus_update_CD.json"
    #json_loc = "NEL/nam_entkb.json"
    import math
    with open(json_loc) as train_data:
        train = json.load(train_data)
        for example in train:
            text = example["content"]
            QID = example["wiki"]
            offset = (example["entities"][0][1], example["entities"][0][2])
            entity_label = example["entities"][0][3]
            entities = [(offset[0], offset[1], entity_label)]
            links_dict = {QID: 1.0}
            dataset.append((text, {"links": {offset: links_dict}, "entities": entities}))
    with open("NEL/nam_entkb.json") as train_data:
        train = json.load(train_data)
        for example in train:
            text = example["content"]
            QID = example["wiki"]
            offset = (example["entities"][0][1], example["entities"][0][2])
            entity_label = example["entities"][0][3]
            entities = [(offset[0], offset[1], entity_label)]
            links_dict = {QID: 1.0}
            dataset.append((text, {"links": {offset: links_dict}, "entities": entities}))
            nam.append((text, {"links": {offset: links_dict}, "entities": entities}))
    with open("NEL/entkb_frus.json") as train_data:
        train = json.load(train_data)
        for example in train:
            text = example["content"]
            QID = example["wiki"]
            offset = (example["entities"][0][1], example["entities"][0][2])
            entity_label = example["entities"][0][3]
            entities = [(offset[0], offset[1], entity_label)]
            links_dict = {QID: 1.0}
            dataset.append((text, {"links": {offset: links_dict}, "entities": entities}))
            nam.append((text, {"links": {offset: links_dict}, "entities": entities}))            
    gold_ids = []
    for text, annot in dataset:
        for span, links_dict in annot["links"].items():
            for link, value in links_dict.items():
                if value:
                    gold_ids.append(link)
    c = Counter(gold_ids)
    cd2 = Counter(el for el in c.elements() if c[el]>2)
    import random
    train_dataset = []
    test_dataset = []
    for QID in cd2:
        #print(QID)
        indices = [i for i, j in enumerate(gold_ids) if j == QID]
        t=math.floor(len(indices)*.9)
        train_dataset.extend(dataset[index] for index in indices[0:t]) # first 8 in training
        test_dataset.extend(dataset[index] for index in indices[t:]) # last 2 in test

    print(len(train_dataset))
    print(len(test_dataset))

    random.shuffle(train_dataset)
    random.shuffle(test_dataset)
    from spacy.training import Example
    TRAIN_EXAMPLES = []
    if "sentencizer" not in nlp.pipe_names:
        nlp.add_pipe("sentencizer")
    sentencizer = nlp.get_pipe("sentencizer")
    for text, annotation in train_dataset:
        example = Example.from_dict(nlp.make_doc(text), annotation)
        #print(example)
        example.reference = sentencizer(example.reference)
        TRAIN_EXAMPLES.append(example)
    from spacy.ml.models import load_kb
    if "entity_linker" not in nlp.pipe_names:
        entity_linker = nlp.add_pipe("entity_linker", config={"incl_prior": True, "n_sents":1}, last=True)
    entity_linker = nlp.get_pipe("entity_linker")
    entity_linker.initialize(get_examples=lambda: TRAIN_EXAMPLES, kb_loader=load_kb(newkb))
    optimizer = entity_linker.create_optimizer()


    with nlp.select_pipes(enable=["entity_linker"]):   # train only the entity_linker
        optimizer = nlp.resume_training()
        for itn in range(50):   # 500 iterations takes about a minute to train
            random.shuffle(TRAIN_EXAMPLES)
            batches = minibatch(TRAIN_EXAMPLES, size=compounding(4.0, 32.0, 1.001))  # increasing batch sizes
            losses = {}
            for batch in batches:
                nlp.update(
                    batch,   
                    drop=0.2,      # prevent overfitting
                    losses=losses,
                    sgd=optimizer,
                )
            if itn % 5 == 0:
                print(itn, "Losses", losses)   # print the training loss

    print(itn, "Losses", losses)
    
    if not os.path.exists(newkb):
         os.mkdir(newkb)
    kb.to_disk(newkb)
    nlp.to_disk('models/ner_lg')

if __name__ == '__main__':
    db,ner,newkb, sql = cli()
    main(db,ner,newkb,sql)