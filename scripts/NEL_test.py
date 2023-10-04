import os
import pandas as pd
import pymysql
import re
import string
import sys
import json
from tqdm import tqdm
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import csv
import spacy
from spacy.kb import KnowledgeBase
from spacy.kb import InMemoryLookupKB

nlp= spacy.load('/mnt/d/Documents/NERpipeline/models/combinednlp')
kb = InMemoryLookupKB(vocab=nlp.vocab, entity_vector_length=300)
kb.from_disk('/mnt/d/Documents/NERpipeline/models/combinedkb')
kb2 = InMemoryLookupKB(vocab=nlp.vocab, entity_vector_length=300)
kb2.from_disk('/mnt/d/Documents/NERpipeline/models/combinedkb')

def qidlookup(fname,qentity):
    #print(qentity[1:])
    with open(fname, 'r') as f:
        for line in f:
            qid = int(line.split('\t')[0])
            qid1,refs,cts = line.split('\t')
            if qid==int(qentity[1:]):
                return(json.loads(refs))
                break

# Function to take a csv KG file with head, reln, and tail as variables
# and convert it to a dictionary with all tails for a head as values
def load_qid(filename):
	qiddf = pd.read_csv(filename)
	qiddf = qiddf[qiddf['tail'].str.contains("Q")]
	qiddf['headnum'] = qiddf['head'].str[1:].astype('int')
	qiddf['tailnum'] = qiddf['tail'].str[1:].astype('int')
	qiddf1 = qiddf.groupby('headnum')['tailnum'].apply(list).reset_index(name='new')
	qiddict2= qiddf1.set_index('headnum')['new'].to_dict()
	return qiddict2

#Given a WikidataID, this functon returns its list of relations
def qidlookup2(fname,qentity):
    try:
        qid = int(qentity[1:])
        return(fname[qid])
    except:
        pass

def gettargs(ent, qdict):
	refs = {}
	targlist= {}
	candidates = kb2.get_alias_candidates(ent.text)
	onelist =set([c.entity_ for c in candidates])
	o = str([o for o in onelist])[1:-1]
	if len(set([c.entity_ for c in candidates]))==1:
		for c in candidates:
			if c.entity_  not in refs :
				qid = qidlookup2(qdict, o[1:-1])
				if qid is not None:
					refs[o[1:-1]]= [ent.text,qid]
	if len(set([c.entity_ for c in candidates]))>1:
		for c in candidates:
			if ent.text not in targlist:
				targlist[ent.text]= {}
			if c.entity_ not in targlist[ent.text]:
				qid = qidlookup2(qdict,c.entity_)
				if qid is not None:
					targlist[ent.text][c.entity_]=qid
	return refs,targlist


# This is the function to look at overlaps in a target list and reference list
# It needs to be cleaned up and checked
def entdisamb(targlist, refs):
    qidscore = {}
    if len(targlist)==0:
        print("No ambiguous entities to disambiguate")
        pass
    if (len(refs)>0):
        for a,b in refs.items():
            qidd = {}
            prim = {}
            for i,j in enumerate(targlist.items()):
                if j[0] not in qidd:
                    qidd[j[0]]= {}
                if j[0] not in prim:
                    prim[j[0]]= {}
                if j[0] not in qidscore:
                    qidscore[j[0]]={}
                for x, item in j[1].items():
                    if x not in qidd[j[0]]:
                         prim[j[0]][x] = 2 if (int(a[1:])==int(x[1:])) else 1 if ((int(a[1:]) in item) | (int(x[1:]) in b[1])) else 0
                    else:
                        p=2 if (int(a[1:])==int(x[1:])) else 1 if ((int(a[1:]) in item) | (int(x[1:]) in b[1])) else 0
                        prim[j[0]][x] = prim[j[0]][x]+p
                    intersection = [item1 for item1 in item if item1 in b[1]]
                    if x not in qidd[j[0]]:
                        qidd[j[0]][x]=(len(intersection)/len(item))
                        #qidd[j[0]][x]=(len(intersection))
                        #qidscore[j[0]][x]=(len(intersection)/len(item))+ prim[j[0]][x]
                    if x not in qidscore[j[0]]:
                        qidscore[j[0]][x]=(len(intersection)/len(item))+ prim[j[0]][x]
                        #print("Score: ",qidscore)
                    else:
                        qidd[j[0]][x]=qidd[j[0]][x]+(len(intersection)/len(item))
                        #qidd[j[0]][x]=qidd[j[0]][x]+(len(intersection))
                        qidscore[j[0]][x]=qidscore[j[0]][x] + qidd[j[0]][x]+ prim[j[0]][x]
    else:
        print("No references\nLooking at overlaps in targets")
    return qidscore

# This function will create an empty dictionary for the targlist entities to hold the scores
def create_qiddict(targlist):
    qscore = {}
    for a,b in targlist.items():
        qscore[a] = {}
        for k11 in b.keys():
            qscore[a][k11]=0
    return qscore	

# This function scores the direct overlaps (a Wikidata ID is in the relations of another Wikidata ID)
def get_direct_score(targlist, qidct ):
    for ent_a,inner_a in targlist.items():
        for ent_b, inner_b in targlist.items():
            if ent_a!=ent_b:
                for inner_keyA in inner_a.keys():
                    for inner_keyB,inner_valuesB in inner_b.items():
                        if int(inner_keyA[1:]) in inner_valuesB:
                            qidct[ent_b][inner_keyB] +=1
                            #qidct[ent_a][inner_keyA] +=1
    return qidct

# This function scores the direct overlaps (a Wikidata ID is in the relations of another Wikidata ID)
def get_indirect_score(targlist, qidct):
    for ent_a,inner_a in targlist.items():
        for inner_keyA, inner_valuesA in inner_a.items():
            for ent_b, inner_b in targlist.items():
                for inner_keyB,inner_valuesB in inner_b.items():
                    if ent_a!=ent_b:
                        lenset = len(set(inner_valuesA))+len(set(inner_valuesB))
                        qidct[ent_a][inner_keyA]+= ((len(set(inner_valuesA).intersection(set(inner_valuesB))))/(lenset/2))
    return qidct

# This functon takes the maximum dictionary entity from the qiddict
# It will return the name of the most likely candidate 
def get_max_score(qsdict):
	x = []
	for i,j in enumerate(qsdict.items()):
		print(j)
		x.append([j[0],max(qsdict[j[0]],key=qsdict[j[0]].get)])
		print(j[0],x)
	return x	


#qiddf = pd.read_csv('c:/Users/arpie/Dropbox/kbHL_reln.csv')
qiddict = load_qid('/mnt/c/Users/arpie/Dropbox/kbHL_reln.csv')
stdname=pd.read_csv('/mnt/d/Documents/NERpipeline/std_name.csv')
te = "Fayette County is in Georgia. So is Fulton County."
te2 = "This action was a victory for Pietro Nenni over those Socialist Party elements favoring closer cooperation with the Communist Party."
doc = nlp(str(te2))
refs = {}
targlist = {}
for ent in doc.ents:
    r, ta= gettargs(ent,qiddict)
    refs.update(r)
    targlist.update(ta)

qidct = create_qiddict(targlist)
dirscore = get_direct_score(targlist,qidct)
get_indirect_score(targlist, qidct)
get_max_score(dirscore)

qidscores = entdisamb(targlist, refs)
get_max_score(qidscores)

