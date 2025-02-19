# NERpipeline2

This document describes the steps needed to run the NER/NEL pipeline.

## Description

This repository has the scripts and raw data neeeded to train an NER model and Knowledge Base using spaCy and then to use those models on text. 

## Getting Started

### Dependencies

* Requirements are listed in requirements.txt.
* ex. Windows 10

### Installing

A config.ini file listing the credentials for a local MySQL database and the History Lab MySQL database and its postgres database needs to be created in the main folder. 

### Executing program

* How to run the program
* First, install the Python requirements
```
python3 -m pip install requirements.txt
```
* Second, create a base knowledge base using files in the kbbase subdirectory
* This will create a new DB in MySQL 
* Currently, the base files include: a) All former and current Members of the US Congress; b) World leaders from Archigos; and c) Foreign ministers

```
python3 scripts/create_kb_base.py
```
* Third, train the NER model.
* The NER model is necessary to create the Knowledge Base because Spacy uses the NER model vocabulary for the Knowledge Base description.
```
python3 scripts/NERtraining.py --nlploc models/ner_model --iter 30 --train datafiles/training/covid/*.json datafiles/training/dpi/*.json
```

* Fourth, add new entries to an SQL database to serve as the basis for the new Knowledge Base.

```
python3 scripts/add_to_kb.py --sqldb dpikb --newdb covidkb --entfiles datafiles/kbfiles/*ent* datafiles/kbfiles/dpi/*ent* datafiles/kbfiles/covid/*ent* --aliasfiles datafiles/kbfiles/*alias* datafiles/kbfiles/dpi/*alias* datafiles/kbfiles/covid/*alias*
```

* Fifth, create a new Knowledge Base.

```
python3 scripts/createkb3a.py --ner models/covidnlp --sql local --kb models/covidkb --db covidkb
```

* Finally, run the NER/NEL models

```
python3 scripts/runNER3.py --db covid --ner models/covidnlp --kb models/covidkb --sql covid --outdir nerout/covid
```

## Utilities

There are also some utilities to help with the NER/NEL process.

The output from the models is at the document level. We might want to aggregate the results from individual documents into a single csv file.

```
python3 scripts/neraggregatespacy.py --db nerout/covid --newfile nerout/covid_all.csv --stdname covidkb
```


After running the models, there will probably be some entities that did not link. There is a Python script that will take a range of entity counts and do SPARQL and Google searches for the entity name. The entities have to be verified manually.

```
python3 scripts/sparqlsearch2.py --file nerout/pdbs_all.csv --min 20 --locfile datafiles/pdbsloc20.csv --persfile datafiles/pdbspers20.csv --max 50
```

Once the entities have been manually extracted, there is a script which will format a file to make it easy to incorporate into a new SQL KB.

```
python3 scripts/kbfile_format.py --entfile datafiles/covid_adds2.csv --entvars wiki wikiname d
escr entity --entnf datafiles/kbfiles/covid/covid_ent2.csv --aliasnf datafiles/kbfiles/covid/covid_alias2.csv
```


