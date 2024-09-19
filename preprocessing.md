# Preprocessing

History Lab uses a spaCy NER model trained with our own annotated data. The NER model has to be trained and then a KnowledgeBase created to perform Named Entity Linking.

## NER training

There are two sets of NER training files in the datafiles/training subdirectory. We have started training two separate NER models based on the documents we have. The files in the covid subdirectory were trained on documents from our Covid collection which consists mainly of emails. Often in the emails, a person's last name will be listed, followed by a comma and the first name. The existing model will mislabel these entities so the training files have more examples of this type of entity label. A model trained on this data could be used with the UN archive or the Clinton emails.

The files in the dpi subdirectory were trainined on the more traditional History Lab documents, mainly the Foreign Relations of the United States and the Central Foreign Policy Files. Many of our documents will list a person's title followed by the last name (e.g., Prime Minister Thatcher). The training files label more of these entities so that the model will catch them. 

```
python3 scripts/NERtraining.py --nlploc models/ner_model --iter 30 --train datafiles/training/covid/*.json datafiles/training/dpi/*.json
```

## KnowledgeBase

There are a couple of steps required to build the KnowledgeBase. SpaCy stores two different sets of information in the KB. The first is the entity ID, along with a description of the entity and the frequency of mentions. Note that this means that the entity name is not stored in this part of the KB. Instead, spaCy stores the names of all the entities in an alias section. The aliases consist of three parts: the alias name, the list of entities sharing that alias in entity ID form, and the probability of each entity occurring. (For convenience, we proporton the probability equally across the list of entities, so if two entities share an alias, the probability of each is .5.) 

For convenience, we have been using a MySQL database to store three tables associated with the KB: entities, aliases, and standard names. We first create a base MySQL database using files in the kbbase subdirectory. Currently, the base files include: a) All former and current Members of the US Congress; b) World leaders from Archigos; and c) Foreign ministers. There are also csv files of other political leaders and their aliases that we have collected. The script below is hard-coded to write into the local MySQL database. 

```
python3 scripts/create_kb_base.py
```

As we processed different collections, we created new files with entities and aliases from the documents. These are in the datafiles/kbfiles subdirectory. This file will also write to a local MySQL database. 

```
python3 scripts/add_to_kb.py --sqldb dpikb --newdb covidkb --entfiles datafiles/kbfiles/*ent* datafiles/kbfiles/dpi/*ent* datafiles/kbfiles/covid/*ent* --aliasfiles datafiles/kbfiles/*alias* datafiles/kbfiles/dpi/*alias* datafiles/kbfiles/covid/*alias*
```

Once the entities and aliases have been added to the MySQL database, we can create spaCy's KnowledgeBase. We need to specify the NER model we will be using  because Spacy uses the NER model vocabulary for the Knowledge Base description.

```
python3 scripts/createkb3a.py --ner models/covidnlp --sql local --kb models/covidkb --db covidkb
```

There does not seem to be an easy way to edit existing entries in a spaCy KB. We can add new entities to the KB, but spaCy assumes the KB will be built one time only. 

```
python3 scripts/add_to_basekb.py --db 'HLkb_base' --sql local --kb 'models/base_kb2024' --ner 'models/HLnlp_2024' --addfile 'datafiles/kb_adds.txt'
```
