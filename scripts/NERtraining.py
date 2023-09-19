description = """Script to get training data to annotate"""
import argparse
from argparse import RawTextHelpFormatter

def cli():
    parser = argparse.ArgumentParser(description=description, formatter_class=RawTextHelpFormatter)
    parser.add_argument(
        "--train",
		nargs = "*",
        required=True,
        type=str,
        default=[],
        help="Annotated files to train NER")
    parser.add_argument(
        "--nlploc",
        required=True,
        type=str,
        help="Directory name of new NER model")
    parser.add_argument(
        "--newlabs",
		nargs = "*",
        required=False,
        type=str,
        default=[],
        help="New labels (if any) to add to the NER pipeline")
    parser.add_argument(
        "--iter",
        required=False,
        type=int,
        default=50,
        help="Number of iterations to run (default 50)")
    args = parser.parse_args()
    train = args.train
    nlploc = args.nlploc
    newlabs = args.newlabs
    iter = args.iter
    return(train, nlploc, newlabs, iter)

def main(train, nlploc, newlabs, iter):
    import os
    import sys
    import json
    from spacy.training.example import Example

    from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
    import psycopg2
    import csv
    import spacy
    import random
    from spacy.util import minibatch, compounding
    from pathlib import Path

    dirname = os.getcwd()
    os.chdir(dirname)
    nlp = spacy.load("en_core_web_lg")
    TRAIN_DATA = []
    for t in train:
        print(t)
        with open(t) as train_data:
            tr = json.load(train_data)
        for data in tr:
            ents = [tuple(entity[1:4]) for entity in data['entities']]
            TRAIN_DATA.append((data['content'],{'entities':ents})) 


    losses = {}
    pipe_exceptions = ["ner", "trf_wordpiecer", "trf_tok2vec"]
    unaffected_pipes = [pipe for pipe in nlp.pipe_names if pipe not in pipe_exceptions]
    ner=nlp.get_pipe("ner")
    for l in newlabs:
        ner.add_label(l)
    # TRAINING THE MODEL
    with nlp.disable_pipes(*unaffected_pipes):
        sizes = compounding(1.0, 4.0, 1.001)
        # Training for 100 iterations     
        for itn in range(iter):
            # shuffle examples before training
            random.shuffle(TRAIN_DATA)
            # batch up the examples using spaCy's minibatch
            batches = minibatch(TRAIN_DATA, size=sizes)
            # dictionary to store losses
            losses = {}
            for batch in batches:
                for text, annotations in batch:
                    #print(text)
                    doc = nlp.make_doc(text)
                    #print(doc, annotations)
                    example = Example.from_dict(doc, annotations)
                    nlp.update([example], drop=0.5, losses=losses)
                    
    if not os.path.exists(nlploc):
         os.mkdir(nlploc)
    nlp.to_disk(nlploc)
if __name__ == '__main__':
    train, nlploc, newlabs, iter = cli()
    main(train, nlploc, newlabs, iter)
