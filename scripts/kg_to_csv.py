description = """Script to create knowledge graph from csv"""
import argparse
from argparse import RawTextHelpFormatter

def cli():
    parser = argparse.ArgumentParser(description=description, formatter_class=RawTextHelpFormatter)
    parser.add_argument(
        "--entfile",
        required=True,
        type=str,
        default='',
        help="CSV with list of entities to lookup")
    parser.add_argument(
        "--newfileloc",
        required=True,
        type=str,
        default='',
        help="Location for new csv file")
    parser.add_argument(
        "--plist",
        nargs = "*",
        required=True,
        type=str,
        default=[],
        help="List of relations to look for")
    parser.add_argument(
        "--wiki",
        required=True,
        type=str,
        default='',
        help="Name of Wikidata ID in entfile")

    args = parser.parse_args()
    entfile = args.entfile
    newfileloc = args.newfileloc
    plist = args.plist
    wiki = args.wiki
    return (entfile, newfileloc, plist,wiki)

def get_results(endpoint_url, query):
    from SPARQLWrapper import SPARQLWrapper, JSON, POST
    sparql = SPARQLWrapper(endpoint_url, agent='Mia Test agent')
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    sparql.setMethod(POST)
    return sparql.query().convert()

def parsewiki(wfield, result):
    rfield = ''
    datafield = wfield
    if datafield in result:
        w = result[datafield]  # w is a dictionary
        if ((w["type"]=="uri") | (w["type"]=="literal")):
            url = str(w["value"]).replace("http://www.wikidata.org/entity/","")
            rfield=url
    return rfield

def wiki_search(entlist, wproperty):
    endpoint_url = "https://query.wikidata.org/sparql"
    wikilist = []
    query = f"""SELECT DISTINCT ?item ?itemLabel ?position ?positionLabel ?start ?end  ?wdLabel
WHERE {{?item wdt:P31 ?instance;
       p:{wproperty} ?position_statement.

       ?position_statement ps:{wproperty} ?position .
     OPTIONAL {{
        ?position_statement ps:{wproperty} ?position ;
        pq:P580 ?start . }}
     OPTIONAL {{
        ?position_statement ps:{wproperty} ?position ;
        pq:P582 ?end . }}
       ?item ?prop ?position.
      ?wd wikibase:directClaim ?prop .
       VALUES ?item {{ {entlist}  }}
       SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
      }}""" 
    results = get_results(endpoint_url, query)
    for result in results["results"]["bindings"]:
        wiki = parsewiki("item", result)                
        iteml = parsewiki("itemLabel", result)                
        party = parsewiki("position", result)
        partyname = parsewiki("positionLabel", result)
        relnname = parsewiki("wdLabel", result)
        pstart = parsewiki("start", result)
        pend = parsewiki("end", result)
        
        wikilist.append([wiki, iteml, party, partyname,relnname,wproperty,pstart,pend])
    return wikilist

def check_relns(p,alist):
    import os
    import pandas as pd
    if os.path.isfile('datafiles/KGfiles/'+str(p)+'_reln.csv'):
        dfexist = pd.read_csv('datafiles/KGfiles/'+str(p)+'_reln.csv')
        existlist = dfexist.WikidataID.values.tolist()
        newlist = [a for a in alist if a not in existlist]
    else:
        newlist = alist
        dfexist = pd.DataFrame(columns=['WikidataID','itemLabel','relnID','relnname','relntypename','relntype','startdate','enddate'])
    return newlist, dfexist

def get_relns(p, alist):
    import pandas as pd
    searchlist, dfexist = check_relns(p, alist)
    wl = []
    for i in range(0, len(searchlist), 5000):
        sublist = searchlist[i:i+5000]
        s = ' '.join(f'wd:{w}' for w in sublist)
        try:
            wiki = wiki_search(s, str(p))
        except:
            wiki = []
        wl.extend(wiki)
    df11 = pd.DataFrame(wl, columns=['WikidataID', 'itemLabel', 'relnID', 'relnname', 'relntypename', 'relntype', 'startdate', 'enddate'])
    dfexist = pd.concat([dfexist, df11], ignore_index=True)
    return dfexist

def main(entfile, newfileloc, plist, wiki):
    import os
    import pandas as pd
    df = pd.read_csv(entfile)
    #df = df[df[wiki].str.contains("Q")]
    archlist = df[wiki].values.tolist()
    print(type(plist))
    for p in plist :
        print(p)
        newdf = get_relns(p,archlist)
        newdf.to_csv(newfileloc+str(p)+'_reln.csv', index=False)

if __name__ == '__main__':
    entfile, newfileloc, plist, wiki = cli()
    main(entfile, newfileloc, plist, wiki)
