
endpoint_url = "https://query.wikidata.org/sparql"

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

def get_all_aliases(entlist):
    endpoint_url = "https://query.wikidata.org/sparql"
    wikilist = []
    #print(entlist)
    query = """
    SELECT  ?item ?itemAltLabel ?sitelinks
    WHERE 
    {
    VALUES ?item {%s}
            ?item skos:altLabel ?itemAltLabel .
    ?item wikibase:sitelinks ?sitelinks.
            FILTER (lang(?itemAltLabel)='en')
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    } 
    """ % entlist

    results = get_results(endpoint_url, query)
    count = 0
    for result in results["results"]["bindings"]:
        # Keeping track of result count
        count += 1
        if count>=1:
            wiki = parsewiki("item", result)                
            alias = parsewiki("itemAltLabel", result)
            sitelinks = parsewiki("sitelinks", result)
            wikilist.append([wiki, alias,sitelinks])
    return wikilist

def sqlengine(db):
    import configparser
    import sqlalchemy
    from sqlalchemy import create_engine
    from sqlalchemy_utils import database_exists, create_database
    from sqlalchemy import create_engine, text as sql_text
    config = configparser.ConfigParser()
    config.read('config.ini')
    sql = "local"
    if ((sql=="HL") | (sql=="local"))  :
        sqlhost = config[sql]['host']
        sqluser = config[sql]['user']
        sqlpwd = config[sql]['pwd']
    else:
        print("Unknown SQL server")
        sys.exit()
    url = f'mysql+pymysql://'+sqluser[1:-1]+':'+sqlpwd[1:-1]+'@'+sqlhost[1:-1]+'/'+db
    try:
        engine = sqlalchemy.create_engine('mysql+pymysql://'+sqluser[1:-1]+':'+sqlpwd[1:-1]+'@'+sqlhost[1:-1]+'/'+db+ '?charset=utf8mb4' )
        #, encoding='utf8'
    except:
        engine = sqlalchemy.create_engine('mysql+pymysql://'+sqluser[1:-1]+':'+sqlpwd[1:-1]+'@'+sqlhost[1:-1] )
        engine.execute("CREATE DATABASE "+db+" CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;")
        engine = sqlalchemy.create_engine('mysql+pymysql://'+sqluser[1:-1]+':'+sqlpwd[1:-1]+'@'+sqlhost[1:-1]+'/'+db+ '?charset=utf8mb4' )
    if not database_exists(engine.url):
        create_database(engine.url)
        with engine.connect() as conn:
            conn.execute(sql_text("ALTER SCHEMA  `"+ db +"`  DEFAULT CHARACTER SET utf8mb4  DEFAULT COLLATE utf8mb4_unicode_520_ci ;"))
    return engine


def getDesc_from_ID_old(entlist):
    endpoint_url = "https://query.wikidata.org/sparql"
    wikilist = []
    #print(entlist)
    query = """
    SELECT DISTINCT ?item ?itemLabel ?itemDescription ?sitelinks
    WHERE 
    {
    VALUES ?item {%s}.
    ?item wikibase:sitelinks ?sitelinks.
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
    """ % entlist

    results = get_results(endpoint_url, query)
    count = 0
    for result in results["results"]["bindings"]:
        # Keeping track of result count
        count += 1
        if count>=1:
            wiki = parsewiki("item", result)                
            fname = parsewiki("itemLabel", result)
            descr = parsewiki("itemDescription", result)
            sitelinks = parsewiki("sitelinks", result)
            wikilist.append([wiki, fname, descr,sitelinks])
    return wikilist

def getDesc_from_ID(entlist):
    endpoint_url = "https://query.wikidata.org/sparql"
    wikilist = []
    #print(entlist)
    query = """
SELECT DISTINCT ?item ?itemLabel ?itemDescription (SAMPLE(?instanceLabel) as ?instanceLabel)
    WHERE {
      {
    SELECT ?item ?itemLabel ?itemDescription ?instanceLabel
    {
    VALUES ?item {%s}.
    ?item wdt:P31 ?instance.
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
        }
    }
      }
  group by ?item ?itemLabel ?itemDescription 
    """ % entlist
    results = get_results(endpoint_url, query)
    count = 0
    for result in results["results"]["bindings"]:
        # Keeping track of result count
        count += 1
        if count>=1:
            wiki = parsewiki("item", result)                
            fname = parsewiki("itemLabel", result)
            descr = parsewiki("itemDescription", result)
            instance = parsewiki("instanceLabel", result)
            wikilist.append([wiki, fname, descr,instance])
    return wikilist


def getIDfromWiki(entity,wname):
    endpoint_url = "https://query.wikidata.org/sparql"
    wikilist = []
    query = """
SELECT DISTINCT ?item ?itemLabel ?itemDescription ?sitelinks
WHERE
{
    ?item wdt:P31 ?prop .
    VALUES ?item { wd:%s  }.
    ?item wikibase:sitelinks ?sitelinks.

    SERVICE wikibase:label { bd:serviceParam wikibase:language  
                                "[AUTO_LANGUAGE],en" }
}
     """ % entity

    results = get_results(endpoint_url, query)
    count = 0
    #if((len(results["results"]["bindings"])==0)|(len(results["results"]["bindings"])>10)):
    #    wikilist=""
    #else:
    for result in results["results"]["bindings"]:
        # Keeping track of result count
        count += 1
        if count>=1:
            wiki = parsewiki("item", result)                
            fname = parsewiki("itemLabel", result)
            descr = parsewiki("itemDescription", result)
            sitelinks = parsewiki("sitelinks", result)
            wikilist.append([wname, wiki, fname, descr,sitelinks,"w"])
    return wikilist

def getIDfromWikiPers(entity,wname):
    endpoint_url = "https://query.wikidata.org/sparql"
    wikilist = []
    query = """
SELECT ?item ?itemLabel ?itemDescription ?sitelinks
WHERE
{
    ?item wdt:P31 wd:Q5 .
    VALUES ?item { wd:%s  }.
    ?item          wikibase:sitelinks ?sitelinks.
    SERVICE wikibase:label { bd:serviceParam wikibase:language  
                                "[AUTO_LANGUAGE],en" }
}
     """ % entity

    results = get_results(endpoint_url, query)
    count = 0
    #if((len(results["results"]["bindings"])==0)|(len(results["results"]["bindings"])>10)):
    #    wikilist=""
    #else:
    for result in results["results"]["bindings"]:
        # Keeping track of result count
        count += 1
        if count>=1:
            wiki = parsewiki("item", result)                
            fname = parsewiki("itemLabel", result)
            descr = parsewiki("itemDescription", result)
            sitelinks = parsewiki("sitelinks", result)
            wikilist.append([wname, wiki, fname, descr,sitelinks,"w"])
    return wikilist

def getLocIDs(entity):
    endpoint_url = "https://query.wikidata.org/sparql"
    wikilist = []
    query = """   SELECT DISTINCT ?item ?itemLabel ?itemDescription ?sitelinks
	WHERE {
        ?item wdt:P31 ?enttype.
     ?item ?label \"%s\" @en.
    ?item wikibase:sitelinks ?sitelinks.
     SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
     }
 """ % entity
    results = get_results(endpoint_url, query)
    count = 0
    if((len(results["results"]["bindings"])==0)|(len(results["results"]["bindings"])>10)):
        wikilist=""
    else:
        for result in results["results"]["bindings"]:
            # Keeping track of result count
            count += 1
            if count>=1:
                wiki = parsewiki("item", result)                
                fname = parsewiki("itemLabel", result)
                descr = parsewiki("itemDescription", result)
                sitelinks = parsewiki("sitelinks", result)
                wikilist.append([entity, wiki, fname, descr,sitelinks,"s"])
    return wikilist

def getPersIDs(entity):
    endpoint_url = "https://query.wikidata.org/sparql"
    wikilist = []
    query = """
    SELECT DISTINCT ?item ?itemLabel ?itemDescription ?sitelinks
     WHERE {
        ?item wdt:P31 wd:Q5.
     ?item ?label \"%s\" @en.
    ?item wikibase:sitelinks ?sitelinks.
     SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
     }
      """ % entity.title()

    results = get_results(endpoint_url, query)
    count = 0
    if((len(results["results"]["bindings"])==0)|(len(results["results"]["bindings"])>10)):
        wikilist=""
    else:
        for result in results["results"]["bindings"]:
            # Keeping track of result count
            count += 1
            if count>=1:
                wiki = parsewiki("item", result)                
                fname = parsewiki("itemLabel", result)
                descr = parsewiki("itemDescription", result)
                sitelinks = parsewiki("sitelinks", result)
                wikilist.append([entity, wiki, fname, descr,sitelinks,"s"])
    return wikilist
