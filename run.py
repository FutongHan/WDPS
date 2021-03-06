from bs4 import BeautifulSoup, Comment
from pyspark import SparkContext
import requests
import json
import sys
import spacy

##### HTML PROCESSING #####
def record_to_html(record):
    _, record = record

    # find html in warc file
    ishtml = False
    html = ""
    for line in record.splitlines():
        # html starts with <html
        if line.startswith("<html"):
            ishtml = True
        if ishtml:
            html += line
    if not html:
        return

    # key for the output
    key = ''
    for line in record.splitlines():
        if line.startswith("WARC-TREC-ID"):
            key = line.split(': ')[1]
            break
    if not key:
        return

    yield key, html


def html_to_text(record):
    key, html = record

    useless_tags = ['footer', 'header', 'sidebar', 'sidebar-right', 'sidebar-left', 'sidebar-wrapper', 'wrapwidget', 'widget']
    soup = BeautifulSoup(html, "html.parser")
    [s.extract() for s in soup(['script', 'style', 'code', 'title', 'head', 'footer', 'header'])]
    [s.extract() for s in soup.find_all(id=useless_tags)]
    [s.extract() for s in soup.find_all(name='div', attrs={"class": useless_tags})]

    for element in soup(s=lambda s: isinstance(s, Comment)):
        element.extract()

    paragraph = soup.find_all("p")
    text = ""
    for p in paragraph:
        if p.get_text(" ", strip=True) != '':
            text += p.get_text(" ", strip=True)+"\n"
    if text == "":
        text = soup.get_text(" ", strip=True)

    yield key, text

##### ENTITY RECOGNITION #####
def named_entity_recognition(record):
    key, html = record

    doc = SPACY(html)

    for mention in doc.ents:
        label = mention.label_
        name = mention.text.rstrip().replace("'s", "").replace("´s", "")
        if(label in ["TIME", "DATE", "PERCENT", "MONEY", "QUANTITY", "ORDINAL", "CARDINAL", "EVENT"]):
            continue
        
        for split_result in name.split(", "):
            yield key, split_result, label

##### ENTITY CANDIDATE GENERATION #####
def generate_candidates(record):
    key, name, label = record
    nr_of_candidates = 100

    url = 'http://%s/freebase/label/_search' % DOMAIN_ES
    response = requests.get(url, params={'q': name, 'size': nr_of_candidates})
    id_labels = []
    if response:
        response = response.json()
        for hit in response.get('hits', {}).get('hits', []):

            freebase_label = hit.get('_source', {}).get('label')
            freebase_id = hit.get('_source', {}).get('resource')
            freebase_score = hit.get('_score', {})
            id_labels.append((freebase_label, freebase_score, freebase_id))

    entity = link_entity(name, label, id_labels)
    if not entity:
        return
    yield key, name, entity[2]


#### ENTITY RANKING + LINKING #########
def link_entity(name, label, candidates):
    exact_matches = []

    if not candidates:
        return
    
    if label != "PERSON" and candidates[0][1] < 4:
        return

    for candidate in candidates:
        if name.lower() == candidate[0].lower():
            exact_matches.append(candidate)
        elif label == "PERSON" and name.lower() in candidate[0].lower():
            exact_matches.append(candidate)
    if not exact_matches:
        return
    for match in exact_matches:
        freebaseID = match[2][1:].replace("/", ".")
        if(sparql_query(freebaseID, label)):
            return match

    return candidates[0]

def sparql_query(freebaseID, label):
    url = 'http://%s/sparql' % DOMAIN_KB
    query = "select * where {<http://rdf.freebase.com/ns/%s> <http://rdf.freebase.com/ns/type.object.type> ?o} limit 100" % freebaseID
    response = requests.post(url, data={'print': True, 'query': query})
    if response:
        try:
            response = response.json()
            if label == "PERSON" and "people." in json.dumps(response, indent=2):
                return True
            if label == "NORP" and "organisation" in json.dumps(response, indent=2):
                return True
            if label == "FAC" and "" in json.dumps(response, indent=2):
                return True
            if label == "ORG" and "organisation." in json.dumps(response, indent=2):
                return True
            if label == "GPE" and "location." in json.dumps(response, indent=2):
                return True
            if label == "LOC" and "location." in json.dumps(response, indent=2):
                return True
            if label == "PRODUCT" and "" in json.dumps(response, indent=2):
                return True
            if label == "EVENT" and "event." in json.dumps(response, indent=2):
                return True
            if label == "WORK_OF_ART" and "" in json.dumps(response, indent=2):
                return True
            if label == "LAW" and "law." in json.dumps(response, indent=2):
                return True
            if label == "LANGUAGE" and "language." in json.dumps(response, indent=2):
                return True
            return False

        except Exception as e:
            print('error')
            raise e

def output(record):
    key, name, entity_id = record
    yield key + '\t' + name + '\t' + entity_id

if __name__ == "__main__":
    try:
        _, DOMAIN_ES, DOMAIN_KB, INPUT, OUTPUT = sys.argv
    except Exception:
        print('Usage: DOMAIN_ES, DOMAIN_TRIDENT')
        sys.exit(0)
    SPACY = spacy.load("en_core_web_sm")
    # Spark setup with conf from command line
    sc = SparkContext()
    # split WARC
    config = {"textinputformat.record.delimiter": "WARC/1.0"}

    # Read the Warc file to rdd
    rdd = sc.newAPIHadoopFile(INPUT,
                               "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                               "org.apache.hadoop.io.LongWritable",
                               "org.apache.hadoop.io.Text", conf=config)

    # Process the warc files, result is an rdd with each element "key + '\t' + name + '\t' + FreebaseID"
    rdd = rdd.flatMap(record_to_html)
    rdd = rdd.flatMap(html_to_text)
    rdd = rdd.flatMap(named_entity_recognition)
    rdd = rdd.flatMap(generate_candidates)
    rdd = rdd.flatMap(output)

    #print(rdd.take(10))
    result = rdd.saveAsTextFile(OUTPUT)
