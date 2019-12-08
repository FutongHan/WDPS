
# coding: utf-8

# In[ ]:


import sys
import gzip
import requests
import json
from bs4 import BeautifulSoup, Comment
import spacy
import csv
from pyspark import SparkContext
nlp = spacy.load("en_core_web_lg")

DOMAIN_ES, DOMAIN_KB = sys.argv

INFILE = 'data/sample.warc.gz'
out_file = 'output.tsv'

sc = SparkContext("yarn", "wdps1911")

##### HTML PROCESSING #####

rdd = sc.newAPIHadoopFile(INFILE,
                          "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                          "org.apache.hadoop.io.LongWritable",
                          "org.apache.hadoop.io.Text",
                          conf={"textinputformat.record.delimiter": "WARC/1.0"})

def find_key(payload):
    key = None
    for line in payload.splitlines():
        if line.startswith("WARC-TREC-ID"):
            key = line.split(': ')[1]
            return key
    return ''

def record2html(record):
    # find html in warc file
    ishtml = False
    html = ""
    for line in record.splitlines():
        # html starts with <html
        if line.startswith("<html"):
            ishtml = True
        if ishtml:
            html += line
    return html


def html2text(record):
    html_doc = record2html(record)
    # Rule = "/<.*>/";
    useless_tags = ['footer', 'header', 'sidebar', 'sidebar-right',
                    'sidebar-left', 'sidebar-wrapper', 'wrapwidget', 'widget']
    if html_doc:
        soup = BeautifulSoup(html_doc, "html.parser")
        # remove tags: <script> <style> <code> <title> <head>
        [s.extract() for s in soup(
            ['script', 'style', 'code', 'title', 'head', 'footer', 'header'])]
        # remove tags id= useless_tags
        [s.extract() for s in soup.find_all(id=useless_tags)]
        # remove tags class = useless_tags
        [s.extract() for s in soup.find_all(
            name='div', attrs={"class": useless_tags})]
        # remove comments
        for element in soup(s=lambda s: isinstance(s, Comment)):
            element.extract()
        # text = soup.get_text("\n", strip=True)

        # get text in <p></p>
        paragraph = soup.find_all("p")
        text = ""
        for p in paragraph:
            if p.get_text(" ", strip=True) != '':
                text += p.get_text(" ", strip=True)+"\n"
        if text == "":
            text = soup.get_text(" ", strip=True)
        # text = re.sub(Rule, "", text)
        # escape character
        # soup_sec = BeautifulSoup(text,"html.parser")

        return text
    return ""

def candidate_entity_recognization(record):
#     score_margin = 4
#     diff_margin = 1
    # Read warc file
#     warcfile = gzip.open('data/sample.warc.gz', "rt", errors="ignore")

#     with open('output.tsv', 'w+') as out_file:
#         tsv_writer = csv.writer(out_file, delimiter='\t')
    _,payload = record
    for record in split_records(warcfile):
        key = find_key(record)  # The filename we need to output

        if not key:
            continue

            """ 1) HTML processing """
        html = html2text(record)

            """ 2) SpaCy NER """
        doc = nlp(html)

            # No entity in the document, proceed to next doc
        if doc.ents == ():
            continue
        for X in doc.ents:
            yield (X.text, X.label_)
            
rdd = rdd.flatMap(candidate_entity_recognization)

def generate_entities(domain, query, size):
    url = 'http://%s/freebase/label/_search' % domain
    response = requests.get(url, params={'q': query, 'size': size})
    id_labels = []
    if response:
        response = response.json()
        for hit in response.get('hits', {}).get('hits', []):

            freebase_label = hit.get('_source', {}).get('label')
            freebase_id = hit.get('_source', {}).get('resource')
            freebase_score = hit.get('_score', {})

            id_labels.append((freebase_label, freebase_score, freebase_id))

    return id_labels

def sparql(domain, freebaseID, label):
    url = 'http://%s/sparql' % domain
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
            
def link_entity(label, name,score_margin,diff_margin):
    print("name,label",name,label)

    # Candidate generation using Elasticsearch
    nr_of_candidates = 100
    candidates = generate_entities(DOMAIN_ES, name, nr_of_candidates)

    exact_matches = []

    if not candidates:
        return None

    if label != "PERSON" and candidates[0][1] < 4:
        return None

    if label == "PERSON" and candidates[0][1] < 1.5:
        return None

    for candidate in candidates:
        if name.lower() == candidate[0].lower():
            exact_matches.append(candidate)

    if not exact_matches:
        return candidates[0]

    for match in exact_matches:
        freebaseID = match[2][1:].replace("/",".")
        if(sparql(DOMAIN_KB, freebaseID, label)):
            return match

    return candidates[0]


def run(record):
    _,payload = record
    score_margin = 4
    diff_margin = 1
    for record in split_records(warcfile):
        key = find_key(record)  # The filename we need to output

        if not key:
            continue

        """ 1) HTML processing """
        html = html2text(record)

        """ 2) SpaCy NER """
        doc = nlp(html)

            # No entity in the document, proceed to next doc
        if doc.ents == ():
            continue
            
        """ 3) Entity Linking """
        for entity in doc.ents:
            label = entity.label_
            name = entity.text.rstrip().replace("'s","").replace("´s","")
            if(label in ["TIME","DATE","PERCENT","MONEY","QUANTITY","ORDINAL","CARDINAL","EVENT"]):
                continue
            candidate = link_entity(label, name,score_margin,diff_margin)
            if not candidate:
                continue
            yield([key, name ,candidate[2]])
#             tsv_writer.writerow([key, name ,candidate[2]])

rdd = rdd.saveAsTextFile(out_file)

