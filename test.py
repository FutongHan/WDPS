from __future__ import print_function

import csv
import spacy
from bs4 import BeautifulSoup, Comment
from pyspark import SparkConf, SparkContext
import requests
import json
import time
import threading
import sys

nlp = spacy.load("en_core_web_sm")


##### HTML PROCESSING #####
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
    _, record = record

    # Get the key for the output
    key = ''
    for line in record.splitlines():
        if line.startswith("WARC-TREC-ID"):
            key = line.split(': ')[1]
    
    # No key, process the next record
    if not key:
        yield '',''
    
    html_doc = record2html(record)
    useless_tags = ['footer', 'header', 'sidebar', 'sidebar-right', 'sidebar-left', 'sidebar-wrapper', 'wrapwidget', 'widget']
    if html_doc:
        soup = BeautifulSoup(html_doc, "html.parser")
        # remove tags: <script> <style> <code> <title> <head>
        [s.extract() for s in soup(['script', 'style', 'code', 'title', 'head', 'footer', 'header'])]
        # remove tags id= useless_tags
        [s.extract() for s in soup.find_all(id=useless_tags)]
        # remove tags class = useless_tags
        [s.extract() for s in soup.find_all(name='div', attrs={"class": useless_tags})]
        # remove comments
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
    yield '',''


##### ENTITY CANDIDATE GENERATION #####
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


#### ENTITY RANKING + LINKING #########
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


def link_entity(label, name, score_margin, diff_margin):
    # print("name,label", name, label)

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
        freebaseID = match[2][1:].replace("/", ".")
        if(sparql(DOMAIN_KB, freebaseID, label)):
            return match

    return candidates[0]


# Spark will handle this function on the cluster
def process(DOMAIN_ES, DOMAIN_KB):
    def process_partition(warc):
        key, html = warc

        score_margin = 4
        diff_margin = 1

        """ 2) SpaCy NER """
        doc = nlp(html)

        # No entity in the document, proceed to next record
        if doc.ents == ():
            return

        # Get the mentions in the document
        # linked_list = []
        for mention in doc.ents:
            label = mention.label_
            name = mention.text.rstrip().replace("'s", "").replace("´s", "")

            if(label in ["TIME", "DATE", "PERCENT", "MONEY", "QUANTITY", "ORDINAL", "CARDINAL", "EVENT"]):
                continue

            """ 3) Entitiy Linking """
            # 3.1 Get candidates
            candidate = link_entity(label, name, score_margin, diff_margin)

            # No candidates
            if not candidate:
                continue

            # Yield all the linked entities
            yield key + '\t' + name + '\t' + candidate[2]

    return process_partition


def setup_spark(DOMAIN_ES, DOMAIN_KB):
    # Spark setup
    sc = SparkContext()

    # Read the Warc file to rdd
    warc = sc.newAPIHadoopFile('hdfs:///user/bbkruit/sample.warc.gz',
                               "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                               "org.apache.hadoop.io.LongWritable",
                               "org.apache.hadoop.io.Text",
                               conf={"textinputformat.record.delimiter": "WARC/1.0"})

    # Process the warc files, result is an rdd with each element "key + '\t' + name + '\t' + FreebaseID"
    warc = warc.flatMap(html2text)
    warc = 
    result = warc.flatMap(process(DOMAIN_ES, DOMAIN_KB))

    print(result.take(10))
    #result = result.saveAsTextFile('sample')
    
    print('success')


if __name__ == "__main__":
    try:
        _, DOMAIN_ES, DOMAIN_KB = sys.argv
    except Exception:
        print('Usage: DOMAIN_ES, DOMAIN_TRIDENT')
        sys.exit(0)

    setup_spark(DOMAIN_ES, DOMAIN_KB)