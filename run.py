import sys
import gzip
import os
import requests
import json
from bs4 import BeautifulSoup, Comment
import spacy
import math
nlp = spacy.load("en_core_web_lg")

# from elasticsearch import search


##### HTML PROCESSING #####
def split_records(stream):
    payload = ''
    for line in stream:
        if line.strip() == "WARC/1.0":
            yield payload
            payload = ''
        else:
            payload += line


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

##### ENTITY LINKING USING TRIDENT #####
def sparql(domain, query, label):
    url = 'http://%s/sparql' % domain
    response = requests.post(url, data={'print': True, 'query': query})
    if response:
        try:
            response = response.json()
            if label == "PERSON" and "people." in json.dumps(response, indent=2):
                return true
            if label == "NORP" and "organisation" in json.dumps(response, indent=2):
                return true         
            if label == "FAC" and "" in json.dumps(response, indent=2):
                return true
            if label == "ORG" and "organisation." in json.dumps(response, indent=2):
                return true
            if label == "GPE" and "location." in json.dumps(response, indent=2):
                return true             
            if label == "LOC" and "location." in json.dumps(response, indent=2):
                return true             
            if label == "PRODUCT" and "" in json.dumps(response, indent=2):
                return true             
            if label == "EVENT" and "event." in json.dumps(response, indent=2):
                return true             
            if label == "WORK_OF_ART" and "" in json.dumps(response, indent=2):
                return true
            if label == "LAW" and "law." in json.dumps(response, indent=2):
                return true 
            if label == "LANGUAGE" and "language." in json.dumps(response, indent=2):
                return true         
            
        except Exception as e:
            # print(response)
            print('error')
            raise e

##### MAIN PROGRAM #####
def run(DOMAIN_ES, DOMAIN_KB):
    # Read warc file
    warcfile = gzip.open('data/sample.warc.gz', "rt", errors="ignore")

    for record in split_records(warcfile):
        key = find_key(record)  # The filename we need to output

        if key != '':
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
                name = entity.text
        
                if(label in ["TIME","DATE","PERCENT","MONEY","QUANTITY","ORDINAL","CARDINAL"]):
                    continue

                # Candidate generation using Elasticsearch
                nr_of_candidates = 100
                candidates = generate_entities(
                    DOMAIN_ES, entity.text, nr_of_candidates)
                                
                
                # No candidates, skip to next doc
                if not candidates:
                    continue
                    
                candidate = candidates[0]
                print(name,label,candidate)
                
                score_margin = 5
                diff_margin = 1
      
                # Query in KB
                for i in range(len(candidates) - 1):


                    if(candidates[i][1] < score_margin):
                        break
                    
                    if(math.abs(candidates[i][1] - candidates[i+1][1]) > diff_margin):
                        print(name,label,candidates[i])
                    else:
                        for j in range(i, len(candidates)):
                            if(math.abs(candidates[i][1] - candidates[j][1]) > diff_margin):
                                print(name,label,candidates[i])
                                break
                            # Query the candidate
                            freebaseID = candidate[j][1:].replace("/",".")
                            query = "select * where {<http://rdf.freebase.com/ns/%s> <http://rdf.freebase.com/ns/type.object.type> ?o} limit 100" % freebaseID
                            if(sparql(DOMAIN_KB, query, label)):
                                print(name,label,candidates[j])
                                break  
                    break
                    # Check if the candidate's tag/label matches the label given by spaCy
                break


if __name__ == '__main__':
    try:
        _, DOMAIN_ES, DOMAIN_KB = sys.argv
    except Exception as e:
        print('Usage: python start.py DOMAIN_ES, DOMAIN_TRIDENT')
        sys.exit(0)

    run(DOMAIN_ES, DOMAIN_KB)
