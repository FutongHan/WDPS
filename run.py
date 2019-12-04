import sys
import gzip
import os
import requests
import json
from bs4 import BeautifulSoup, Comment
import spacy
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
def sparql(domain, query):
    url = 'http://%s/sparql' % domain
    response = requests.post(url, data={'print': True, 'query': query})
    if response:
        try:
            response = response.json()
            print(json.dumps(response, indent=2))
        except Exception as e:
            print(response)
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
                # Candidate generation using Elasticsearch
                nr_of_candidates = 100
                candidates = generate_entities(
                    DOMAIN_ES, entity.text, nr_of_candidates)
                
                # No candidates, skip to next doc
                if not candidates:
                    continue
      
                # Query in KB
                for candidate in candidates:
                    print(candidate[2])
                #     query = "select * where {<http://rdf.freebase.com/ns%s> ?p ?o} limit 100" % candidate[2]
                #     sparql(DOMAIN_KB, query)


if __name__ == '__main__':
    try:
        _, DOMAIN_ES, DOMAIN_KB = sys.argv
    except Exception as e:
        print('Usage: python start.py DOMAIN_ES, DOMAIN_TRIDENT')
        sys.exit(0)

    run(DOMAIN_ES, DOMAIN_KB)
