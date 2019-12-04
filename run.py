import sys
import gzip
import os
import requests
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


##### ENTITY LINKING #####
# def search_candidate(token, DOMAIN):
#     entity_dict = {}
#     entities = None
#     if entity_dict.__contains__(token):
#         entities = entity_dict[token]
#     else:
#         entities = search(DOMAIN, token).items()
#         entity_dict[token] = entities
#     return entities

##### ENTITY GENERATION #####
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

            id_labels.append((freebase_id, freebase_label, freebase_score))

    return id_labels

##### MAIN PROGRAM #####


def run(DOMAIN):
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

            entities = [(X.text, X.label_, X.kb_id_) for X in doc.ents]
            print(entities)

            """ 4) Entity Linking """


if __name__ == '__main__':
    run()
    try:
        _, DOMAIN = sys.argv
    except Exception as e:
        print('Usage: python start.py DOMAIN')
        sys.exit(0)

    run(DOMAIN)
