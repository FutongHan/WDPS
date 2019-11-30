
# coding: utf-8

# In[1]:


from bs4 import BeautifulSoup, Comment
import os
from elasticsearch import search


# In[ ]:


# INFILE = sys.argv[1]
# OUTFILE = sys.argv[2]
# ELASTICSEARCH = "node "


# In[2]:


import spacy
from spacy.lang.en.examples import sentences 
nlp = spacy.load('en_core_web_sm')

def parse(sentence):
    doc = nlp(sentence)
    return [(X.text, X.label_) for X in doc.ents]


# In[3]:


def split_records(stream):
    payload = ''
    for line in stream:
        if line.strip() == 'WARC/1.0':
            yield payload
            payload = ''
        else:
            payload += line


# In[4]:


def find_key(payload):
    key = None
    for line in payload.splitlines():
        if line.startswith("WARC-TREC-ID"):
            key = line.split(': ')[1]
            return key
    return ''


# In[5]:


def record2html(record):
### find html in warc file
    ishtml = False
    html = ""
    for line in record.splitlines():
        ### html starts with <html
        if line.startswith("<html"):
            ishtml = True
        if ishtml:
            html += line
    return html


# In[9]:


entity_dict = []
def search_candidate(token):
    entities = None
    if entity_dict.__contains__(token):
        entities = entity_dict[token]
    else:
        entities = search(ELASTICSEARCH,token).items()
        entity_dict[token] = entities
    return entities


# In[10]:


def html2text(record):
    html_doc = record2html(record)
    # Rule = "/<.*>/";
    useless_tags = ['footer', 'header', 'sidebar', 'sidebar-right', 'sidebar-left', 'sidebar-wrapper', 'wrapwidget', 'widget']
    if html_doc:
        soup = BeautifulSoup(html_doc,"html.parser");
        ### remove tags: <script> <style> <code> <title> <head>
        [s.extract() for s in soup(['script','style', 'code','title','head','footer','header'])]
        ### remove tags id= useless_tags
        [s.extract() for s in soup.find_all(id = useless_tags)]
        ### remove tags class = useless_tags
        [s.extract() for s in soup.find_all(name='div',attrs={"class": useless_tags})]
        ### remove comments
        for element in soup(s=lambda s: isinstance(s, Comment)):
            element.extract()
        # text = soup.get_text("\n", strip=True)

        ### get text in <p></p>
        paragraph = soup.find_all("p")
        text = ""
        for p in paragraph:
            if p.get_text(" ", strip=True) != '':
                text += p.get_text(" ", strip=True)+"\n"
        if text ==  "":
            text = soup.get_text(" ", strip=True)
        # text = re.sub(Rule, "", text)
        # escape character
        # soup_sec = BeautifulSoup(text,"html.parser")

        return text
    return ""


# In[11]:


with open('0000tw-00.warc.565132',errors = 'ignore') as f:
    for record in split_records(f):
        key = find_key(record)
        if key != '':
            #print(key)
            html = html2text(record)
            #print(html)
            doc = nlp(html.replace('\"',' ').replace('\'',' ').replace('|',' '))
#             print([(X.text, X.label_) for X in doc.ents])
            for X in doc.ents:
                entities = search_candidate(X.text)
                print(entities)

