import gzip
from bs4 import BeautifulSoup, Comment
import spacy
import os
# nlp = en_core_web_sm.load()

nlp = spacy.load("en_core_web_sm")


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


def main():
    # Read warc file
    warcfile = gzip.open('data/sample.warc.gz', "rt", errors="ignore")

    for record in split_records(warcfile):
        key = find_key(record)
        if key != '':
            # HTML processing
            html = html2text(record)

            # print(html)

            # SpaCy Tokenization
            doc = nlp(html)

            # for token in doc:
            #     print(token.text, token.lemma_, token.pos_, token.tag_,
            #           token.dep_, token.shape_, token.is_alpha, token.is_stop)

            # Entity recognition
            # doc = nlp(html.replace('\"',' ').replace('\'',' ').replace('|"',' ').replace('',' '))
            print([(X.text, X.label_) for X in doc.ents])

            


if __name__ == "__main__":
    main()
