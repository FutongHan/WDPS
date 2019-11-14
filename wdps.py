import gzip
from bs4 import BeautifulSoup


def split_records(stream):
    payload = ''
    for line in stream:
        if line.strip() == "WARC/1.0":
            yield payload
            payload = ''
        else:
            payload += line


def main():
    # Read warc file
    warcfile = gzip.open('data/sample.warc.gz', "rt", errors="ignore")

    for record in split_records(warcfile):
        # NLP preprocessing
        soup = BeautifulSoup(record, 'html.parser')
        print(soup.get_text())
        # JS still in the file


if __name__ == "__main__":
    main()
