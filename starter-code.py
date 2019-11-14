import gzip
import sys

KEYNAME = "WARC-TREC-ID"

def find_labels(payload, labels):
    key = None
    for line in payload.splitlines():
        if line.startswith(KEYNAME):
            key = line.split(': ')[1]
            break
    for label, freebase_id in labels.items():
        if key and (label in payload):
            yield key, label, freebase_id



def split_records(stream):
    payload = ''
    for line in stream:
        if line.strip() == "WARC/1.0":
            yield payload
            payload = ''
        else:
            payload += line

if __name__ == '__main__':
	if len(sys.argv) < 1:
        	print('Usage: python3 starter-code.py INPUT')
        	sys.exit(0)

	cheats = dict((line.split('\t',2) for line in open('data/sample-labels-cheat.txt').read().splitlines()))
	warcfile = gzip.open(sys.argv[1], "rt", errors="ignore")
	for record in split_records(warcfile):
		for key, label, freebase_id in find_labels(record, cheats):
			print(key + '\t' + label + '\t' + freebase_id)
