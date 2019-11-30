import requests

def search(domain, query):
    url = 'http://%s/freebase/label/_search' % domain
    response = requests.get(url, params={'q': query, 'size': 100})
    id_labels = {}
    if response:
        response = response.json()
        for hit in response.get('hits', {}).get('hits', []):
            freebase_label = hit.get('_source', {}).get('label')
            freebase_id = hit.get('_source', {}).get('resource')
            freebase_score = hit.get('_score')
            freebase_type = hit.get('type')
			
            freebase_number_of_facts = 0
            freebase_match = 0
            freebase_similarity = 0
            id_labels.setdefault(freebase_id, set()).add(freebase_label).add(freebase_type).add(freebase_score).add(freebase_number_of_facts).add(freebase_match).add(freebase_similarity)

if __name__ == '__main__':
    import sys
    try:
        _, DOMAIN, QUERY = sys.argv
    except Exception as e:
        print('Usage: python elasticsearch.py DOMAIN QUERY')
        sys.exit(0)

    for entity, labels in search(DOMAIN, QUERY).items():
        print(entity, labels)