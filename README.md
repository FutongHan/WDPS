<<<<<<< HEAD
Web Data Processing System

This repository contains the Lab Assignment of the 2019-2020 edition of the Vrije Universiteit Amsterdam, Web Data Processing System course of Group 11.

Working Space
/home/wdps1911/WDPS2019

This project is to perform Entity Linking on a collection of web pages. The method consists of the following four steps:

1. Entity Extraction:
    Extract text from HTML pages in WARC files using Beautiful Soup.
    Tokenize the text and recognize named entities in the content using spaCy with en_core_web_sm. The small package is chose for performance reasons.
    
2. Candidate Generation:
	Search for candidate entities in Freebase using ELASTICSEARCH for each mention.
	The result of this step is a list of candidate Freebase IDs for each entity and their score. The score is based on string comparison.
 
3. Candidate Ranking:
	If the mention is no person, and the score is below 4, we consider the mention as unlinkable. If the mention is a person, we check if the mention is contained in the candidate's name. For possible candidates, Freebase is queried to get information about the type of the candidate, which is then compared to the mention type returned ba Spacy. If there is a match, we link the mention and the candidate.
  
4. Entity Linking:
	Link the entity mention to the candidate entity. Return the result in the format: document IDs + '\t' + entity surface form + '\t' + Freebase entity ID
  
To ensure scalability, we use Apache Spark. A virtual environment is set up that contains the dependencies.

Run:
sh run.sh INPUT OUTPUT

Default is INPUT="hdfs:///user/bbkruit/sample.warc.gz" and OUTPUT="output"

Care: the output directory will be deleted if it exists

We have these dependencies:

Spacy (en_core_web_sm)
BeautifulSoup, Comment
SparkContext

install with:
pip install -U spacy
python -m spacy download en_core_web_sm
pip install bs4
pip install pyspark
pip install Beautifulsoup