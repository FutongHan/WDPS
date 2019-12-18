Web Data Processing System

This repository contains the Lab Assignment of the 2019-2020 edition of the Vrije Universiteit Amsterdam, Web Data Processing System course for Group 11.

Working Sapce
/home/wdps1911/WDPS2019

This project is to perform Entity Linking on a collection of web pages. the method consists of the following five steps:

1.Entity Extraction:
    ·Extract text from HTML pages in WARC files using beautifulsoups.
    ·Tokenize each text and recognize named entities in the content using spaCy(en_core_web_sm)
    
2.Link each entity mention to a set of candidate entities in Freebase using ELASTICSEARCH
  In this step, we will get a list of candidate Freebase IDs for each entity and their score given by ELASTICSEARCH.
  Rank them by the score given by elasticsearch.
  
3.Choose the candidates which score is under 4, ignore it as an unlinkable entity.
  If meet the candidate with the same lexical surface as mention return it.
  Then query the other candidate's abstract in DBPedia using SPARQL.
  
4.Link the entity mention to the candidate entity.
  return the result in the format: document IDs + '\t' + entity surface form + '\t' + Freebase entity ID
  
After finish it at local, we perform our system with Saprk at.

Configuration:
--executor-memory 8G
--driver-memory 8G
--num-executors 16

Run:
sh run.sh

Output:
/Sample/sample
