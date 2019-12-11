
# coding: utf-8

# In[ ]:


import sys
import gzip
import requests
import json
from bs4 import BeautifulSoup, Comment
import spacy
import csv
from pyspark import SparkContext
nlp = spacy.load("en_core_web_lg")

_, DOMAIN_ES, DOMAIN_KB = sys.argv

INFILE = 'data/sample.warc.gz'
out_file = 'output.tsv'

sc = SparkContext("yarn", "wdps1911")

##### HTML PROCESSING #####

rdd = sc.newAPIHadoopFile(INFILE,
                          "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                          "org.apache.hadoop.io.LongWritable",
                          "org.apache.hadoop.io.Text",
                          conf={"textinputformat.record.delimiter": "WARC/1.0"})



rdd = rdd.saveAsTextFile(out_file)

