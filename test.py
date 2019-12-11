import sys
import gzip
import requests
import json
from bs4 import BeautifulSoup, Comment
import spacy
import csv
nlp = spacy.load("en_core_web_lg")