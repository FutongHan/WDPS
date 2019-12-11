#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function

import csv
import spacy
from bs4 import BeautifulSoup, Comment
from pyspark import SparkConf, SparkContext
import time
import threading
import sys
if sys.version >= '3':
    import queue as Queue
else:
    import Queue

nlp = spacy.load("en_core_web_lg")


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
    _, record = record
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


def find_mentions(record):
    _, record = record

    doc = nlp(record)

    # No entity in the document, proceed to next doc
    if doc.ents == ():
        return

    """ 3) Entity Linking """
    for entity in doc.ents:
        label = entity.label_
        name = entity.text.rstrip().replace("'s", "").replace("´s","")
        if(label in ["TIME", "DATE","PERCENT","MONEY","QUANTITY","ORDINAL","CARDINAL","EVENT"]):
            continue
        return label, name

   

            # """ 3) Entity Linking """
            # for entity in doc.ents:
            #     label = entity.label_
            #     name = entity.text.rstrip().replace("'s","").replace("´s","")
            #     if(label in ["TIME","DATE","PERCENT","MONEY","QUANTITY","ORDINAL","CARDINAL","EVENT"]):
            #         continue
                
                
                
                
            #     candidate = link_entity(label, name,score_margin,diff_margin)
            #     if not candidate:
            #         continue
            #     print([key, name ,candidate[2]])
            #     tsv_writer.writerow([key, name ,candidate[2]])


def process(warc):
    _, record = warc

    # Get the key for the output
    key = find_key(record)

    # No key, process the next record
    if not key:
        return
    
    """ 1) HTML processing """
    html = html2text(record)

    """ 2) SpaCy NER """
    doc = nlp(html)

    # No entity in the document, proceed to next record
    if doc.ents == ():
        return

    test = [entity.label_ for entity in doc.ents]

    return test


    # """ 3) Entity Linking """
    # for entity in doc.ents:
    #     label = entity.label_
    #     name = entity.text.rstrip().replace("'s","").replace("´s","")
        
    #     if(label in ["TIME","DATE","PERCENT","MONEY","QUANTITY","ORDINAL","CARDINAL","EVENT"]):
    #         continue









def parallelize():
    conf = SparkConf()
    conf.set("spark.ui.showConsoleProgress", "false")
    conf.set("spark.driver.memory", "15g")
    # conf.set('spark.executor.memory', '2g')
    # conf.set('spark.executor.cores', '4')

    sc = SparkContext(appName="PythonStatusAPIDemo", conf=conf)

    # Read the Warc file to rdd
    warc = sc.newAPIHadoopFile('hdfs:///user/bbkruit/sample.warc.gz',
                              "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                              "org.apache.hadoop.io.LongWritable",
                              "org.apache.hadoop.io.Text",
                              conf={"textinputformat.record.delimiter": "WARC/1.0"})

    # Process the warc files
    result = warc.map(process)
    print(result.take(10))

    print('success')




    # # Process the HTML files
    # step1 = warc.map(html2text)
    # step2 = step1.map(find_mentions)

    # # print(warc.collect())



if __name__ == "__main__":
    parallelize()


# def delayed(seconds):
#     def f(x):
#         time.sleep(seconds)
#         return x
#     return f


# def call_in_background(f, *args):
#     result = Queue.Queue(1)
#     t = threading.Thread(target=lambda: result.put(f(*args)))
#     t.daemon = True
#     t.start()
#     return result


# def main():
#     conf = SparkConf().set("spark.ui.showConsoleProgress", "false")
#     sc = SparkContext(appName="PythonStatusAPIDemo", conf=conf)

#     def run():
#         rdd = sc.parallelize(range(10), 10).map(delayed(2))
#         reduced = rdd.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
#         return reduced.map(delayed(2)).collect()

#     result = call_in_background(run)
#     status = sc.statusTracker()
#     while result.empty():
#         ids = status.getJobIdsForGroup()
#         for id in ids:
#             job = status.getJobInfo(id)
#             print("Job", id, "status: ", job.status)
#             for sid in job.stageIds:
#                 info = status.getStageInfo(sid)
#                 if info:
#                     print("Stage %d: %d tasks total (%d active, %d complete)" %
#                           (sid, info.numTasks, info.numActiveTasks, info.numCompletedTasks))
#         time.sleep(1)

#     print("Job results are:", result.get())
#     sc.stop()
