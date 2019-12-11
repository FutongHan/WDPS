
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

INFILE = 'hdfs:///user/bbkruit/sample.warc.gz'
out_file = 'output.tsv'

sc = SparkContext("yarn", "wdps1911")

##### HTML PROCESSING #####

rdd = sc.newAPIHadoopFile(INFILE,
                          "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                          "org.apache.hadoop.io.LongWritable",
                          "org.apache.hadoop.io.Text",
                          conf={"textinputformat.record.delimiter": "WARC/1.0"})



rdd = rdd.saveAsTextFile(out_file)

def main():
    conf = SparkConf().set("spark.ui.showConsoleProgress", "false")
    sc = SparkContext(appName="PythonStatusAPIDemo", conf=conf)

    def run():
        rdd = sc.parallelize(range(10), 10).map(delayed(2))
        reduced = rdd.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
        return reduced.map(delayed(2)).collect()

    result = call_in_background(run)
    status = sc.statusTracker()
    while result.empty():
        ids = status.getJobIdsForGroup()
        for id in ids:
            job = status.getJobInfo(id)
            print("Job", id, "status: ", job.status)
            for sid in job.stageIds:
                info = status.getStageInfo(sid)
                if info:
                    print("Stage %d: %d tasks total (%d active, %d complete)" %
                          (sid, info.numTasks, info.numActiveTasks, info.numCompletedTasks))
        time.sleep(1)

    print("Job results are:", result.get())
    sc.stop()

if __name__ == "__main__":
    main()