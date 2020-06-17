from pyspark import SparkConf,SparkContext
from pyspark.sql import Row,SQLContext
from pyspark.streaming import StreamingContext
import requests
import sys

conf = SparkConf()
conf.setAppName("TwitterStreamApplication")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 1)
ssc.checkpoint("checkpoint_TwitterStreamApp")
dataStream = ssc.socketTextStream("localhost",9090)

def sumup_tags_counts(new_values, total_sum):
    return (total_sum or 0) + sum(new_values)

def return_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']

def stream_dataframe_to_flask(df):
    top_tags = [str(t.tag) for t in df.select("tag").collect()]
    tags_count = [p.counts for p in df.select("count").collect()]
    url = 'http://0.0.0.0:5050/updateData'
    request_data = {'words': str(top_tags), 'counts': str(tags_count)}
    response = requests.post(url, data=request_data)

def process_rdd(time, rdd):
    print("------------- %s --------------" % str(time))
    try:
        sql_context_instance = return_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda w: Row(tag=w[0], counts=w[1]))
        print(row_rdd)
        tags_counts_df = sql_context_instance.createDataFrame(row_rdd)
        tags_counts_df.registerTempTable("tag_with_counts")
        selected_tags_counts_df = sql_context_instance.sql("select tag, counts from tag_with_counts order by counts desc limit 8")
        selected_tags_counts_df.show()
        stream_dataframe_to_flask(selected_tags_counts_df)
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

 
words = dataStream.flatMap(lambda line: line.split(" "))
hashtags = words.filter(lambda w: '#' in w).map(lambda x: (x, 1))
# hashtags = words.filter(lambda w: '@' in w).map(lambda x: (x, 1)) # process email count in tweets
tags_totals = hashtags.updateStateByKey(sumup_tags_counts)
tags_totals.foreachRDD(process_rdd)
ssc.start()
ssc.awaitTermination()
                    