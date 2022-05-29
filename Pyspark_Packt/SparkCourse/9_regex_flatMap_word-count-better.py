import re
from pyspark import SparkConf, SparkContext
from collections import OrderedDict

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext.getOrCreate(conf = conf)

#input = sc.textFile("file:///sparkcourse/book.txt")
input = sc.textFile("./book.txt")
words = input.flatMap(normalizeWords)
wordCounts = words.countByValue()

#wordCounts = {k:v for k, v in sorted(wordCounts.items(), reverse=True, key = lambda item: item[1])}

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
