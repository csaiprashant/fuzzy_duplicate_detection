from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from collections import Counter
from ast import literal_eval
import nltk
import os
import re

snow = nltk.stem.SnowballStemmer('english')

# Configure Spark context
conf = SparkConf().setAppName("inv_index")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

# Broadcast set of top 1000 words
vocabulary = sc.textFile("/bigd34/vocabulary.out")
vocabulary = vocabulary.map(lambda line: literal_eval(line))
vocab = sc.broadcast(set(vocabulary.keys().collect()))


def ntf(file_contents):
    """
    Takes a list of words in a file and returns a list of (word, word_ntf) tuples.
    :param file_contents: list of strings
    :return: list of tuples
    """
    count = Counter(file_contents)
    normalizing_term = 1 / float(count.most_common(1)[0][1])
    count_dict = {k: v * normalizing_term for k, v in count.items()}
    return count_dict.items()


# Create an RDD from the directory. Each row in the RDD is a tuple (file_path, the_entire_file_as_a_string)
index = sc.wholeTextFiles('/cosc6339_hw2/gutenberg-500/')
# Extract the file_name from the file_path. On the contents, perform the same operations as the function clean in part1.py but keep only the words that appear in our vocabulary
index = index.map(lambda (file_path, contents): (os.path.split(file_path)[1], [snow.stem(word) for word in re.sub('[^a-zA-Z]+', ' ', contents).lower().split() if snow.stem(word) in vocab.value])).filter(lambda (file_name, cleaned_contents): len(cleaned_contents) != 0).cache()
# Calculate normalized term frequency (ntf) of each word in cleaned_contents and map to (file_name, list_of_words_in_file_with_their_ntf) tuples
ntf_index = index.map(lambda (file_name, cleaned_contents): (file_name, ntf(cleaned_contents))).cache()
index.unpersist()
# Flatten the RDD to (word, [(file_name, word's_ntf)]) tuples and concatenate values by key to get the inverted index
inv_index = ntf_index.flatMap(lambda(file_name, words_with_ntf) : [(word[0], [(file_name, word[1])]) for word in words_with_ntf]).reduceByKey(lambda a, b: a + b)
ntf_index.unpersist()
# Save the inverted index as snappy compressed parquet file
inv_index_df = inv_index.toDF()
inv_index_df.write.parquet("/bigd34/index.parquet.snap", compression='snappy')
