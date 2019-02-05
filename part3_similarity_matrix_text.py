from pyspark import SparkConf, SparkContext
from ast import literal_eval
from itertools import combinations

# Configure Spark context
conf = SparkConf().setAppName("similarity_matrix")
sc = SparkContext(conf=conf)


def pair(x):
    """
    Takes a list of (key, value) tuples and returns a list of (pair_of_keys, product_of_respective_values) tuples for
    all combinations of keys in the original list.
    :param x: list of (doc, ntf_doc) tuples
    :return: list of ((doc1, doc2), ntf_doc1 * ntf_doc2) tuples
    """
    xx = list(combinations(x, 2))
    xy = [((a[0][0], a[1][0]), a[0][1] * a[1][1]) for a in xx]
    return xy

# Create an RDD from the file. Each row in the RDD is a line in the file stored as a string
inv_index = sc.textFile("/bigd34/inverted.txt", minPartitions=64)
# Convert the strings to tuples. Each row in the RDD becomes a (word, list_of_documents_that_word_appears_in_along_with_its_ntf) tuple
inv_index = inv_index.map(lambda x: literal_eval(x)).cache()
# Apply the pair function to the value of each tuple and sum by key to get similarity metric for each pair of documents
sim_matrix = inv_index.flatMap(lambda (word, list_of_documents) : pair(list_of_documents)).reduceByKey(lambda a, b : a + b, numPartitions=64)
inv_index.unpersist()
# Save the RDD as a text file
sim_matrix.saveAsTextFile("/bigd34/similarity_matrix")
