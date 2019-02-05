from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from itertools import combinations

# Configure Spark context
conf = SparkConf().setAppName("similarity_matrix")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)


def row_to_tuple(list_of_rows):
    """
    Takes a list of Row objects and returns a list of tuples
    :param list_of_rows: list of Row objects
    :return: list of tuples
    """
    return [tuple(x) for x in list_of_rows]


def pair(x):
    """
    Takes a list of (key, value) tuples and returns a list of (pair_of_keys, product_of_respective_values) tuples for
    all combinations of keys in the original list.
    :param x: list of (doc, ntf_doc) tuples
    :return xy: list of ((doc1, doc2), ntf_doc1 * ntf_doc2) tuples
    """
    xx = list(combinations(x, 2))
    xy = [((a[0][0], a[1][0]), a[0][1] * a[1][1]) for a in xx]
    return xy


# Create a Dataframe from the file
inv_index_df = spark.read.parquet("/bigd34/index.parquet.snap")
# Convert the Dataframe to RDD
inv_index = inv_index_df.rdd.map(tuple).map(lambda (a, b): (a, row_to_tuple(b)))
# Apply the pair function to the value of each tuple and sum by key to get similarity metric for each pair of documents
sim_matrix = inv_index.flatMap(lambda (word, list_of_documents): pair(list_of_documents)).reduceByKey(lambda a, b: a + b)
# Save the RDD as a parquet file
sim_matrix_df = sim_matrix.toDF()
sim_matrix_df.write.parquet("/bigd34/sim_matrix.parquet.snap", compression='snappy')
