from ast import literal_eval
from pyspark import SparkConf, SparkContext

# Configure Spark context
config = SparkConf()
config.setAppName("top_10")
sc = SparkContext(conf=config)

# Create an RDD from the file. Each row in the RDD is a line in the file stored as a string
sim_matrix = sc.textFile("/bigd34/matrix.txt")
# Convert the strings to tuples. Each row in the RDD is a (pair of documents, similarity metric) tuple
sim_matrix = sim_matrix.map(lambda x: literal_eval(x))
# Take the top 10 most similar pairs of documents
most_similar = sim_matrix.takeOrdered(10, key=lambda x: -x[1])
# Save the result
most_sim = sc.parallelize(most_similar)
most_sim.saveAsTextFile("/bigd34/most_sim")
