from pyspark import SparkConf, SparkContext
import nltk
import re

# set of stopwords
stopwords = {'i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', "you're", "you've", "you'll", "you'd",
             'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', "she's", 'her', 'hers',
             'herself', 'it', "it's", 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which',
             'who', 'whom', 'this', 'that', "that'll", 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been',
             'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but',
             'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against',
             'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down',
             'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when',
             'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no',
             'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'may', 'can', 'will', 'just',
             'don', "don't", 'shall', 'should', "should've", 'now', 'd', 'll', 'm', 'o', 're', 've', 'y', 'ain', 'aren',
             "aren't", 'could', 'couldn', "couldn't", 'didn', "didn't", 'doesn', "doesn't", 'hadn', "hadn't", 'hasn',
             "hasn't", 'haven', "haven't", 'isn', "isn't", 'ma', 'mightn', "mightn't", 'mustn', "mustn't", 'needn',
             "needn't", 'shan', "shan't", 'shouldn', "shouldn't", 'wasn', "wasn't", 'weren', "weren't", 'won', "won't",
             'would', 'wouldn', "wouldn't"}

snow = nltk.stem.SnowballStemmer('english')


def clean(line):
    """
    Takes a string. Removes all characters other than the alphabets in the string. Converts the result to lowercase and
    splits it into a list of words. Finally, applies the Snowball Stemmer on each word in this list if it's not a
    stopword and returns this list of words.
    :param line: string
    :return: cleaned_list_of_words: list of strings
    """
    alphabet_line = re.sub('[^a-zA-Z]+', ' ', line)
    cleaned_list_of_words = [snow.stem(word) for word in alphabet_line.lower().split() if word not in stopwords]
    return cleaned_list_of_words


# Configure Spark context
config = SparkConf()
config.setAppName("wordcount")
sc = SparkContext(conf=config)

# Create an RDD from the dataset. Each row in the RDD is a line of a file in the dataset with no order being maintained
text = sc.textFile("/cosc6339_hw2/gutenberg-500/*", minPartitions=4096)
# Pass each row of the RDD through our clean function and filter out words which are of length 1 or over 20
text = text.flatMap(lambda line: clean(line)).filter(lambda x: 1 < len(x) < 21)
# Map each word to a tuple (word, 1) and add them by key to get tuple (word, wordcount) for each word
text = text.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b, numPartitions=4096)
# Save the 1000 most common words
vocab = text.takeOrdered(1000, key=lambda x: -x[1])
vocabulary = sc.parallelize(vocab)
vocabulary.saveAsTextFile("/bigd34/vocabulary")
