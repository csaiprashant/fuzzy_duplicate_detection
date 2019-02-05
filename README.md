# Fuzzy Duplicate Detection in a Big Data Text Corpus

## Introduction
The objective of this project is to come up with a mechanism to detect duplicate objects, where the term ‘duplicate’ means same or mostly similar representations of the same object in a document collection. The project is divided into four parts:
1. Generate a vocabulary of the 1,000 most popular words in the document collection. Proper pre-processing steps like stopword removal and stemming were performed.
2. Generate an inverted index for the 1,000-word vocabulary.
3. Compute the similarity matrix for the inverted index generated in part 2.
4. Provide a list of 10 most identical documents in the document collection.

We also compare different file formats which can be used to store data in Big Data applications. The file formats being considered are text, AVRO, parquet and parquet with snappy compression. We write the inverted index to the file format in question in Part 2 and read data from this file format to memory in Part 3 which gives us an estimate of the format’s read and write efficiencies for our application. We also compare the file sizes of the inverted index saved to disk.

The document collection used for this homework is the ‘Gutenberg Dataset’, which is a collection of 3,036 English books written by 142 authors. However, all calculations were performed on the medium subset of the dataset which had 519 books. We used the pyspark framework to implement Spark functionalities in Python 2.7 on the University of Houston’s whale cluster.

    The project was done using Python 2.7.13 and Spark 2.3.1
### For full report, please refer [Project Report](https://github.com/csaiprashant/academic_projects/blob/master/Fuzzy%20Duplicate%20Detection%20in%20a%20Big%20Data%20Text%20Corpus/projectreport.pdf)

## Files in this Repository
- README.md
- part1_wordcount.py - Generates a 1000-word vocabulary from the text corpus.
- part2_inverted_index_(file-type).py - Generates an inverted index for each word in the vocabulary. The (file-type) represents the file format in which the generated inverted index will be written (text, AVRO, parquet or snappy-compressed parquet). 
    Inverted index is of the form: term1: {doc1:weight1_1,doc2:weight2_1,doc3:weight3_1,…} where weightx_y is: no. of occurrences of term x in document y / total number of words in document y
- part3_similarity_matrix_(file-type).py - Computes cosine similarity for every pair of documents in the corpus using the inverted index generated in part2. The (file-type) represents the file format in which the generated similarity matrix will be written (text, AVRO, parquet or snappy-compressed parquet). 
    Similarity matrix is of the form S(docx, docy) = Σ𝑡ϵ𝑉(𝑤𝑒𝑖𝑔ℎ𝑡 𝑡_𝑑𝑜𝑐𝑥 × 𝑤𝑒𝑖𝑔ℎ𝑡 𝑡_𝑑𝑜𝑐𝑦).
- part4_output.py - Lists the top 10 most similar documents in the corpus.
