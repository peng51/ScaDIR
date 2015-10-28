# ScaDIR
Large-scale distributed image retrieval pipeline on top of OpenCV, Hadoop, Mahout and Solr.

Development language: Java

#### Models and Algorithms

The bag-of-visual-words model is used to represent images and bag-of-words model to represent textual sentences.
SIFT from Lire is used to extract SIFT features from images.

We have designed and implemented distributed clustering algorithms on Hadoop, which can run much faster than Mahout K-Means to boost the running time performance of the bag-of-visual-words model.

Inverted indexing with TF-IDF weighting is used to index visual or textual documents.

A linear rule fusion method is used to fuse the ranks of documents in the image retrieval and text retrieval, to get new ranks of the documents, in order to create a better ranked list of documents for the query document (one image and a few keywords).


#### Setup

Java: 1.6

Hadoop: 1.2.1

Mahout: 0.8

Solr: 4.6.1

Build tool: Eclipse
