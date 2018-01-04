# SQL-Query-Executor
This is a data query system using SQL syntax, which has hundreds of time speed-up than the query commands in R and Q. The technique used to accelerate is hashtable. Since this executor does not import the whole data set into main memory, it has obvious advantage when the data is extremely large( greater than 1 million tuples).

Proprocess.py: Pre-indexing for each csv files. Edit Preprocess.py to pre-index needed csv files. In line 7, change csv filenames if needed.
helperAGAIN.py: helper functions for the query system.
get_query_result.py: main function for running the query system.


HOW TO RUN:
In terminal, use the following command:

python3 Proprocess.py ## generateing needed indexing files

python3 get_query_result.py 'SELECT XX FROM XXX WHERE XXX' ## get query result

TEST DATASETS:
The test data sets mentioned in the final report can be downloaded in the following link:
https://drive.google.com/drive/folders/1IZsJE-HBMsc5RvL6Oce1X-gMRKoyJNhR?usp=sharing
