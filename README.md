# DATA WAREHOUSE USING REDSHIFT

A music streaming startup, 'Sparkify', currently has JSON logs for user activity, as well as JSON metadata for songs in the application. These JSON documents currently reside in S3.

The aim of the project is to build an ETL pipeline, which will extract the data from S3, stage the data in redshift, and subsequently transform the data into a set of dimension tables in redshift, which can then be used for analysis of application usage.

From an analytics perspective, the 'Sparkify' team wishes to be able to find insights into which songs their users are listening to.


## Redshift Considerations

The schema design in redshift can heavily influence the query performance associated. Some relevant areas for query performance are;

- Defining how redshift distributes data across nodes.

- Defining the sort keys, which can determine ordering and speed up joins.

- Definining foreign key and primarty key constraints.


### Data Distribution

How data is distributed is orchestrated by the selected distribution style. When using a 'KEY' distribution style, we inform redshift on how the data should be distributed across nodes, as data will be distributed such that data with that particular key are allocated to the same node.

A good selection for this distribution keys is such that data is distributed evenly, such as to prevent performance hotspots, with collocating related data such that we can easily perform joins. We essentially want to perform joins on columns which are a distribution key for both the tables. Then, redshift can run joins locally instead of having to perform network I/O. We want to choose one dimension table to use as the distribution key for a fact table when using a star schema. We want to use the dimension table which is most commonly joined.

For a slowly changing dimension table, of relatively small size (<1M entries in the case of Redshift) using an 'ALL' distribution style is a good choice. This distributes the table across all nodes for each of retrieval and performance.

### Data Ordering

Using a 'sort key' determines the order with which data is stored on disk for a particular table. Query Performance is increased when the sort key is used in the where clause. Only one sort key can be specified, with multiple columns. Using a 'Compound Key', specifies precedence in columns, and sorts by the first key, then the second key. 'Interleaved Keys', treat each column with equal importance. Compound keys can improve the performance of joins, group by, and order by statements.


## Project Requirements

The requirements for the project are a valid aws account, with accompanying security credentials, as well as a python environment, which satisfies the module requirements given in requirements.txt

You will need to add aws access key and secret information to the dwf.cfg file, under AWS ACCESS. This is not to be comitted to git.

### Redshift
In order to spin up a redshift cluster, we need the following;

To create an IAM role and policy for the redshift cluster to inherit
To create the redshift cluster given a particular configuration.
To spin down the redshift cluster, we wish to remove the above mentioned.

To follow IAC (Infrastructure as Code) practices, and to allow us to easily spin up and spin down the redshift cluster to save costs, we can use the following scripts:

- aws_dwh.py

The scripts will create the neccessary resources for redshift to run.


## ETL Pipeline

The ETL pipeline comprises of two steps;

- Loading the data into the staging tables on redshift from S3.

- Populating analytics tables in redshift from the staging tables.

In the first step, we wish to copy the data from two directories of JSON formatted documents, staging_events, and staging_songs. For the staging songs, we are provided with the format of the data in 's3://udacity-dend/log_json_path.json' and hence we COPY the staging events using this document as the format. With regards to staging songs, no format is provided, and hence we use the 'auto' format for copying across the data.

With regards to creating the analytics tables, we firstly create the songs table from the staging_songs table, using 'SELECT DISTINCT' statements to avoid duplicates in the songs. We do the same for artists, and users respectively, once again using a 'SELECT DISTINCT' statement to avoid duplication. To create the songplays analytics tables, select from the staging_events table, joining artists and songs tables to retrieve the song_ids and artists_ids. We filter the insert statement by the entries for which page is equal to 'NextSong'. We create the time table from the songplays. we use the 'extract' function, to extract the particular part of the datetime object, and the 'timestamp' function to convert the epoch timestamp to a datetime object.

The ETL pipeline is such that the script will load all events from the json files into staging tables, and subsequently into analytics tables, such that the sparkify analytics team can produce valuable insights into user listening behaviour.


## How to Run this Project

- Create an IAM user credentials on AWS console

- Enter the credential info and the cluster configurations in the aws_conf.cfg and dwh.cfg files.

- Create the Redshift cluster by running the aws_dwh.py file in your either your terminal or notebook:
   ```
   Notebook
   %run aws_dwh.py
   ```

- Create tables by running create_tables.py:
   ```
   Notebook
   %run create_tables.py
   ```

- Execute ETL process by running etl.py:
   ```
   Notebook
   %run etl.py
   ```
- Remember to delete the cluster when you are done as this will incur charges overnight  


 ## Project Files
 
 - aws_conf.cfg - Aws configrations and cluster information for creating the cluster
 
 - aws_dwh.py - Python Script for for creating the redshift cluster
 
 - create_tables.py - Python Script for creating tables from the queries
 
 - dwh.cfg - Aws configuration for connecting to the cluster
 
 - etl.py - Python Script for executing the ETL pipeline
 
 - sql_queries.py - Python Script containing all queries to be executed on the cluster
 
 - test.ipynb - Notebook for testing the pipeline
 