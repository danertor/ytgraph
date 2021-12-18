[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/apache-airflow.svg)](https://pypi.org/project/apache-airflow/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
# A YouTube video connection analyzer.
 - Data is collected daily from https://netsg.cs.sfu.ca/youtubedata/
 - The target case will be a graph database to store the relationships between yt_ids, an analytics table and a backup copy of the files in a relational database that will also be used as ground truth source.

## The purpose:
The goal of this project is to analyze the relationship between the videos of YouTube. It uses a graph type data model that can be later use with other graph engines like GraphQL.
An example of questions that this project might help to answer are:
 - Which is the video that is more connected
 - How clustered are the video connections among the different categories? Are the connections usually isolated by video category? Or the videos of a particular category connect to other videos in other categories?
 - Are there any isolated networks of videos? how big they are?
 - What video user has the most connected videos?
 - Are the longest videos usually more connected than the short ones (few seconds in duration).


In addition, this data is cleaned and ready to use to perform clustering data modeling using machine learning.

The data is available on a webserver, and it is published every day in a zip format.
This project uses Apache Airflow for scheduling and processing this data.
It uses only one dag that it is executed every day. The dag contains reusable and easily scalable tasks. A clean code approach was used for developing the logic of the tasks, trying to be as atomic as possible without generating too much over engineering.

We chose the data model described below, so it is easily stored in a relation database. We have a fact table that contains the video information and a connection table that stores the connections of the videos. It is called "related".
The related, or connections, data is stored in the easiest and most accessible form just in case we want to expand this dag and save it into another graph analytical database or engine. So the storage is agnostics of the framework or technology used for analysis. 

This project Uses PostgreSQL as the relational database.
All above framework and code is being hosted in Amazon AWS:
 - Amazon S3
 - Amazon EC2 for Airflow instance.
 - Amazon Redshift for SQL

### Definitions:
A related video is a YouTube video that appears in the "related videos" section of another video. So every YouTube video contains one or more related videos.

### Scalability
**"If the data was increased by 100x"**

A better approach will be Apache Cassandra where the dates will be the partition key. Most likely the latest days or month will be queried the most.

**"If the pipelines were run on a daily basis by 7am"**

In case of an SLA is instructed that forces the dag to finish at a certain moment of the day, an scalable solution will be to use Kuberneted micro-services to spin up Airflow Workers to make the dag runtime scalable. 

**"If the database needed to be accessed by 100+ people"**

The database architecture of this project is a data warehouse like instance. Since it is a batch that feeds this dataware house, the scalability of the database will depend on the data warehouse engine used. Another approach will be to use Apache Cassandra as described above.


## Data quality issues:
 - For a certain depth, the data is split in separate files.
 - Relationships between ids are stored in an inefficient tabular format.
 - Every folder date has a `log.txt` file with the sum of records included in the folder. It will be use as part of the sanity check once we load the files into a database.
 - Steps to clean the data:
	-- Convert the `age` column from number into a datetime object in database.
	-- Expand the `related_ids` into a separate table or graph database.
	-- Add the job date from the folder name and save it into the table for those records.

## Data Dictionary
#### The downloaded files from the source have the following format:
 - `1.txt`, `2.txt`, `3.txt` files:
 
Text field name | Comments
------------- | -------------
video ID | an 11-digit string, which is unique
uploader | a string of the video uploader username
age | an integer number of days between the date when the video was uploaded and Feb.15, 2007 (YouTube's establishment)
category | a string of the video category chosen by the uploader
length | an integer number of the video length
views | an integer number of the views
rate | a float number of the video rate
ratings | an integer number of the ratings
comments | an integer number of the comments
related IDs | up to 20 strings of the related video IDs

#### The stage converted files downloaded files from the source have the following format:
 - details.txt

Text field name | Comments
------------- | -------------
date | Job date when the data was extracted
video ID | an 11-digit string, which is unique
uploader | a string of the video uploader username
age | a string with the datetime when the video was uploaded 
category | a string of the video category chosen by the uploader
length | an integer number of the video length
views | an integer number of the views
rate | a float number of the video rate
ratings | an integer number of the ratings
comments | an integer number of the comments

 - related.txt

Text field name | Comments
------------- | -------------
video ID | an 11-digit string, which is unique
Related ID | a 11-digit string of the related video

#### SQL Tables 

![Alt text](https://raw.githubusercontent.com/danertor/ytgraph/master/doc/data_model.svg)

## Implementation comments
*To see the data dictionary, please refer to the previous Step 3.*
The data pipeline implementation can be found on the `dags` folder.
The data quality tasks check for two things:
 - There must be at least one record of details loaded on every batch run (day).
 - The total of video records found in the log files must be equal to the total of record loaded in the `details` table.

This project include several unittests using the `pytest` python library.
Among other things it checks especially the handling and parsing of the data files and data records.
The different tests are found in the `test` folder.

## Install instructions

*Note: This project must be installed in a GNU/Linux Machine. Apache Airflow 1.X requires a GNU/Linux or MacOS machine.*

>bash /bin/install.sh
