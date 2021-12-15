## Steps Done
--
### Step 1: Scope the Project and Gather Data
 - Data collected from https://netsg.cs.sfu.ca/youtubedata/
 - The target cases will be a graph database to store the relationships between yt_ids, an analytics table and a backup copy of the files in a relational database that will also be used as ground truth source.

### Step 2: Explore and Assess the Data
#### Data quality issues:
 - For a certain depth, the data is split in separate files.
 - Relationships between ids are stored in an inefficient tabular format.
 - Every folder date has a log.txt file with the sum of records included in the folder. It will be use as part of the sanity check once we load the files into a database.
 - Steps to clean the data:
	-- Convert the "age" column from number into a datetime object in database.
	-- Expand the related_ids into a separate table or graph database.
	-- Add the "job date" from the folder name and save it into the table for those records.

### Step 3: Define the Data Model
#### The downloaded files from the source have the following format:
 - 1.txt, 2.txt, 3.txt files:
 
Text field name | Comments
------------- | -------------
video ID | an 11-digit string, which is unique
uploader | a string of the video uploader's username
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
uploader | a string of the video uploader's username
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


### Step 4: Run ETL to Model the Data
