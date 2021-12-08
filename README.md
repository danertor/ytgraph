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
	-- Convert the days columns from number into a datetime object in database.
	-- Expand the related_ids into a separate table or graph database.
	-- Add the "extracted date" from the folder name and save it into the table for those records.

### Step 3: Define the Data Model

### Step 4: Run ETL to Model the Data
