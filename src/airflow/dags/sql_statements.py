# video ID, uploader, age, category, length, views, rate, ratings, comments, related ID
# mNyLXihvAIQ,Ceniale90,1236,Music,204,515,5,6,3
CREATE_DETAILS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS trips (
date VARCHAR(20) NOT NULL,
video_id VARCHAR(20) NOT NULL,
uploader VARCHAR(256) NOT NULL,
age TIMESTAMP NOT NULL,
category VARCHAR(256),
length INTEGER,
views BIGINT,
rate BIGINT,
ratings BIGINT,
comments VARCHAR(4000),
PRIMARY KEY(date, video_id))
DISTSTYLE ALL;
"""

CREATE_RELATED_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS related (
id INTEGER NOT NULL,
video_id VARCHAR(20) NOT NULL,
related_id VARCHAR(20) NOT NULL,
PRIMARY KEY(id))
DISTSTYLE ALL;
"""

CREATE_TABLES_SQL = '\n'.join([CREATE_DETAILS_TABLE_SQL, CREATE_RELATED_TABLE_SQL])

AGGREGATED_RELATED_SQL = """
BEGIN;
DROP TABLE IF EXISTS related_aggregated;
CREATE TABLE related_aggregated AS
SELECT
    d.date,
    d.video_id,
    d.uploader,
    d.age,
    d.category,
    d.length,
    d.views,
    d.rate,
    d.ratings,
    d.comments,
FROM details d
JOIN (
    SELECT
        video_id,
        COUNT(related_id) AS num_relateds
    FROM related
    GROUP BY video_id
) AS ag ON d.video_id = ag.video_id
;"""
