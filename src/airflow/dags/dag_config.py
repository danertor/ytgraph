"""
For providing access to the general configuration objects that will be used in this module.
It contains one or more singletons.
"""
config = {
  "DATA_PATH": "../data/yt_data",
  'DATE_FORMAT': "%y%m%d",
  "TARGET_BASE_URI": "https://netsg.cs.sfu.ca/youtubedata/",
  "DETAILS_FILES": ["1.txt", "2.txt", "3.txt", "4.txt"],
  "NUMBER_DETAIL_FIELDS_IN_FILE": 9,
  "S3_BUCKET_NAME_DETAILS": 'detauls_bucket',
  "S3_BUCKET_NAME_RELATED": 'related_bucket',
  "DEFAULT_DATE": "080717"
}
