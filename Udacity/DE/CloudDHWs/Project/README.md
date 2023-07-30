## Case of use:

Sparkify __wants to analyze the data they have been collecting about songs and user activity in their new music streaming app__.


To achieve the objective, the construction of a cloud DWH is required, using [Amazon Redshift](https://aws.amazon.com/redshift/). 

The inputs are files located in S3 buckets:
    * Song data: `s3://udacity-dend/song_data`
    * Log data: `s3://udacity-dend/log_data`



## Run ETL

The ETL takes as input the content of the buckets:
* `song_data/`: Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID.
* `log_data/`: The files containst datasets consists of log files in JSON format, based on the songs in the dataset above.   
 

To run the ETL process in the terminal run the following commands:

```bash
python create_tables.py
python etl.py 
```