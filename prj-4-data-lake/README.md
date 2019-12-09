# Sparkify Song Play Data Lake

## Objective
To use Spark to build a data lake, sourced from log and song data json files, and extracts the data to dimensional tables in parquet format to allow the analysts to understand what songs Sparkify users are listening to.

## Design
The data analysts want to be able to analyse the songs that their users listen to so our fact  will be songplays. From the other data available we can define the dimensions that the analysts will want to slice, dice and aggregate the songplays over. These will be as follows:

- users
- songs 
- artists
- time

To aid performance we will use the partitioning feature of parquet files. `songplays` will be partition by year and month, `time` by year and month and `songs` by year and artist

## How to run
1. etl.py and dl.cfg should be copied to your AWS EMR instance (scp is simple way to do this)
2. Edit dl.cfg to supply the AWS Access Key and AWS Secret Access Key
3. Edit etl.py, edit the main function to set the output_data variable to the s3 bucket where you want the output files to be written 
4. Run etl.py as you would a normal python script `python etl.py`

## ETL Pipeline function

main() - the main function executed when the script is executed with python from the command line

create_spark_session() - function to create a spark session needed by the two main etl functions with the necessary config options enabled to work with aws and S3

process_song_data() - reads and processes the song json files and produces the song and artist parquet output files

process_log_data() - reads and processes the log data json files. Produces users and time dimensions and the main songplays fact parquet files

