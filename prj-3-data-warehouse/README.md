# Sparkify Song Play Datawarehouse

## Objective
To create a cloud based data warehouse to model data, sourced from log and song data json files, to allow the analysts to understand what songs Sparkify users are listening to.

## Database Design
The data analysts want to be able to analyse the songs that their users listen to so our fact table will be songplays. From the other data available we can define the dimensions that the analysts will want to slice, dice and aggregate the songplays over. These will be as follows:

- users
- songs
- artists
- time

## ETL Design
Log and song data is stored in json files that reside in an S3 bucket. We will be taking advantage of the highly performant Redshift copy command which leverages the Amazon Redshift massively parallel processing (MPP) architecture to read and load data in parallel from files in an Amazon S3 bucket. We will stage the data into flat tables and the utilise the power of Redshift to transform the staging tables into dimension tables and fact table.

