# Folder description

* zoomcamp_repo_week_6_stream_processing: Contains files from Week-6 lecture repository.

## Assignment description:

        Please implement a streaming application, for finding out popularity of PUlocationID across green and fhv trip datasets.
        Please use the datasets fhv_tripdata_2019-01.csv.gz (https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv)
        and green_tripdata_2019-01.csv.gz (https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/green). 

## Solution

Solutions to the assignment are provided in following sub-directory:
        `zoomcamp_repo_week_6_stream_processing/python/streams-example/pyspark_ny_taxi/`

Link: https://github.com/Mahdi-Moosa/DE_Zoomcamp-2023_Week-6_Stream-Processing/tree/main/zoomcamp_repo_week_6_stream_processing/python/streams-example/pyspark_ny_taxi

Within this directory, there are 3 python scripts:

* *producer_modified.py:* This script reads csv files and acts as Kafka producer. Kafka topic needs to be passed. For example:
    * `python producer_modified.py 'fhv_csv'` to run the fhv data (in the ../../resources directory)
    * `python producer_modified.py 'green_csv'` to run the green data (in the ../../resources directory)

* *consumer_modified.py:* Kafka Consumer script. By default, consumes 'fhv_csv' topic. To consume, 'green_csv':
    * run `python consumer_modified.py --topic 'green_csv'`
* streaming_modified.py