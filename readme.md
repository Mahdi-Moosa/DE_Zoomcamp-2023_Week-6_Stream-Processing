# Folder description

* zoomcamp_repo_week_6_stream_processing: Contains files from Week-6 lecture repository.

## Assignment description:

Please implement a streaming application, for finding out popularity of PUlocationID across green and fhv trip datasets.
Please use the datasets [fhv_tripdata_2019-01.csv.gz](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv)
and [green_tripdata_2019-01.csv.gz](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/green). 

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

* *streaming_modified.py:* This script reads both 'fhv_csv' and 'green_csv' kafka topics and performs aggregation based on *pickup location id*. This script needs to be submitted to spark (by spark-submit). A bash script is available in the pyspark folder. To run the script:
    * run: `./../pyspark/spark-submit.sh streaming_modified.py`

### Example output

        -------------------------------------------
        Batch: 0
        -------------------------------------------
        +------------+----------------+
        |PULocationID|ride_count_green|
        +------------+----------------+
        |7           |666             |
        |41          |638             |
        |129         |630             |
        |255         |600             |
        |181         |456             |
        |42          |444             |
        |82          |415             |
        |74          |404             |
        |112         |382             |
        |256         |370             |
        |95          |284             |
        |80          |270             |
        |25          |264             |
        |260         |264             |
        |75          |258             |
        |223         |222             |
        |36          |184             |
        |49          |177             |
        |97          |171             |
        |166         |154             |
        +------------+----------------+
        only showing top 20 rows

        -------------------------------------------
        Batch: 0
        -------------------------------------------
        +------------+--------------+
        |PUlocationID|ride_count_fhv|
        +------------+--------------+
        |181         |1009          |
        |79          |846           |
        |255         |739           |
        |61          |663           |
        |112         |656           |
        |148         |645           |
        |256         |603           |
        |17          |582           |
        |80          |565           |
        |37          |550           |
        |48          |507           |
        |231         |491           |
        |107         |490           |
        |36          |489           |
        |249         |481           |
        |97          |453           |
        |265         |453           |
        |49          |437           |
        |234         |425           |
        |68          |421           |
        +------------+--------------+
        only showing top 20 rows