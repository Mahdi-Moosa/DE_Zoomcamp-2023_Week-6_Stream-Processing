# Steps to run

* Conda activate env - `pip install requirements`.

* `conda install -c conda-forge docker`

* Docker install steps: https://docs.docker.com/engine/install/ubuntu/#install-using-the-repository
    * Imp points: Need to set up the repository first before apt istall finds docker.

* `docker network create 'kafka-spark-network'` 
* go to python/docker/kafka folder and `docker compose up`
* create volume: `docker volume create --name=hadoop-distributed-file-system`



# Key concepts

* Simplified intro to Kafka: https://www.youtube.com/watch?v=zPLZUDPi4AY

* Different Kafka terms and their descriptions: https://www.youtube.com/watch?v=SXQtWyRpMKs&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=63
    * Kafka cluser
    * Node
    * Replication
    * Topic
    * Retention
    * Partition
    * Producer
    * Consumer
    * Consumer groups
    * Offset 
    * Consumer offset
    * Auto offset reset (earliest vs. latest)
    * Ackwnowledgement (All vs. 1  vs. 0 )

* Good resource on Kafka-partitioning:
    * https://www.openlogic.com/blog/kafka-partitions

* Tutorial run failed initially. There was a version mismatch between installed spark (3.3.2) and spark that was installed in docker (`spark-submit --version` showed 3.3.1). Error message was:
        `ERROR TransportRequestHandler: Error while invoking RpcHandler#receive()`

    * To solve this, modified build.sh and spark-base.Dockerfile to install spark version 3.3.2. After docker build, `streaming.py` ran successfully.