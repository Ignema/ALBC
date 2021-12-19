# Twitter: Component Based Architecture Example

## Installation

You need a cassandra database up and running. In this example, we used docker to set up a single node cassandra cluster like follows:

    > docker pull cassandra
    
    > docker run --name cassandra-1 -d -p 7000:7000 -p 7001:7001 -p 7199:7199 -p 9042:9042 -p 9160:9160 cassandra

    > docker exec -it cassandra-1 cqlsh -e "
    CREATE KEYSPACE twitter
    WITH REPLICATION = {
        'class' : 'SimpleStrategy',
        'replication_factor' : 1
    };"

Next up, you need to clone the repository and install dependencies

    > git clone 

    > pip install -r requirements.txt

Now run the uvicorn server

    > uvicorn main:app --reload