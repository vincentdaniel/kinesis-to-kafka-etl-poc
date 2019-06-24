# Kinesis to Kafka POC with KCL V2

POC to run a simple ETL that reads data from Kinesis and writes it to different topics in kafka based on configuration.

## Setup

- Create a test kinesis stream called `kinesist-test-stream`
- Put your AWS credentials in `~/.aws/credentials`
    - The account should have access to the kinesis test stream
- `docker-compose up -d`
- Run the ETL main: `com.vincentdaniel.poc.etl.kinesis.KinesisToKafkaETL`
    - Example of program arguments: `--kinesis-stream="kinesis-test-stream" --kinesis-app="KinesisToKafka" --aws-access-key-id="XXXX" --aws-secret-access-key="XXXX" --kinesis-region="us-east-1" --kafka-bootstrap-servers="localhost:9092" --kafka-topics="type1:type1,type2:type2,type3:type3"`
- Run the script to put data into the kinesis stream: `com.vincentdaniel.poc.etl.kinesis.utils.KinesisJsonProducer`
- Use kafka console consumer to see messages being consumed and splitted into different topics based on the `action` field of the incoming messages and the `--kafka-topics` flag
    - `kafka-console-consumer.sh --bootstrap-server "localhost:9092" --topic "type2"`

## Tear down

- `docker-compose down`
