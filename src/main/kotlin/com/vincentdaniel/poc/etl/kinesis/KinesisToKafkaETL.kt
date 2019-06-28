package com.vincentdaniel.poc.etl.kinesis

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.slf4j.LoggerFactory
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.Option
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.kinesis.common.ConfigsBuilder
import software.amazon.kinesis.common.InitialPositionInStream
import software.amazon.kinesis.common.InitialPositionInStreamExtended
import software.amazon.kinesis.coordinator.Scheduler
import software.amazon.kinesis.retrieval.polling.PollingConfig
import java.io.Serializable
import java.net.URI
import java.util.*
import kotlin.system.exitProcess


@Command(name = "Kinesis To Kafka ETL POC", version = ["0.0.1"])
class KinesisToKafkaETL : Runnable, Serializable {
    private val logger = LoggerFactory.getLogger(this::class.java)

    @Option(names = ["-h", "--help"], usageHelp = true, description = ["print help"])
    var helpRequested: Boolean = false

    @Option(names = ["--aws-access-key-id"], paramLabel = "AWS_ACCESS_KEY_ID")
    var awsAccessKeyId: String? = System.getenv("AWS_ACCESS_KEY_ID")

    @Option(names = ["--aws-secret-access-key"], paramLabel = "AWS_SECRET_ACCESS_KEY")
    var awsSecretAccessKey: String? = System.getenv("AWS_SECRET_ACCESS_KEY")

    @Option(names = ["--kinesis-stream"], paramLabel = "KINESIS_STREAM")
    var kinesisStreamName: String? = System.getenv("KINESIS_STREAM")

    @Option(names = ["--kinesis-endpoint"], paramLabel = "LOCAL_MODE")
    var localMode: Boolean = System.getenv("LOCAL_MODE")?.toBoolean() ?: false

    @Option(names = ["--kinesis-region"], paramLabel = "KINESIS_REGION")
    var kinesisRegion: String? = System.getenv("KINESIS_REGION") ?: "us-east-1"

    @Option(names = ["--kinesis-app"], paramLabel = "KINESIS_APP")
    var kinesisAppName: String? = System.getenv("KINESIS_APP") ?: "KinesisToKafka"

    @Option(names = ["--kafka-bootstrap-servers"], paramLabel = "KAFKA_BOOTSTRAP_SERVERS")
    var kafkaBootstrapServers: String? = System.getenv("KAFKA_BOOTSTRAP_SERVERS")

    @Option(
        names = ["--kafka-topic"],
        paramLabel = "KAFKA_TOPIC"
    )
    var kafkaTopic: String? = System.getenv("KAFKA_TOPIC")

    override fun run() {
        if (helpRequested) {
            CommandLine(this).usage(System.err)
            exitProcess(1)
        }

        val randomUUID = UUID.randomUUID()
        val workerID = "KinesisToKafkaETL:$randomUUID"

        if (kafkaTopic.isNullOrBlank()) {
            logger.error("Kafka topic should be provided")
            CommandLine(this).usage(System.err)
            exitProcess(1)
        }

        val kafkaProducerConfig = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaBootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to "org.apache.kafka.common.serialization.StringSerializer",
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to "org.apache.kafka.common.serialization.StringSerializer",
            ProducerConfig.CLIENT_ID_CONFIG to "$kinesisAppName$randomUUID"
        )
        val kafkaProducer = KafkaProducer<String, String>(kafkaProducerConfig)

        val awsCredentials =
            StaticCredentialsProvider.create(AwsBasicCredentials.create(awsAccessKeyId, awsSecretAccessKey))
        val kinesisClient = KinesisAsyncClient.builder()
            .credentialsProvider(awsCredentials)
            .region(Region.of(kinesisRegion))
            .also {
                if (localMode) {
                    it.endpointOverride(URI.create("http://localhost:4568"))
                }
            }
            .build()
        val dynamoClient = DynamoDbAsyncClient.builder()
            .credentialsProvider(awsCredentials)
            .region(Region.of(kinesisRegion))
            .also {
                if (localMode) {
                    it.endpointOverride(URI.create("http://localhost:4569"))
                }
            }
            .build()
        val cloudWatchClient = CloudWatchAsyncClient.builder()
            .credentialsProvider(awsCredentials)
            .region(Region.of(kinesisRegion))
            .also {
                if (localMode) {
                    it.endpointOverride(URI.create("http://localhost:4586"))
                }
            }
            .build()
        val configsBuilder = ConfigsBuilder(
            kinesisStreamName!!, kinesisAppName!!, kinesisClient, dynamoClient,
            cloudWatchClient, workerID,
            ShardProcessorFactory(kafkaProducer, kafkaTopic!!)
        )

        val scheduler = Scheduler(
            configsBuilder.checkpointConfig(),
            configsBuilder.coordinatorConfig(),
            configsBuilder.leaseManagementConfig(),
            configsBuilder.lifecycleConfig(),
            configsBuilder.metricsConfig(),
            configsBuilder.processorConfig()
                .callProcessRecordsEvenForEmptyRecordList(true),
            configsBuilder.retrievalConfig()
                .retrievalSpecificConfig(
                    PollingConfig(kinesisStreamName!!, kinesisClient)
                        .idleTimeBetweenReadsInMillis(250)
                        .maxGetRecordsThreadPool(Optional.of(100_000))
                )
                .initialPositionInStreamExtended(
                    InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON)
                )
        )
        val schedulerThread = Thread(scheduler)
        schedulerThread.isDaemon = true
        schedulerThread.start()

        Runtime.getRuntime().addShutdownHook(
            Thread {
                try {
                    kafkaProducer.close()
                } catch (e: Throwable) {
                    logger.error("Unable to gracefully shutdown pipeline", e)
                }
            }
        )
    }

    private fun parseTopics(topics: String?): Map<String, String> =
        topics
            ?.split(",")
            ?.map {
                it.trim().split(':', limit = 2).let { pair ->
                    pair[0] to pair[1]
                }
            }
            ?.toMap()
            .orEmpty()


    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            CommandLine.run(KinesisToKafkaETL(), System.err, *args)
        }
    }
}

