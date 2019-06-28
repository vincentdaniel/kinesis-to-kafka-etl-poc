package com.vincentdaniel.poc.etl.kinesis

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.PropertyNamingStrategy
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import software.amazon.kinesis.lifecycle.events.*
import software.amazon.kinesis.processor.ShardRecordProcessor
import software.amazon.kinesis.processor.ShardRecordProcessorFactory
import software.amazon.kinesis.retrieval.KinesisClientRecord
import java.io.IOException
import java.nio.charset.CharacterCodingException
import java.nio.charset.Charset
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit

class ShardsRecordProcessor(
    val kafkaProducer: KafkaProducer<String, String>,
    val kafkaTopic: String,
    val objectMapper: ObjectMapper
) : ShardRecordProcessor {
    private val logger = LoggerFactory.getLogger(this::class.java)

    private var shardId: String = ""
    private val decoder = Charset.forName("UTF-8").newDecoder()

    override fun initialize(initializationInput: InitializationInput) {
        this.shardId = initializationInput.shardId()
        logger.info("Initializing record processor for shard: ${initializationInput.shardId()}")
        logger.info("- Initializing @ Sequence: ${initializationInput.extendedSequenceNumber()}")
    }

    override fun leaseLost(leaseLostInput: LeaseLostInput) {
        logger.info("Lost lease, so terminating. shardId = $shardId")
    }

    override fun shardEnded(shardEndedInput: ShardEndedInput) {
        try {
            logger.info("Reached shard end checkpointing. shardId = $shardId")
            shardEndedInput.checkpointer().checkpoint()
        } catch (e: ShutdownException) {
            logger.error("Exception while checkpointing at shard end. Giving up", e)
        } catch (e: InvalidStateException) {
            logger.error("Exception while checkpointing at shard end. Giving up", e)
        }
    }

    override fun shutdownRequested(shutdownRequestedInput: ShutdownRequestedInput) {
        try {
            logger.info("Scheduler is shutting down, checkpointing. shardId = $shardId")
            shutdownRequestedInput.checkpointer().checkpoint()
        } catch (e: ShutdownException) {
            logger.error("Exception while checkpointing at requested shutdown. Giving up", e)
        } catch (e: InvalidStateException) {
            logger.error("Exception while checkpointing at requested shutdown. Giving up", e)
        }
    }

    override fun processRecords(processRecordsInput: ProcessRecordsInput) {
        try {
            if (processRecordsInput.records().isNotEmpty()) {
                logger.info(
                    "Processing ${processRecordsInput.records().size} records from $shardId - latency = ${Duration.ofMillis(
                        processRecordsInput.millisBehindLatest()
                    ).prettyPrint()}"
                )
                processRecordsInput.records().forEach { record -> processSingleRecord(record) }
                logger.info("Checkpoint - latency = ${Duration.ofMillis(processRecordsInput.millisBehindLatest()).prettyPrint()}")
            }
            processRecordsInput.checkpointer().checkpoint()
        } catch (t: Throwable) {
            logger.error("Caught throwable while processing records. Aborting")
            Runtime.getRuntime().halt(1)
        }
    }

    private fun processSingleRecord(record: KinesisClientRecord) {
        try {
            val data = decoder.decode(record.data()).toString()

            logger.debug(data)
            try {
                logger.debug("Pushing record to kafka topic $kafkaTopic: $data")
                kafkaProducer.send(ProducerRecord<String, String>(kafkaTopic, UUID.randomUUID().toString(), data))
                    .get(1, TimeUnit.SECONDS)

            } catch (e: IOException) {
                logger.warn("Unable to parse data: $data", e)
            }

            val approximateArrivalTimestamp = record.approximateArrivalTimestamp().toEpochMilli()
            val currentTime = System.currentTimeMillis()
            val ageOfRecordInMillisFromArrival = currentTime - approximateArrivalTimestamp
            logger.debug("Shard: $shardId - PartitionKey: ${record.partitionKey()} - SequenceNumber: ${record.sequenceNumber()}")
            logger.debug("Arrived $ageOfRecordInMillisFromArrival milliseconds ago")

        } catch (e: CharacterCodingException) {
            logger.error("Malformed data: ${record.data()}", e)
        } catch (e: Exception) {
            logger.info("Record does not match sample record format. Ignoring record with data; ${record.data()}")
        }

    }

    private fun Duration.prettyPrint(): String {
        return this.toString()
            .substring(2)
            .replace("(\\d[HMS])(?!$)".toRegex(), "$1 ")
            .toLowerCase()
    }

}

class ShardProcessorFactory(
    private val kafkaProducer: KafkaProducer<String, String>,
    private val kafkaTopic: String
) : ShardRecordProcessorFactory {

    private val objectMapper = ObjectMapper()
        .registerModule(JavaTimeModule())
        .registerModule(KotlinModule())
        .apply {
            propertyNamingStrategy = PropertyNamingStrategy.SNAKE_CASE
            setSerializationInclusion(JsonInclude.Include.NON_NULL)
            configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false)
            configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
            configure(JsonParser.Feature.IGNORE_UNDEFINED, true)
        }


    override fun shardRecordProcessor(): ShardRecordProcessor {
        return ShardsRecordProcessor(kafkaProducer, kafkaTopic, objectMapper)
    }
}
