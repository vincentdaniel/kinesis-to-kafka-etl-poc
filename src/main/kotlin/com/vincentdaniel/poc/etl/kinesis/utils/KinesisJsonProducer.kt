package com.vincentdaniel.poc.etl.kinesis.utils

import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.amazonaws.services.kinesis.model.PutRecordRequest
import java.nio.ByteBuffer
import java.util.*

/*
Simple helper to put data into a kinesis stream

To be able to use these "script", you first need to set your AWS credentials need to be set in ~/.aws/credentials file:
[default]
aws_access_key_id=XXXXX
aws_secret_access_key=YYYYYY
 */
object KinesisJsonProducer {
    @JvmStatic
    fun main(args: Array<String>) {
        val streamName = "kinesis-test-stream"

        val kinesisClient = AmazonKinesisClientBuilder.defaultClient()
        repeat(10) {
            val uuid = UUID.randomUUID().toString()
            val jsonLines = listOf(
                """{"action": "type1", "test":"test-$uuid"}""",
                """{"action": "type2", "test":"test-$uuid"}""",
                """{"action": "type3", "test":"test-$uuid"}""",
                """{"action": "type4", "test":"test-$uuid"}"""
            )
            jsonLines.forEach { line ->
                println("Putting record: $line")
                val putRecord = PutRecordRequest()
                    .withStreamName(streamName)
                    .withPartitionKey(uuid)
                    .withData(ByteBuffer.wrap(line.toByteArray()))
                val result = kinesisClient.putRecord(putRecord)
                println("Done ${result.sequenceNumber} - ${result.shardId}")
            }
        }
        kinesisClient.shutdown()
    }
}
