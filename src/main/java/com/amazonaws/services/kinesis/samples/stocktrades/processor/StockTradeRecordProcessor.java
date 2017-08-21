/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazonaws.services.kinesis.samples.stocktrades.processor;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.samples.stocktrades.model.StockTrade;

/**
 * Processes records retrieved from stock trades stream.
 *
 * 此类实现IRecordProcessor的三个方法：
 * initialize - 由KLC调用，让RecordProcessor了解何时准备好开始接收记录。
 * processRecords - 用于处理接收的记录。
 * shutdown - 由KLC调用，让RecordProcessor了解何时停止接收记录。
 */
public class StockTradeRecordProcessor implements IRecordProcessor {

    private static final Log LOG = LogFactory.getLog(StockTradeRecordProcessor.class);
    private String kinesisShardId;

    // Reporting interval
    //private static final long REPORTING_INTERVAL_MILLIS = 60000L; // 1 minute
    private static final long REPORTING_INTERVAL_MILLIS = 10000L; // 10 seconds
    private long nextReportingTimeInMillis;

    // Checkpointing interval
    //private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L; // 1 minute
    private static final long CHECKPOINT_INTERVAL_MILLIS = 10000L; // 10 seconds
    private long nextCheckpointTimeInMillis;

    // Aggregates stats for stock trades
    private StockStats stockStats = new StockStats();

    /**
     * {@inheritDoc}
     */
    public void initialize(String shardId) {
        LOG.info("Initializing record processor for shard: " + shardId);
        this.kinesisShardId = shardId;
        nextReportingTimeInMillis = System.currentTimeMillis() + REPORTING_INTERVAL_MILLIS;
        nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
    }

    /**
     * {@inheritDoc}
     */
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        LOG.info("Shutting down record processor for shard: " + kinesisShardId);
        // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
        if (reason == ShutdownReason.TERMINATE) {
            checkpoint(checkpointer);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {

        // 统计某一分片的交易信息。
        for (Record record : records) {
            // process record
            processRecord(record);
        }

        // 如果到了统计间隔，打印结果并重置。
        if (System.currentTimeMillis() > nextReportingTimeInMillis) {
            reportStats();
            resetStats();
            nextReportingTimeInMillis = System.currentTimeMillis() + REPORTING_INTERVAL_MILLIS;
        }

        // 如果到了Checkpoint时间，执行checkpoint。
        if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
            checkpoint(checkpointer);
            nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
        }
    }

    private void processRecord(Record record) {
        StockTrade trade = StockTrade.fromJsonAsBytes(record.getData().array());
        if (trade == null) {
            LOG.warn("Skipping record. Unable to parse record into StockTrade. Partition Key: " + record.getPartitionKey());
            return;
        }
        stockStats.addStockTrade(trade);
    }

    private void reportStats() {
        System.out.println("****** Shard " + kinesisShardId + " stats for last 10 seconds ******\n" +
                stockStats + "\n" +
                "****************************************************************\n");
    }

    private void resetStats() {
        stockStats = new StockStats();
    }

    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        LOG.info("Checkpointing shard " + kinesisShardId);
        try {
            checkpointer.checkpoint();
        } catch (ShutdownException se) {
            // Ignore checkpoint if the processor instance has been shutdown (fail over).
            LOG.info("Caught shutdown exception, skipping checkpoint.", se);
        } catch (ThrottlingException e) {
            // Skip checkpoint when throttled. In practice, consider a backoff and retry policy.
            LOG.error("Caught throttling exception, skipping checkpoint.", e);
        } catch (InvalidStateException e) {
            // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
            LOG.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
        }
    }

}
