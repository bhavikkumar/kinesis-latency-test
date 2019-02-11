package io.bhavik;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel;
import com.amazonaws.services.kinesis.model.Record;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Consumer implements IRecordProcessorFactory {

  private static final Logger log = LoggerFactory.getLogger(Consumer.class);
  private final AtomicLong largestTimestamp = new AtomicLong(0);
  private final List<Long> sequenceNumbers = new ArrayList<>();
  private final Object lock = new Object();

  private class RecordProcessor implements IRecordProcessor {

    @Override
    public void initialize(String shardId) {
    }

    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
      long timestamp = 0;
      List<Long> seqNos = new ArrayList<>();

      for (Record r : records) {
        timing.add(Long.valueOf(System.currentTimeMillis() - Long.parseLong(r.getPartitionKey())).intValue());
        timestamp = Math.max(timestamp, Long.parseLong(r.getPartitionKey()));

        try {
          byte[] b = new byte[r.getData().remaining()];
          r.getData().get(b);
          seqNos.add(Long.parseLong(new String(b, "UTF-8").split(" ")[0]));
        } catch (Exception e) {
          log.error("Error parsing record", e);
          System.exit(1);
        }
      }

      synchronized (lock) {
        if (largestTimestamp.get() < timestamp) {
          log.info(String.format(
              "Found new larger timestamp: %d (was %d), clearing state",
              timestamp, largestTimestamp.get()));
          largestTimestamp.set(timestamp);
          sequenceNumbers.clear();
        }

        // Only add to the shared list if our data is from the latest run.
        if (largestTimestamp.get() == timestamp) {
          sequenceNumbers.addAll(seqNos);
          Collections.sort(sequenceNumbers);
        }
      }

      try {
        checkpointer.checkpoint();
      } catch (Exception e) {
        log.error("Error while trying to checkpoint during ProcessRecords", e);
      }
    }

    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
      log.info("Shutting down, reason: " + reason);
      try {
        checkpointer.checkpoint();
      } catch (Exception e) {
        log.error("Error while trying to checkpoint during Shutdown", e);
      }
    }
  }

  public void logResults() {
    synchronized (lock) {
      if (largestTimestamp.get() == 0) {
        return;
      }

      if (sequenceNumbers.size() == 0) {
        log.info("No sequence numbers found for current run.");
        return;
      }

      // The producer assigns sequence numbers starting from 1, so we
      // start counting from one before that, i.e. 0.
      long last = 0;
      long gaps = 0;
      for (long sn : sequenceNumbers) {
        if (sn - last > 1) {
          gaps++;
        }
        last = sn;
      }

      log.info(String.format(
          "Found %d gaps in the sequence numbers. Lowest seen so far is %d, highest is %d",
          gaps, sequenceNumbers.get(0), sequenceNumbers.get(sequenceNumbers.size() - 1)));
      log.info("Mean is {}, records read is {}", getMean(), timing.size());
    }
  }

  @Override
  public IRecordProcessor createProcessor() {
    return this.new RecordProcessor();
  }

  public static void main(String[] args) {
    KinesisClientLibConfiguration config =
        new KinesisClientLibConfiguration(
            "KinesisProducerLibSampleConsumer",
            Producer.STREAM_NAME,
            new ProfileCredentialsProvider("development"),
            "KinesisProducerLibSampleConsumer")
            .withRegionName(Producer.REGION)
            .withMetricsLevel(MetricsLevel.NONE)
            .withInitialPositionInStream(InitialPositionInStream.LATEST)
            .withIdleTimeBetweenReadsInMillis(250);

    final Consumer consumer = new Consumer();

    Executors.newScheduledThreadPool(1).scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        consumer.logResults();
      }
    }, 10, 1, TimeUnit.SECONDS);

    new Worker.Builder()
        .recordProcessorFactory(consumer)
        .config(config)
        .build()
        .run();
  }

  private List<Integer> timing = new ArrayList<Integer>();

  public double getMean() {
    double sum = 0;
    for (Integer time : timing) {
      sum += time.doubleValue();
    }
    return sum / timing.size();
  }
}
