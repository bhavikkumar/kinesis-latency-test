package io.bhavik;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Producer {

  private static final Logger log = LoggerFactory.getLogger(Producer.class);

  private static final ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(1);

  private static final String TIMESTAMP = Long.toString(System.currentTimeMillis());

  private static final int DATA_SIZE = 128;

  private static final int SECONDS_TO_RUN = 180;

  private static final int RECORDS_PER_SECOND = 1000;

  public static final String STREAM_NAME = "latency-test";

  public static final String REGION = "us-west-2";

  public static KinesisProducer getKinesisProducer() {
    KinesisProducerConfiguration config = new KinesisProducerConfiguration();
    config.setRegion(REGION);
    config.setCredentialsProvider(new ProfileCredentialsProvider("development"));
    config.setMetricsLevel("none");
    KinesisProducer producer = new KinesisProducer(config);
    return producer;
  }

  public static void main(String[] args) throws Exception {
    final KinesisProducer producer = getKinesisProducer();

    // The monotonically increasing sequence number we will put in the data of each record
    final AtomicLong sequenceNumber = new AtomicLong(0);

    // The number of records that have finished (either successfully put, or failed)
    final AtomicLong completed = new AtomicLong(0);

    // KinesisProducer.addUserRecord is asynchronous. A callback can be used to receive the results.
    final FutureCallback<UserRecordResult> callback = new FutureCallback<UserRecordResult>() {
      @Override
      public void onFailure(Throwable t) {
        // We don't expect any failures during this sample. If it
        // happens, we will log the first one and exit.
        if (t instanceof UserRecordFailedException) {
          Attempt last = Iterables.getLast(
              ((UserRecordFailedException) t).getResult().getAttempts());
          log.error(String.format(
              "Record failed to put - %s : %s",
              last.getErrorCode(), last.getErrorMessage()));
        }
        log.error("Exception during put", t);
        System.exit(1);
      }

      @Override
      public void onSuccess(UserRecordResult result) {
        completed.getAndIncrement();
      }
    };

    // The lines within run() are the essence of the KPL API.
    final Runnable putOneRecord = new Runnable() {
      @Override
      public void run() {
        ByteBuffer data = Utils.generateData(sequenceNumber.get(), DATA_SIZE);
        // TIMESTAMP is our partition key
        ListenableFuture<UserRecordResult> f =
            producer.addUserRecord(STREAM_NAME, Long.toString(System.currentTimeMillis()), Utils.randomExplicitHashKey(), data);
        Futures.addCallback(f, callback);
      }
    };

    // This gives us progress updates
    EXECUTOR.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        long put = sequenceNumber.get();
        long total = RECORDS_PER_SECOND * SECONDS_TO_RUN;
        double putPercent = 100.0 * put / total;
        long done = completed.get();
        double donePercent = 100.0 * done / total;
        log.info(String.format(
            "Put %d of %d so far (%.2f %%), %d have completed (%.2f %%)",
            put, total, putPercent, done, donePercent));
      }
    }, 1, 1, TimeUnit.SECONDS);

    // Kick off the puts
    log.info(String.format(
        "Starting puts... will run for %d seconds at %d records per second",
        SECONDS_TO_RUN, RECORDS_PER_SECOND));
    executeAtTargetRate(EXECUTOR, putOneRecord, sequenceNumber, SECONDS_TO_RUN, RECORDS_PER_SECOND);

    EXECUTOR.awaitTermination(SECONDS_TO_RUN + 1, TimeUnit.SECONDS);
    log.info("Waiting for remaining puts to finish...");
    producer.flushSync();
    log.info("All records complete.");

    // This kills the child process and shuts down the threads managing it.
    producer.destroy();
    log.info("Finished.");
  }

  private static void executeAtTargetRate(
      final ScheduledExecutorService exec,
      final Runnable task,
      final AtomicLong counter,
      final int durationSeconds,
      final int ratePerSecond) {
    exec.scheduleWithFixedDelay(new Runnable() {
      final long startTime = System.nanoTime();

      @Override
      public void run() {
        double secondsRun = (System.nanoTime() - startTime) / 1e9;
        double targetCount = Math.min(durationSeconds, secondsRun) * ratePerSecond;

        while (counter.get() < targetCount) {
          counter.getAndIncrement();
          try {
            task.run();
          } catch (Exception e) {
            log.error("Error running task", e);
            System.exit(1);
          }
        }

        if (secondsRun >= durationSeconds) {
          exec.shutdown();
        }
      }
    }, 0, 1, TimeUnit.MILLISECONDS);
  }
}
