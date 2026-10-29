package com.kai.matchengine.outbound;

import com.kai.matchengine.event.outbound.OutboundEvent;
import com.kai.matchengine.ingress.FatalFailureHandler;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 单线程出站：一次只处理一批；批内全部异步发送成功后推进 {@link OutboundWatermark}。
 *
 * <p>失败由 {@link AsyncRecordSender} 重试；最终仍失败 → fatal。
 */
public final class OutboundPublisher implements Runnable, AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(OutboundPublisher.class);

  private final BlockingQueue<OutboundBatch> queue;
  private final int queueCapacity;
  private final OutboundWatermark watermark;
  private final AsyncRecordSender sender;
  private final OutboundTopics topics;
  private final OutboundEventEncoder encoder;
  private final BackpressureMonitor backpressureMonitor;
  private final FatalFailureHandler fatalFailureHandler;
  private final long sendTimeoutMs;

  private final AtomicBoolean running = new AtomicBoolean(false);
  private volatile Thread worker;

  public OutboundPublisher(
      int queueCapacity,
      OutboundWatermark watermark,
      AsyncRecordSender sender,
      OutboundTopics topics,
      OutboundEventEncoder encoder,
      FatalFailureHandler fatalFailureHandler,
      long sendTimeoutMs) {
    this(
        queueCapacity,
        watermark,
        sender,
        topics,
        encoder,
        BackpressureMonitor.NOOP,
        fatalFailureHandler,
        sendTimeoutMs);
  }

  public OutboundPublisher(
      int queueCapacity,
      OutboundWatermark watermark,
      AsyncRecordSender sender,
      OutboundTopics topics,
      OutboundEventEncoder encoder,
      BackpressureMonitor backpressureMonitor,
      FatalFailureHandler fatalFailureHandler,
      long sendTimeoutMs) {
    if (queueCapacity < 1) {
      throw new IllegalArgumentException("queueCapacity must be >= 1");
    }
    this.queueCapacity = queueCapacity;
    this.queue = new ArrayBlockingQueue<>(queueCapacity);
    this.watermark = Objects.requireNonNull(watermark, "watermark");
    this.sender = Objects.requireNonNull(sender, "sender");
    this.topics = Objects.requireNonNull(topics, "topics");
    this.encoder = Objects.requireNonNull(encoder, "encoder");
    this.backpressureMonitor =
        Objects.requireNonNull(backpressureMonitor, "backpressureMonitor");
    this.fatalFailureHandler = Objects.requireNonNull(fatalFailureHandler, "fatalFailureHandler");
    this.sendTimeoutMs = sendTimeoutMs;
  }

  /** 撮合线程调用：入队；队列满则阻塞等待。 */
  public void enqueue(OutboundBatch batch) throws InterruptedException {
    Objects.requireNonNull(batch, "batch");
    backpressureMonitor.onEnqueueDepth(
        queue.size(), queueCapacity, batch.matchOffset(), batch.symbolId());
    queue.put(batch);
  }

  public void enqueueUninterruptibly(OutboundBatch batch) {
    Objects.requireNonNull(batch, "batch");
    while (true) {
      try {
        backpressureMonitor.onEnqueueDepth(
            queue.size(), queueCapacity, batch.matchOffset(), batch.symbolId());
        queue.put(batch);
        return;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        String msg = "interrupted while enqueue outbound offset=" + batch.matchOffset();
        log.error(msg, e);
        fatalFailureHandler.onFatal(msg, e);
        throw new IllegalStateException(msg, e);
      }
    }
  }

  public OutboundWatermark watermark() {
    return watermark;
  }

  public synchronized void start() {
    if (running.get()) {
      return;
    }
    running.set(true);
    worker = new Thread(this, "outbound-publisher");
    worker.setDaemon(true);
    worker.start();
  }

  @Override
  public void run() {
    try {
      while (running.get()) {
        OutboundBatch batch = queue.poll(200, TimeUnit.MILLISECONDS);
        if (batch == null) {
          continue;
        }
        publishBatch(batch);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (Throwable t) {
      String msg = "fatal outbound publisher failure";
      log.error(msg, t);
      fatalFailureHandler.onFatal(msg, t);
    } finally {
      running.set(false);
    }
  }

  private void publishBatch(OutboundBatch batch) throws InterruptedException {
    if (batch.events().isEmpty()) {
      watermark.advanceTo(batch.matchOffset());
      log.debug(
          "outbound empty-batch ack offset={} (ingress reject / no events)", batch.matchOffset());
      return;
    }
    if (log.isDebugEnabled()) {
      log.debug(
          "outbound send start offset={} symbolId={} events={}",
          batch.matchOffset(),
          batch.symbolId(),
          batch.events().size());
    }
    CountDownLatch done = new CountDownLatch(batch.events().size());
    java.util.concurrent.atomic.AtomicReference<Throwable> firstError =
        new java.util.concurrent.atomic.AtomicReference<>();

    for (OutboundEvent event : batch.events()) {
      String topic = encoder.topicFor(event, topics);
      byte[] key = kafkaKey(batch, event);
      byte[] value = encoder.encode(batch, event);
      if (log.isTraceEnabled()) {
        log.trace(
            "outbound record offset={} topic={} key={} type={} bytes={}",
            batch.matchOffset(),
            topic,
            new String(key, StandardCharsets.UTF_8),
            event.getClass().getSimpleName(),
            value.length);
      }
      sender.sendAsync(
          topic,
          key,
          value,
          (success, error) -> {
            if (!success) {
              firstError.compareAndSet(null, error == null ? new IllegalStateException("send failed") : error);
            }
            done.countDown();
          });
    }

    boolean finished = done.await(sendTimeoutMs, TimeUnit.MILLISECONDS);
    if (!finished) {
      String msg =
          "outbound batch send timeout offset="
              + batch.matchOffset()
              + " size="
              + batch.events().size();
      log.error(msg);
      fatalFailureHandler.onFatal(msg, new IllegalStateException(msg));
      running.set(false);
      return;
    }
    Throwable err = firstError.get();
    if (err != null) {
      String msg = "outbound batch send failed offset=" + batch.matchOffset();
      log.error(msg, err);
      fatalFailureHandler.onFatal(msg, err);
      running.set(false);
      return;
    }
    watermark.advanceTo(batch.matchOffset());
    log.info(
        "outbound ACK offset={} symbolId={} events={} ackedWatermark={}",
        batch.matchOffset(),
        batch.symbolId(),
        batch.events().size(),
        watermark.ackedOffset());
  }

  /**
   * 交易对级有序：出站统一用 {@code symbolId} 作 Kafka key（UTF-8 十进制），避免 Update/展示 code 变更导致换分区。
   */
  static byte[] kafkaKey(OutboundBatch batch, OutboundEvent event) {
    Objects.requireNonNull(event, "event");
    return Integer.toString(batch.symbolId()).getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public synchronized void close() {
    running.set(false);
    Thread t = worker;
    if (t != null) {
      t.interrupt();
      try {
        t.join(5_000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
