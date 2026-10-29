package com.kai.matchengine.ingress.disruptor;

import com.kai.matchengine.ingress.FatalFailureHandler;
import com.kai.matchengine.ingress.ValidatedInbound;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import java.util.Objects;
import java.util.concurrent.locks.LockSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 校验通过或 ingress 拒绝的 offset 均写入 Disruptor（Kafka/ingress 线程调用）。
 *
 * <p>环满：等待并打 warn（限频）；等待中被中断或其它异常：fatal（默认 {@code System.exit}+{@code halt}）。
 */
public final class DisruptorInboundPublisher implements IngressDisruptorPublisher {

  private static final Logger log = LoggerFactory.getLogger(DisruptorInboundPublisher.class);

  private static final long RING_FULL_PARK_NS = 1_000_000L; // 1ms
  private static final long RING_FULL_LOG_INTERVAL_NS = 1_000_000_000L; // 1s

  private final RingBuffer<InboundRingEvent> ringBuffer;
  private final FatalFailureHandler fatalFailureHandler;
  private volatile long lastRingFullLogNs;

  public DisruptorInboundPublisher(RingBuffer<InboundRingEvent> ringBuffer) {
    this(ringBuffer, FatalFailureHandler.SYSTEM_EXIT);
  }

  public DisruptorInboundPublisher(
      RingBuffer<InboundRingEvent> ringBuffer, FatalFailureHandler fatalFailureHandler) {
    this.ringBuffer = Objects.requireNonNull(ringBuffer, "ringBuffer");
    this.fatalFailureHandler =
        Objects.requireNonNull(fatalFailureHandler, "fatalFailureHandler");
  }

  @Override
  public void onValidated(ValidatedInbound inbound) {
    publish(inbound, null);
  }

  @Override
  public void onRejected(String topic, int partition, long offset) {
    publish(null, new RejectedSlot(topic, partition, offset));
  }

  private void publish(ValidatedInbound inbound, RejectedSlot rejected) {
    while (true) {
      try {
        long sequence = ringBuffer.tryNext();
        try {
          InboundRingEvent slot = ringBuffer.get(sequence);
          if (rejected != null) {
            slot.loadRejected(rejected.topic, rejected.partition, rejected.offset);
          } else {
            slot.load(inbound);
          }
        } finally {
          ringBuffer.publish(sequence);
        }
        return;
      } catch (InsufficientCapacityException full) {
        if (Thread.currentThread().isInterrupted()) {
          fatalInterrupted(inbound, rejected);
        }
        onRingFull();
        LockSupport.parkNanos(RING_FULL_PARK_NS);
        if (Thread.currentThread().isInterrupted()) {
          fatalInterrupted(inbound, rejected);
        }
      } catch (Throwable t) {
        fatalPublish(inbound, rejected, t);
      }
    }
  }

  private record RejectedSlot(String topic, int partition, long offset) {}

  private void fatalInterrupted(ValidatedInbound inbound, RejectedSlot rejected) {
    InterruptedException cause =
        new InterruptedException("interrupted while waiting for disruptor ring capacity");
    fatalPublish(inbound, rejected, cause);
  }

  private void fatalPublish(ValidatedInbound inbound, RejectedSlot rejected, Throwable t) {
    long offset = inbound != null ? inbound.offset() : rejected.offset();
    int symbolId = inbound != null ? inbound.symbolId() : 0;
    String topic = inbound != null ? inbound.topic() : rejected.topic();
    int partition = inbound != null ? inbound.partition() : rejected.partition();
    String msg =
        "fatal disruptor publish failure symbolId="
            + symbolId
            + " offset="
            + offset
            + " topic="
            + topic
            + " partition="
            + partition
            + (rejected != null ? " rejected=true" : "");
    log.error(msg, t);
    fatalFailureHandler.onFatal(msg, t);
    throw new IllegalStateException(msg, t);
  }

  private void onRingFull() {
    long now = System.nanoTime();
    long prev = lastRingFullLogNs;
    if (now - prev >= RING_FULL_LOG_INTERVAL_NS && compareAndSetLastRingFullLog(prev, now)) {
      log.warn(
          "disruptor ring full, waiting to publish (remainingCapacity={})",
          ringBuffer.remainingCapacity());
    }
  }

  private boolean compareAndSetLastRingFullLog(long expect, long update) {
    if (lastRingFullLogNs != expect) {
      return false;
    }
    lastRingFullLogNs = update;
    return true;
  }
}
