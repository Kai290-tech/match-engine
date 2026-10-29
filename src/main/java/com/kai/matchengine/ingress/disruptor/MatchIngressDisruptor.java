package com.kai.matchengine.ingress.disruptor;

import com.kai.matchengine.ingress.FatalFailureHandler;
import com.kai.matchengine.ingress.ValidatedInboundHandler;
import com.kai.matchengine.ingress.disruptor.MatchingApplyHandler.OutboundListener;
import com.kai.matchengine.match.EngineRegistry;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 入站 Disruptor 管道：单生产者（ingress）+ 单消费者（撮合 apply）。
 *
 * <p>合法与 ingress 拒绝的 offset 均经 publisher 入环；环满时 publisher 等待并 warn。
 * 恢复水位只看快照在撮合完成后写入的 offset。
 */
public final class MatchIngressDisruptor implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(MatchIngressDisruptor.class);

  public enum WaitMode {
    BLOCKING,
    YIELDING,
    SLEEPING,
    BUSY_SPIN
  }

  private final Disruptor<InboundRingEvent> disruptor;
  private final DisruptorInboundPublisher publisher;
  private boolean started;

  public MatchIngressDisruptor(EngineRegistry registry) {
    this(
        registry,
        1024 * 64,
        WaitMode.BLOCKING,
        null,
        OutboundListener.NOOP,
        FatalFailureHandler.SYSTEM_EXIT);
  }

  public MatchIngressDisruptor(
      EngineRegistry registry, int ringSize, WaitMode waitMode, ThreadFactory threadFactory) {
    this(
        registry,
        ringSize,
        waitMode,
        threadFactory,
        OutboundListener.NOOP,
        FatalFailureHandler.SYSTEM_EXIT);
  }

  public MatchIngressDisruptor(
      EngineRegistry registry,
      int ringSize,
      WaitMode waitMode,
      ThreadFactory threadFactory,
      OutboundListener outboundListener) {
    this(
        registry,
        ringSize,
        waitMode,
        threadFactory,
        outboundListener,
        FatalFailureHandler.SYSTEM_EXIT);
  }

  public MatchIngressDisruptor(
      EngineRegistry registry,
      int ringSize,
      WaitMode waitMode,
      ThreadFactory threadFactory,
      OutboundListener outboundListener,
      FatalFailureHandler fatalFailureHandler) {
    Objects.requireNonNull(registry, "registry");
    Objects.requireNonNull(fatalFailureHandler, "fatalFailureHandler");
    if (Integer.bitCount(ringSize) != 1) {
      throw new IllegalArgumentException("ringSize must be power of 2");
    }
    MatchingApplyHandler applyHandler =
        new MatchingApplyHandler(registry, outboundListener, fatalFailureHandler);
    this.disruptor =
        new Disruptor<>(
            new InboundRingEventFactory(),
            ringSize,
            threadFactory == null ? DaemonThreadFactory.INSTANCE : threadFactory,
            ProducerType.SINGLE,
            waitStrategy(waitMode));
    this.disruptor.setDefaultExceptionHandler(fatalExceptionHandler(fatalFailureHandler));
    this.disruptor.handleEventsWith(applyHandler);
    this.publisher =
        new DisruptorInboundPublisher(disruptor.getRingBuffer(), fatalFailureHandler);
  }

  public IngressDisruptorPublisher publisher() {
    return publisher;
  }

  public synchronized void start() {
    if (started) {
      return;
    }
    disruptor.start();
    started = true;
  }

  @Override
  public synchronized void close() {
    if (!started) {
      return;
    }
    disruptor.shutdown();
    started = false;
  }

  private static ExceptionHandler<InboundRingEvent> fatalExceptionHandler(
      FatalFailureHandler fatal) {
    return new ExceptionHandler<>() {
      @Override
      public void handleEventException(Throwable ex, long sequence, InboundRingEvent event) {
        String msg =
            "fatal disruptor event handler failure sequence="
                + sequence
                + " offset="
                + (event == null ? -1L : event.offset());
        log.error(msg, ex);
        fatal.onFatal(msg, ex);
      }

      @Override
      public void handleOnStartException(Throwable ex) {
        String msg = "fatal disruptor consumer start failure";
        log.error(msg, ex);
        fatal.onFatal(msg, ex);
      }

      @Override
      public void handleOnShutdownException(Throwable ex) {
        log.warn("disruptor consumer shutdown error", ex);
      }
    };
  }

  private static WaitStrategy waitStrategy(WaitMode mode) {
    return switch (mode == null ? WaitMode.BLOCKING : mode) {
      case BLOCKING -> new BlockingWaitStrategy();
      case YIELDING -> new YieldingWaitStrategy();
      case SLEEPING -> new SleepingWaitStrategy();
      case BUSY_SPIN -> new BusySpinWaitStrategy();
    };
  }
}
