package com.kai.matchengine.ingress.disruptor;

import com.kai.matchengine.event.inbound.MatchEvent;
import com.kai.matchengine.event.outbound.CommandResult;
import com.kai.matchengine.event.outbound.OutboundEvent;
import com.kai.matchengine.event.outbound.Trade;
import com.kai.matchengine.ingress.FatalFailureHandler;
import com.kai.matchengine.match.EngineRegistry;
import com.kai.matchengine.snapshot.delta.MatchDeltaContext;
import com.lmax.disruptor.EventHandler;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Disruptor 单消费者：将入站事件 {@code apply} 到撮合。
 *
 * <p>撮合 / 出站回调失败视为不可恢复：打错误日志并触发 {@link FatalFailureHandler}（默认
 * {@code System.exit}）。
 */
public final class MatchingApplyHandler implements EventHandler<InboundRingEvent> {

  private static final Logger log = LoggerFactory.getLogger(MatchingApplyHandler.class);

  private final EngineRegistry registry;
  private final OutboundListener outboundListener;
  private final FatalFailureHandler fatalFailureHandler;

  public MatchingApplyHandler(EngineRegistry registry) {
    this(registry, OutboundListener.NOOP, FatalFailureHandler.SYSTEM_EXIT);
  }

  public MatchingApplyHandler(EngineRegistry registry, OutboundListener outboundListener) {
    this(registry, outboundListener, FatalFailureHandler.SYSTEM_EXIT);
  }

  public MatchingApplyHandler(
      EngineRegistry registry,
      OutboundListener outboundListener,
      FatalFailureHandler fatalFailureHandler) {
    this.registry = Objects.requireNonNull(registry, "registry");
    this.outboundListener = Objects.requireNonNull(outboundListener, "outboundListener");
    this.fatalFailureHandler =
        Objects.requireNonNull(fatalFailureHandler, "fatalFailureHandler");
  }

  @Override
  public void onEvent(InboundRingEvent event, long sequence, boolean endOfBatch) {
    try {
      if (event.rejected()) {
        outboundListener.onOutbound(0, event.offset(), List.of());
        if (log.isInfoEnabled()) {
          log.info(
              "apply skipped offset={} topic={} partition={} reason=ingress_reject outboundEvents=0",
              event.offset(),
              event.topic(),
              event.partition());
        }
        return;
      }
      MatchDeltaContext.setOffset(event.offset());
      try {
        MatchEvent inbound = event.event();
        List<OutboundEvent> outbound = registry.apply(inbound);
        if (log.isInfoEnabled()) {
          log.info(
              "apply done offset={} symbolId={} inbound={} {} outboundEvents={}",
              event.offset(),
              event.symbolId(),
              inbound.getClass().getSimpleName(),
              summarize(outbound),
              outbound.size());
        }
        outboundListener.onOutbound(event.symbolId(), event.offset(), outbound);
      } finally {
        MatchDeltaContext.clear();
      }
    } catch (Throwable t) {
      String msg =
          "fatal match apply failure symbolId="
              + event.symbolId()
              + " offset="
              + event.offset()
              + " topic="
              + event.topic()
              + " partition="
              + event.partition();
      log.error(msg, t);
      fatalFailureHandler.onFatal(msg, t);
      // 不重抛：避免再进 Disruptor ExceptionHandler 二次 fatal；退出由 fatal handler 负责
    } finally {
      event.clear();
    }
  }

  private static String summarize(List<OutboundEvent> outbound) {
    CommandResult cmd = null;
    int trades = 0;
    for (OutboundEvent e : outbound) {
      if (e instanceof CommandResult cr) {
        cmd = cr;
      } else if (e instanceof Trade) {
        trades++;
      }
    }
    if (cmd == null) {
      return "result=none trades=" + trades;
    }
    return "result="
        + cmd.eventType()
        + " accepted="
        + cmd.accepted()
        + (cmd.accepted() ? "" : " reject=" + cmd.rejectReason())
        + " clientRequestId="
        + cmd.clientRequestId()
        + " symbol="
        + cmd.symbol()
        + " trades="
        + trades;
  }

  /** 撮合产出回调（后续可接 Kafka 出站）。 */
  @FunctionalInterface
  public interface OutboundListener {
    OutboundListener NOOP = (symbolId, offset, events) -> {};

    void onOutbound(int symbolId, long offset, List<OutboundEvent> events);
  }
}
