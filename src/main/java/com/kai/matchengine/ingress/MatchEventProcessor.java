package com.kai.matchengine.ingress;

import com.kai.matchengine.event.inbound.CancelOrder;
import com.kai.matchengine.event.inbound.CreateSymbol;
import com.kai.matchengine.event.inbound.HaltSymbol;
import com.kai.matchengine.event.inbound.MatchEvent;
import com.kai.matchengine.event.inbound.PlaceOrder;
import com.kai.matchengine.event.inbound.ResumeSymbol;
import com.kai.matchengine.event.inbound.UpdateSymbol;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 反序列化 + 基础校验。仅合法消息交给 {@link ValidatedInboundHandler}。
 *
 * <p>非法消息不进撮合，但会回调 {@link RejectedOffsetHandler}，由下游按序推进 outbound watermark。
 */
public final class MatchEventProcessor {

  private static final Logger log = LoggerFactory.getLogger(MatchEventProcessor.class);

  private final SbeMatchEventCodec codec;
  private final IngressValidator validator;
  private final ValidatedInboundHandler handler;
  private final FatalFailureHandler fatalFailureHandler;
  private final RejectedOffsetHandler rejectedOffsetHandler;

  private final LongAdder accepted = new LongAdder();
  private final LongAdder rejected = new LongAdder();

  public MatchEventProcessor(
      SbeMatchEventCodec codec, IngressValidator validator, ValidatedInboundHandler handler) {
    this(codec, validator, handler, FatalFailureHandler.SYSTEM_EXIT, RejectedOffsetHandler.NOOP);
  }

  public MatchEventProcessor(
      SbeMatchEventCodec codec,
      IngressValidator validator,
      ValidatedInboundHandler handler,
      FatalFailureHandler fatalFailureHandler) {
    this(codec, validator, handler, fatalFailureHandler, RejectedOffsetHandler.NOOP);
  }

  public MatchEventProcessor(
      SbeMatchEventCodec codec,
      IngressValidator validator,
      ValidatedInboundHandler handler,
      FatalFailureHandler fatalFailureHandler,
      RejectedOffsetHandler rejectedOffsetHandler) {
    this.codec = Objects.requireNonNull(codec, "codec");
    this.validator = Objects.requireNonNull(validator, "validator");
    this.handler = Objects.requireNonNull(handler, "handler");
    this.fatalFailureHandler =
        Objects.requireNonNull(fatalFailureHandler, "fatalFailureHandler");
    this.rejectedOffsetHandler =
        Objects.requireNonNull(rejectedOffsetHandler, "rejectedOffsetHandler");
  }

  /**
   * @return true 表示已接受并成功交给下游；false 表示非法未进撮合（已通知 rejectedOffsetHandler）
   */
  public boolean process(byte[] payload, String topic, int partition, long offset) {
    final MatchEvent event;
    try {
      event = codec.decode(payload);
      validator.validate(event);
    } catch (IngressException e) {
      rejected.increment();
      log.info(
          "ingress REJECT reason={} offset={} topic={} partition={} msg={}",
          e.reason(),
          offset,
          topic,
          partition,
          e.getMessage());
      rejectedOffsetHandler.onRejected(topic, partition, offset);
      return false;
    }

    int symbolId =
        switch (event) {
          case PlaceOrder p -> p.symbolId();
          case CancelOrder c -> c.symbolId();
          case CreateSymbol c -> c.symbolId();
          case UpdateSymbol u -> u.symbolId();
          case HaltSymbol h -> h.symbolId();
          case ResumeSymbol r -> r.symbolId();
        };
    log.debug(
        "ingress ACCEPT type={} symbolId={} offset={} event={}",
        event.getClass().getSimpleName(),
        symbolId,
        offset,
        event);
    ValidatedInbound inbound = new ValidatedInbound(event, symbolId, topic, partition, offset);
    try {
      handler.onValidated(inbound);
      accepted.increment();
      return true;
    } catch (Throwable t) {
      String msg =
          "fatal ingress handler failure topic="
              + topic
              + " partition="
              + partition
              + " offset="
              + offset
              + " symbolId="
              + symbolId;
      log.error(msg, t);
      fatalFailureHandler.onFatal(msg, t);
      throw new IllegalStateException(msg, t);
    }
  }

  public long acceptedCount() {
    return accepted.sum();
  }

  public long rejectedCount() {
    return rejected.sum();
  }
}
