package com.kai.matchengine.event.outbound;

import com.kai.matchengine.event.inbound.InboundEventType;
import com.kai.matchengine.match.SymbolStatus;

/**
 * 单条 {@code match_event} 的处理结果（投影到 {@code event_result}，与输入 1:1）。
 *
 * <p>{@code clientRequestId} 来自对应 match_event，供上游关联。
 * <p>{@code eventTime} 为该命令处理完成时刻（由撮合生成）。
 */
public sealed interface CommandResult extends OutboundEvent {

  InboundEventType eventType();

  boolean accepted();

  RejectReason rejectReason();

  long eventTime();

  String symbol();

  /** 上游 match_event 中的 clientRequestId。 */
  long clientRequestId();

  record Place(
      long clientRequestId,
      boolean accepted,
      RejectReason rejectReason,
      long eventTime,
      String symbol,
      long orderId,
      long filledQty,
      long remainingQty,
      boolean resting)
      implements CommandResult {

    @Override
    public InboundEventType eventType() {
      return InboundEventType.PLACE_ORDER;
    }

    public static Place reject(
        long clientRequestId,
        long eventTime,
        String symbol,
        long orderId,
        RejectReason reason) {
      return new Place(
          clientRequestId, false, reason, eventTime, symbol, orderId, 0L, 0L, false);
    }

    public static Place ok(
        long clientRequestId,
        long eventTime,
        String symbol,
        long orderId,
        long filledQty,
        long remainingQty,
        boolean resting) {
      return new Place(
          clientRequestId,
          true,
          null,
          eventTime,
          symbol,
          orderId,
          filledQty,
          remainingQty,
          resting);
    }
  }

  record Cancel(
      long clientRequestId,
      boolean accepted,
      RejectReason rejectReason,
      long eventTime,
      String symbol,
      long orderId)
      implements CommandResult {

    @Override
    public InboundEventType eventType() {
      return InboundEventType.CANCEL_ORDER;
    }

    public static Cancel reject(
        long clientRequestId,
        long eventTime,
        String symbol,
        long orderId,
        RejectReason reason) {
      return new Cancel(clientRequestId, false, reason, eventTime, symbol, orderId);
    }

    public static Cancel ok(long clientRequestId, long eventTime, String symbol, long orderId) {
      return new Cancel(clientRequestId, true, null, eventTime, symbol, orderId);
    }
  }

  /** Create / Halt / Resume 的统一回执。 */
  record Symbol(
      long clientRequestId,
      boolean accepted,
      RejectReason rejectReason,
      long eventTime,
      String symbol,
      int symbolId,
      InboundEventType eventType,
      SymbolStatus status)
      implements CommandResult {

    public static Symbol reject(
        InboundEventType type,
        long clientRequestId,
        long eventTime,
        String symbol,
        int symbolId,
        RejectReason reason) {
      return new Symbol(
          clientRequestId, false, reason, eventTime, symbol, symbolId, type, null);
    }

    public static Symbol ok(
        InboundEventType type,
        long clientRequestId,
        long eventTime,
        String symbol,
        int symbolId,
        SymbolStatus status) {
      return new Symbol(
          clientRequestId, true, null, eventTime, symbol, symbolId, type, status);
    }
  }
}
