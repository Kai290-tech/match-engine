package com.kai.matchengine.outbound;

import com.kai.matchengine.event.inbound.InboundEventType;
import com.kai.matchengine.event.outbound.BookSnapshot;
import com.kai.matchengine.event.outbound.BookUpdate;
import com.kai.matchengine.event.outbound.CommandResult;
import com.kai.matchengine.event.outbound.OrderUpdate;
import com.kai.matchengine.event.outbound.OrderUpdateType;
import com.kai.matchengine.event.outbound.OutboundEvent;
import com.kai.matchengine.event.outbound.RejectReason;
import com.kai.matchengine.event.outbound.Trade;
import com.kai.matchengine.match.SymbolStatus;
import com.kai.matchengine.orderbook.Side;
import com.kai.matchengine.sbe.outbound.BookSnapshotEncoder;
import com.kai.matchengine.sbe.outbound.BookUpdateEncoder;
import com.kai.matchengine.sbe.outbound.CommandResultCancelEncoder;
import com.kai.matchengine.sbe.outbound.CommandResultPlaceEncoder;
import com.kai.matchengine.sbe.outbound.CommandResultSymbolEncoder;
import com.kai.matchengine.sbe.outbound.MessageHeaderEncoder;
import com.kai.matchengine.sbe.outbound.OrderUpdateEncoder;
import com.kai.matchengine.sbe.outbound.TradeEncoder;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * 出站 SBE 编码（单线程 {@link OutboundPublisher} 工作线程使用）。
 *
 * <p>复用 buffer 与 encoder；每次 {@link #encode} 返回 value 副本供 Kafka 异步发送。
 */
public final class SbeOutboundEncoder implements OutboundEventEncoder {

  private static final int BUFFER_CAPACITY = 512;

  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final TradeEncoder tradeEncoder = new TradeEncoder();
  private final OrderUpdateEncoder orderUpdateEncoder = new OrderUpdateEncoder();
  private final BookUpdateEncoder bookUpdateEncoder = new BookUpdateEncoder();
  private final CommandResultPlaceEncoder placeResultEncoder = new CommandResultPlaceEncoder();
  private final CommandResultCancelEncoder cancelResultEncoder = new CommandResultCancelEncoder();
  private final CommandResultSymbolEncoder symbolResultEncoder = new CommandResultSymbolEncoder();
  private final BookSnapshotEncoder bookSnapshotEncoder = new BookSnapshotEncoder();
  private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[BUFFER_CAPACITY]);

  @Override
  public String topicFor(OutboundEvent event, OutboundTopics topics) {
    return switch (event) {
      case CommandResult ignored -> topics.eventResult();
      case Trade ignored -> topics.trade();
      case OrderUpdate ignored -> topics.orderUpdate();
      case BookUpdate ignored -> topics.bookUpdate();
      case BookSnapshot ignored -> topics.bookUpdate();
    };
  }

  @Override
  public byte[] encode(OutboundBatch batch, OutboundEvent event) {
    int symbolId = batch.symbolId();
    return switch (event) {
      case Trade t -> encodeTrade(symbolId, t);
      case OrderUpdate u -> encodeOrderUpdate(symbolId, u);
      case BookUpdate b -> encodeBookUpdate(symbolId, b);
      case CommandResult.Place p -> encodePlaceResult(symbolId, p);
      case CommandResult.Cancel c -> encodeCancelResult(symbolId, c);
      case CommandResult.Symbol s -> encodeSymbolResult(symbolId, s);
      case BookSnapshot s -> encodeBookSnapshot(symbolId, s);
    };
  }

  private byte[] encodeTrade(int symbolId, Trade t) {
    tradeEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
    tradeEncoder
        .symbolId(symbolId)
        .eventId(t.eventId())
        .eventTime(t.eventTime())
        .takerOrderId(t.takerOrderId())
        .takerUserId(t.takerUserId())
        .makerOrderId(t.makerOrderId())
        .makerUserId(t.makerUserId())
        .priceTick(t.priceTick())
        .qty(t.qty())
        .takerSide(toSbeSide(t.takerSide()));
    return copyBuffer(tradeEncoder.limit());
  }

  private byte[] encodeOrderUpdate(int symbolId, OrderUpdate u) {
    orderUpdateEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
    orderUpdateEncoder
        .symbolId(symbolId)
        .eventId(u.eventId())
        .eventTime(u.eventTime())
        .orderId(u.orderId())
        .userId(u.userId())
        .side(toSbeSide(u.side()))
        .priceTick(u.priceTick())
        .remainingQty(u.remainingQty())
        .filledDelta(u.filledDelta())
        .updateType(toSbeOrderUpdateType(u.type()));
    return copyBuffer(orderUpdateEncoder.limit());
  }

  private byte[] encodeBookUpdate(int symbolId, BookUpdate b) {
    bookUpdateEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
    bookUpdateEncoder
        .symbolId(symbolId)
        .bookVersion(b.bookVersion())
        .eventTime(b.eventTime())
        .side(toSbeSide(b.side()))
        .priceTick(b.priceTick())
        .levelQty(b.levelQty());
    return copyBuffer(bookUpdateEncoder.limit());
  }

  private byte[] encodePlaceResult(int symbolId, CommandResult.Place p) {
    placeResultEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
    placeResultEncoder
        .symbolId(symbolId)
        .clientRequestId(p.clientRequestId())
        .accepted(p.accepted() ? (short) 1 : (short) 0)
        .rejectReason(toSbeRejectReason(p.rejectReason()))
        .eventTime(p.eventTime())
        .orderId(p.orderId())
        .filledQty(p.filledQty())
        .remainingQty(p.remainingQty())
        .resting(p.resting() ? (short) 1 : (short) 0);
    return copyBuffer(placeResultEncoder.limit());
  }

  private byte[] encodeCancelResult(int symbolId, CommandResult.Cancel c) {
    cancelResultEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
    cancelResultEncoder
        .symbolId(symbolId)
        .clientRequestId(c.clientRequestId())
        .accepted(c.accepted() ? (short) 1 : (short) 0)
        .rejectReason(toSbeRejectReason(c.rejectReason()))
        .eventTime(c.eventTime())
        .orderId(c.orderId());
    return copyBuffer(cancelResultEncoder.limit());
  }

  private byte[] encodeSymbolResult(int symbolId, CommandResult.Symbol s) {
    symbolResultEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
    symbolResultEncoder
        .symbolId(symbolId)
        .clientRequestId(s.clientRequestId())
        .accepted(s.accepted() ? (short) 1 : (short) 0)
        .rejectReason(toSbeRejectReason(s.rejectReason()))
        .eventTime(s.eventTime())
        .eventType(toSbeInboundEventType(s.eventType()))
        .status(toSbeSymbolStatus(s.status()))
        .code(s.symbol() == null ? "" : s.symbol());
    return copyBuffer(symbolResultEncoder.limit());
  }

  private byte[] encodeBookSnapshot(int symbolId, BookSnapshot s) {
    bookSnapshotEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
    bookSnapshotEncoder
        .symbolId(symbolId)
        .bookVersion(s.bookVersion())
        .eventTime(s.eventTime());
    return copyBuffer(bookSnapshotEncoder.limit());
  }

  private byte[] copyBuffer(int length) {
    byte[] out = new byte[length];
    buffer.getBytes(0, out, 0, length);
    return out;
  }

  private static com.kai.matchengine.sbe.outbound.Side toSbeSide(Side side) {
    return switch (side) {
      case BID -> com.kai.matchengine.sbe.outbound.Side.BID;
      case ASK -> com.kai.matchengine.sbe.outbound.Side.ASK;
    };
  }

  private static com.kai.matchengine.sbe.outbound.OrderUpdateType toSbeOrderUpdateType(
      OrderUpdateType type) {
    return com.kai.matchengine.sbe.outbound.OrderUpdateType.valueOf(type.name());
  }

  private static com.kai.matchengine.sbe.outbound.RejectReason toSbeRejectReason(
      RejectReason reason) {
    if (reason == null) {
      return com.kai.matchengine.sbe.outbound.RejectReason.NULL_VAL;
    }
    return com.kai.matchengine.sbe.outbound.RejectReason.valueOf(reason.name());
  }

  private static com.kai.matchengine.sbe.outbound.InboundEventType toSbeInboundEventType(
      InboundEventType type) {
    return com.kai.matchengine.sbe.outbound.InboundEventType.valueOf(type.name());
  }

  private static com.kai.matchengine.sbe.outbound.SymbolStatus toSbeSymbolStatus(
      SymbolStatus status) {
    if (status == null) {
      return com.kai.matchengine.sbe.outbound.SymbolStatus.NULL_VAL;
    }
    return com.kai.matchengine.sbe.outbound.SymbolStatus.valueOf(status.name());
  }
}
