package com.kai.matchengine.outbound;

import com.kai.matchengine.event.outbound.BookSnapshot;
import com.kai.matchengine.event.outbound.BookUpdate;
import com.kai.matchengine.event.outbound.CommandResult;
import com.kai.matchengine.event.outbound.OrderUpdate;
import com.kai.matchengine.event.outbound.OutboundEvent;
import com.kai.matchengine.event.outbound.Trade;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

/** 出站事件 → UTF-8 JSON（过渡格式；默认请用 {@link SbeOutboundEncoder}）。 */
public final class OutboundJsonEncoder implements OutboundEventEncoder {

  @Override
  public String topicFor(OutboundEvent event, OutboundTopics topics) {
    return switch (event) {
      case CommandResult ignored -> topics.eventResult();
      case Trade ignored -> topics.trade();
      case OrderUpdate ignored -> topics.orderUpdate();
      case BookUpdate ignored -> topics.bookUpdate();
      case BookSnapshot ignored -> topics.bookUpdate(); // 全量簿暂走 book 通道；引擎状态快照另走 MySQL
    };
  }

  @Override
  public byte[] encode(OutboundBatch batch, OutboundEvent event) {
    return toJson(event).getBytes(StandardCharsets.UTF_8);
  }

  public byte[] encodeLegacy(OutboundEvent event) {
    return toJson(event).getBytes(StandardCharsets.UTF_8);
  }

  String toJson(OutboundEvent event) {
    return switch (event) {
      case CommandResult.Place p -> placeJson(p);
      case CommandResult.Cancel c -> cancelJson(c);
      case CommandResult.Symbol s -> symbolJson(s);
      case Trade t -> tradeJson(t);
      case OrderUpdate u -> orderUpdateJson(u);
      case BookUpdate b -> bookUpdateJson(b);
      case BookSnapshot s -> bookSnapshotJson(s);
    };
  }

  private static String placeJson(CommandResult.Place p) {
    return "{"
        + kv("type", "PLACE")
        + ","
        + kv("clientRequestId", p.clientRequestId())
        + ","
        + kv("accepted", p.accepted())
        + ","
        + kv("rejectReason", p.rejectReason() == null ? null : p.rejectReason().name())
        + ","
        + kv("eventTime", p.eventTime())
        + ","
        + kv("symbol", p.symbol())
        + ","
        + kv("orderId", p.orderId())
        + ","
        + kv("filledQty", p.filledQty())
        + ","
        + kv("remainingQty", p.remainingQty())
        + ","
        + kv("resting", p.resting())
        + "}";
  }

  private static String cancelJson(CommandResult.Cancel c) {
    return "{"
        + kv("type", "CANCEL")
        + ","
        + kv("clientRequestId", c.clientRequestId())
        + ","
        + kv("accepted", c.accepted())
        + ","
        + kv("rejectReason", c.rejectReason() == null ? null : c.rejectReason().name())
        + ","
        + kv("eventTime", c.eventTime())
        + ","
        + kv("symbol", c.symbol())
        + ","
        + kv("orderId", c.orderId())
        + "}";
  }

  private static String symbolJson(CommandResult.Symbol s) {
    return "{"
        + kv("type", s.eventType().name())
        + ","
        + kv("clientRequestId", s.clientRequestId())
        + ","
        + kv("accepted", s.accepted())
        + ","
        + kv("rejectReason", s.rejectReason() == null ? null : s.rejectReason().name())
        + ","
        + kv("eventTime", s.eventTime())
        + ","
        + kv("symbol", s.symbol())
        + ","
        + kv("symbolId", s.symbolId())
        + ","
        + kv("status", s.status() == null ? null : s.status().name())
        + "}";
  }

  private static String tradeJson(Trade t) {
    return "{"
        + kv("type", "TRADE")
        + ","
        + kv("eventId", t.eventId())
        + ","
        + kv("eventTime", t.eventTime())
        + ","
        + kv("symbol", t.symbol())
        + ","
        + kv("takerOrderId", t.takerOrderId())
        + ","
        + kv("takerUserId", t.takerUserId())
        + ","
        + kv("makerOrderId", t.makerOrderId())
        + ","
        + kv("makerUserId", t.makerUserId())
        + ","
        + kv("priceTick", t.priceTick())
        + ","
        + kv("qty", t.qty())
        + ","
        + kv("takerSide", t.takerSide().name())
        + "}";
  }

  private static String orderUpdateJson(OrderUpdate u) {
    return "{"
        + kv("type", "ORDER_UPDATE")
        + ","
        + kv("eventId", u.eventId())
        + ","
        + kv("eventTime", u.eventTime())
        + ","
        + kv("symbol", u.symbol())
        + ","
        + kv("orderId", u.orderId())
        + ","
        + kv("userId", u.userId())
        + ","
        + kv("side", u.side().name())
        + ","
        + kv("priceTick", u.priceTick())
        + ","
        + kv("remainingQty", u.remainingQty())
        + ","
        + kv("filledDelta", u.filledDelta())
        + ","
        + kv("updateType", u.type().name())
        + "}";
  }

  private static String bookUpdateJson(BookUpdate b) {
    return "{"
        + kv("type", "BOOK_UPDATE")
        + ","
        + kv("bookVersion", b.bookVersion())
        + ","
        + kv("eventTime", b.eventTime())
        + ","
        + kv("symbol", b.symbol())
        + ","
        + kv("side", b.side().name())
        + ","
        + kv("priceTick", b.priceTick())
        + ","
        + kv("levelQty", b.levelQty())
        + "}";
  }

  private static String bookSnapshotJson(BookSnapshot s) {
    return "{"
        + kv("type", "BOOK_SNAPSHOT")
        + ","
        + kv("bookVersion", s.bookVersion())
        + ","
        + kv("eventTime", s.eventTime())
        + ","
        + kv("symbol", s.symbol())
        + "}";
  }

  private static String kv(String key, String value) {
    if (value == null) {
      return "\"" + key + "\":null";
    }
    return "\"" + key + "\":\"" + escape(value) + "\"";
  }

  private static String kv(String key, long value) {
    return "\"" + key + "\":" + value;
  }

  private static String kv(String key, boolean value) {
    return "\"" + key + "\":" + value;
  }

  private static String escape(String s) {
    return s.replace("\\", "\\\\").replace("\"", "\\\"");
  }
}
