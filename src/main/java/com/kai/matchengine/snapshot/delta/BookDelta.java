package com.kai.matchengine.snapshot.delta;

import com.kai.matchengine.match.SymbolStatus;
import com.kai.matchengine.orderbook.Side;
import java.util.Objects;

/** 单条簿镜像增量（由撮合线程 append，snapshot 线程 apply）。 */
public record BookDelta(
    long matchOffset,
    int symbolId,
    BookDeltaType type,
    long orderId,
    long userId,
    int side,
    long priceTick,
    long qty,
    long eventId,
    long bookVersion) {

  public static final int SIDE_NONE = -1;

  public static BookDelta restAdd(
      long matchOffset,
      int symbolId,
      long orderId,
      long userId,
      Side side,
      long priceTick,
      long qty,
      long eventId,
      long bookVersion) {
    return new BookDelta(
        matchOffset,
        symbolId,
        BookDeltaType.REST_ADD,
        orderId,
        userId,
        side == Side.BID ? 0 : 1,
        priceTick,
        qty,
        eventId,
        bookVersion);
  }

  public static BookDelta restRemove(
      long matchOffset, int symbolId, long orderId, long eventId, long bookVersion) {
    return new BookDelta(
        matchOffset,
        symbolId,
        BookDeltaType.REST_REMOVE,
        orderId,
        0L,
        SIDE_NONE,
        0L,
        0L,
        eventId,
        bookVersion);
  }

  public static BookDelta restQty(
      long matchOffset,
      int symbolId,
      long orderId,
      long qty,
      long eventId,
      long bookVersion) {
    return new BookDelta(
        matchOffset,
        symbolId,
        BookDeltaType.REST_QTY,
        orderId,
        0L,
        SIDE_NONE,
        0L,
        qty,
        eventId,
        bookVersion);
  }

  public static BookDelta metaRecentOrder(long matchOffset, int symbolId, long orderId) {
    return new BookDelta(
        matchOffset,
        symbolId,
        BookDeltaType.META_RECENT_ORDER,
        orderId,
        0L,
        SIDE_NONE,
        0L,
        0L,
        0L,
        0L);
  }

  public static BookDelta metaSequences(
      long matchOffset, int symbolId, long eventId, long bookVersion) {
    return new BookDelta(
        matchOffset,
        symbolId,
        BookDeltaType.META_SEQUENCES,
        0L,
        0L,
        SIDE_NONE,
        0L,
        0L,
        eventId,
        bookVersion);
  }

  public static BookDelta metaSymbolStatus(long matchOffset, int symbolId, SymbolStatus status) {
    Objects.requireNonNull(status, "status");
    return new BookDelta(
        matchOffset,
        symbolId,
        BookDeltaType.META_SYMBOL_STATUS,
        0L,
        0L,
        status.ordinal(),
        0L,
        0L,
        0L,
        0L);
  }

  public SymbolStatus toSymbolStatus() {
    if (type() != BookDeltaType.META_SYMBOL_STATUS) {
      throw new IllegalStateException("not a symbol status delta");
    }
    SymbolStatus[] values = SymbolStatus.values();
    if (side < 0 || side >= values.length) {
      throw new IllegalStateException("invalid symbol status ordinal: " + side);
    }
    return values[side];
  }

  public Side toSide() {
    if (side == 0) {
      return Side.BID;
    }
    if (side == 1) {
      return Side.ASK;
    }
    return null;
  }
}
