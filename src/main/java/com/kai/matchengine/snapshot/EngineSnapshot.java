package com.kai.matchengine.snapshot;

import com.kai.matchengine.match.SymbolStatus;
import com.kai.matchengine.orderbook.Side;
import java.util.List;
import java.util.Objects;

/** 主线程冻住的引擎快照（不可变）。 */
public record EngineSnapshot(
    int schemaVersion,
    String matchTopic,
    int matchPartition,
    long lastFullyProcessedOffset,
    long preparedAtMillis,
    List<SymbolState> symbols) {

  public static final int SCHEMA_VERSION = 3;

  public EngineSnapshot {
    Objects.requireNonNull(matchTopic, "matchTopic");
    Objects.requireNonNull(symbols, "symbols");
    symbols = List.copyOf(symbols);
  }

  public record SymbolState(
      int symbolId,
      String code,
      SymbolStatus status,
      long eventId,
      long bookVersion,
      List<RestingOrderState> orders,
      List<Long> recentOrderIds) {
    public SymbolState {
      Objects.requireNonNull(code, "code");
      Objects.requireNonNull(status, "status");
      Objects.requireNonNull(orders, "orders");
      Objects.requireNonNull(recentOrderIds, "recentOrderIds");
      orders = List.copyOf(orders);
      recentOrderIds = List.copyOf(recentOrderIds);
    }
  }

  /** side: 0=BID 1=ASK；列表顺序 = FIFO。 */
  public record RestingOrderState(
      long orderId, long userId, int side, long priceTick, long qty) {
    public static RestingOrderState of(
        long orderId, long userId, Side side, long priceTick, long qty) {
      return new RestingOrderState(
          orderId, userId, side == Side.BID ? 0 : 1, priceTick, qty);
    }

    public Side toSide() {
      return side == 0 ? Side.BID : Side.ASK;
    }
  }
}
