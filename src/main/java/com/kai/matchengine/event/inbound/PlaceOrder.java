package com.kai.matchengine.event.inbound;

import com.kai.matchengine.orderbook.Side;

/**
 * 下单命令（对应 match_event 中的 Place）。
 *
 * <p>{@code clientRequestId} 由上游定义，撮合原样回写 {@code event_result} 供关联。
 * <p>{@code priceTick} 对 {@link OrderType#MARKET} 可忽略；{@link OrderType#LIMIT}、{@link OrderType#IOC}、{@link OrderType#FOK} 与 {@link OrderType#POST_ONLY} 必须大于 0。
 * <p>省略 {@code qtyUnit} 时视为 {@link QtyUnit#BASE}。市价买单可以是 {@link QtyUnit#QUOTE}，表示计价预算。
 */
public record PlaceOrder(
    int symbolId,
    long clientRequestId,
    long orderId,
    long userId,
    Side side,
    OrderType orderType,
    long priceTick,
    long qty,
    QtyUnit qtyUnit)
    implements MatchEvent {

  /** 数量单位为基础资产。限价、IOC、FOK 与市价卖单使用这个构造。 */
  public PlaceOrder(
      int symbolId,
      long clientRequestId,
      long orderId,
      long userId,
      Side side,
      OrderType orderType,
      long priceTick,
      long qty) {
    this(symbolId, clientRequestId, orderId, userId, side, orderType, priceTick, qty, QtyUnit.BASE);
  }

  public PlaceOrder {
    if (qtyUnit == null) {
      throw new NullPointerException("qtyUnit");
    }
  }
}
