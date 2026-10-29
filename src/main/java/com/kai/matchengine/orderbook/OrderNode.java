package com.kai.matchengine.orderbook;

/**
 * 订单簿内的订单节点（侵入式双向链表节点）。
 *
 * <p>单线程撮合场景下由 {@link OrderBook} 持有并修改；支持 {@link OrderNodePool} 复用。
 */
public final class OrderNode {

  private long orderId;
  private long userId;
  private Side side;
  private long priceTick;
  private long qty;

  OrderNode prev;
  OrderNode next;
  PriceLevel level;

  OrderNode(long orderId, long userId, Side side, long priceTick, long qty) {
    reinit(orderId, userId, side, priceTick, qty);
  }

  void reinit(long orderId, long userId, Side side, long priceTick, long qty) {
    this.orderId = orderId;
    this.userId = userId;
    this.side = side;
    this.priceTick = priceTick;
    this.qty = qty;
    clearLinks();
  }

  void clearLinks() {
    prev = null;
    next = null;
    level = null;
  }

  public long orderId() {
    return orderId;
  }

  public long userId() {
    return userId;
  }

  public Side side() {
    return side;
  }

  public long priceTick() {
    return priceTick;
  }

  public long qty() {
    return qty;
  }

  void setQty(long qty) {
    this.qty = qty;
  }

  boolean isLinked() {
    return level != null;
  }
}
