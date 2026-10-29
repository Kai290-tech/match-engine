package com.kai.matchengine.event.outbound;

import java.util.List;

/**
 * 全量订单簿快照（投影到行情全量通道 / 断档对齐）。
 *
 * <p>不推进 {@code bookVersion}；携带当前版本。买卖盘均为最优价在前。
 */
public record BookSnapshot(
    long bookVersion,
    long eventTime,
    String symbol,
    List<Level> bids,
    List<Level> asks)
    implements OutboundEvent {

  /** 单档绝对量。 */
  public record Level(long priceTick, long qty) {}
}
