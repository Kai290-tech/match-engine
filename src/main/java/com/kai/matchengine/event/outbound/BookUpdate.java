package com.kai.matchengine.event.outbound;

import com.kai.matchengine.orderbook.Side;

/**
 * 订单簿增量（投影到行情通道）。
 *
 * <p>表示某一价位在变更后的绝对剩余量；{@code levelQty == 0} 表示该档已消失。
 * {@code eventTime} 与引起本次簿变更的原子动作共用同一撮合时间。
 */
public record BookUpdate(
    long bookVersion,
    long eventTime,
    String symbol,
    Side side,
    long priceTick,
    long levelQty)
    implements OutboundEvent {}
