package com.kai.matchengine.event.outbound;

import com.kai.matchengine.orderbook.Side;

/**
 * 成交（投影到 Trade topic）。
 *
 * <p>{@code eventTime} 由撮合在该笔成交生效时生成。
 */
public record Trade(
    long eventId,
    long eventTime,
    String symbol,
    long takerOrderId,
    long takerUserId,
    long makerOrderId,
    long makerUserId,
    long priceTick,
    long qty,
    Side takerSide)
    implements OutboundEvent {}
