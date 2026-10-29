package com.kai.matchengine.event.outbound;

import com.kai.matchengine.orderbook.Side;

/**
 * 订单变更（投影到 orderUpdate topic）。
 *
 * <p>{@code eventTime} 由撮合在变更生效时生成。
 */
public record OrderUpdate(
    long eventId,
    long eventTime,
    String symbol,
    long orderId,
    long userId,
    Side side,
    long priceTick,
    long remainingQty,
    long filledDelta,
    OrderUpdateType type)
    implements OutboundEvent {}
