package com.kai.matchengine.event.inbound;

import com.kai.matchengine.orderbook.Side;

/**
 * 下单命令（对应 match_event 中的 Place）。
 *
 * <p>{@code clientRequestId} 由上游定义，撮合原样回写 {@code event_result} 供关联。
 * <p>{@code priceTick} 对 {@link OrderType#MARKET} 可忽略。
 */
public record PlaceOrder(
    int symbolId,
    long clientRequestId,
    long orderId,
    long userId,
    Side side,
    OrderType orderType,
    long priceTick,
    long qty)
    implements MatchEvent {}
