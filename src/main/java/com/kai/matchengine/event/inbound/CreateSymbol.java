package com.kai.matchengine.event.inbound;

/**
 * 创建交易对（对应 match_event CreateSymbol）。
 *
 * <p>成功后 status=TRADING，可下单；{@code code} 用于出站 symbol 字段。
 */
public record CreateSymbol(int symbolId, long clientRequestId, String code) implements MatchEvent {}
