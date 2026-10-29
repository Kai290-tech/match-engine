package com.kai.matchengine.event.inbound;

/**
 * 更新交易对 code（簿必须为空；保留 eventId/bookVersion）。
 */
public record UpdateSymbol(int symbolId, long clientRequestId, String code) implements MatchEvent {}
