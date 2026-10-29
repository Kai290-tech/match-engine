package com.kai.matchengine.event.inbound;

/**
 * 停牌：拒新单，允许撤单。
 */
public record HaltSymbol(int symbolId, long clientRequestId) implements MatchEvent {}
