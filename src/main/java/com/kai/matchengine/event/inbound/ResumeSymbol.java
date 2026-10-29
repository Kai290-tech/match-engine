package com.kai.matchengine.event.inbound;

/**
 * 复牌：从 HALTED → TRADING。
 */
public record ResumeSymbol(int symbolId, long clientRequestId) implements MatchEvent {}
