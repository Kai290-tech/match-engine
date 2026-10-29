package com.kai.matchengine.event.inbound;

/**
 * 进入撮合的统一入站事件（对应 {@code match_event}）。
 */
public sealed interface MatchEvent
    permits PlaceOrder, CancelOrder, CreateSymbol, HaltSymbol, ResumeSymbol, UpdateSymbol {}
