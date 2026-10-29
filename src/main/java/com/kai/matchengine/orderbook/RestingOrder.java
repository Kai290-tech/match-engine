package com.kai.matchengine.orderbook;

/** 簿上存活挂单视图（同价 FIFO 顺序）。 */
public record RestingOrder(long orderId, long userId, Side side, long priceTick, long qty) {}
