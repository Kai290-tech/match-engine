package com.kai.matchengine.orderbook;

/** 价格档深度视图（最优价在前）。 */
public record DepthLevel(long priceTick, long totalQty, int orderCount) {}
