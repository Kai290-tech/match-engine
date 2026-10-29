package com.kai.matchengine.orderbook;

/** 买卖方向。 */
public enum Side {
  BID,
  ASK;

  public Side opposite() {
    return this == BID ? ASK : BID;
  }
}
