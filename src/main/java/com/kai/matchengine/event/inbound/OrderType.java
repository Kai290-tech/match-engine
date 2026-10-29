package com.kai.matchengine.event.inbound;

/** 订单类型。线值与入站 SBE 枚举一致。 */
public enum OrderType {
  LIMIT,
  MARKET,
  /** 限价立即成交：剩余不入簿。 */
  IOC
}
