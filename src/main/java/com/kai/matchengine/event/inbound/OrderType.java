package com.kai.matchengine.event.inbound;

/** 订单类型。线值与入站 SBE 枚举一致。 */
public enum OrderType {
  LIMIT,
  MARKET,
  /** 限价立即成交：剩余不入簿。 */
  IOC,
  /** 限价全部成交，否则一笔不成交。 */
  FOK
}
