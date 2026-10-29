package com.kai.matchengine.event.inbound;

/** 订单类型。线值与入站 SBE 枚举一致。 */
public enum OrderType {
  LIMIT,
  /** 立即吃单。卖单数量为基础资产；买单数量为计价预算。剩余不入簿。 */
  MARKET,
  /** 限价立即成交：剩余不入簿。 */
  IOC,
  /** 限价全部成交，否则一笔不成交。 */
  FOK
}
