package com.kai.matchengine.event.inbound;

/** 订单类型。线值与入站 SBE 枚举一致。 */
public enum OrderType {
  LIMIT,
  /** 立即吃单。卖单只按基础数量；买单还可按计价预算。剩余不入簿。 */
  MARKET,
  /** 限价立即成交：剩余不入簿。 */
  IOC,
  /** 限价全部成交，否则一笔不成交。 */
  FOK,
  /** 只做 Maker。会吃单则整单取消，否则入簿。 */
  POST_ONLY
}
