package com.kai.matchengine.event.outbound;

/** 订单变更类型（投影到 orderUpdate topic）。 */
public enum OrderUpdateType {
  /** 限价剩余入簿。 */
  RESTING,
  /** 部分成交后仍挂在簿上（maker）。 */
  PARTIALLY_FILLED,
  /** 完全成交，已离开簿。 */
  FILLED,
  /** 主动撤单。 */
  CANCELLED,
  /** 市价未成交剩余被取消（未入簿）。 */
  CANCELLED_REMAINDER
}
