package com.kai.matchengine.event.inbound;

/** {@link PlaceOrder#qty()} 的单位。线值与入站 SBE 枚举一致。 */
public enum QtyUnit {
  /** 基础资产数量。 */
  BASE,
  /** 计价资产预算。仅市价买单可用。 */
  QUOTE
}
