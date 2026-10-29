package com.kai.matchengine.event.inbound;

/** 进入撮合的事件类型（对应 match_event）。 */
public enum InboundEventType {
  PLACE_ORDER,
  CANCEL_ORDER,
  CREATE_SYMBOL,
  HALT_SYMBOL,
  RESUME_SYMBOL,
  UPDATE_SYMBOL
}
