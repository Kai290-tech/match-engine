package com.kai.matchengine.match;

/** 撮合时钟；成交与订单变更等对外时间均由此生成。 */
public interface TimeSource {

  /** 当前时间（epoch millis）。 */
  long currentTimeMillis();
}
