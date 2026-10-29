package com.kai.matchengine.snapshot.delta;

/** 撮合线程当前正在处理的 match_event offset。 */
public final class MatchDeltaContext {

  private static final ThreadLocal<Long> CURRENT_OFFSET = new ThreadLocal<>();

  private MatchDeltaContext() {}

  public static void setOffset(long offset) {
    CURRENT_OFFSET.set(offset);
  }

  public static long currentOffset() {
    Long off = CURRENT_OFFSET.get();
    return off == null ? -1L : off;
  }

  public static void clear() {
    CURRENT_OFFSET.remove();
  }
}
