package com.kai.matchengine.match;

/** 默认系统时钟。 */
public final class SystemTimeSource implements TimeSource {

  public static final SystemTimeSource INSTANCE = new SystemTimeSource();

  private SystemTimeSource() {}

  @Override
  public long currentTimeMillis() {
    return System.currentTimeMillis();
  }
}
