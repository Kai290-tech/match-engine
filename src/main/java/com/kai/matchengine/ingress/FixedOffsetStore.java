package com.kai.matchengine.ingress;

/** 固定起始 offset，占位直至快照就绪。 */
public final class FixedOffsetStore implements OffsetStore {

  private final long offset;

  public FixedOffsetStore(long offset) {
    if (offset < 0) {
      throw new IllegalArgumentException("offset must be >= 0");
    }
    this.offset = offset;
  }

  @Override
  public long loadStartingOffset(String topic, int partition) {
    return offset;
  }
}
