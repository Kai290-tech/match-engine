package com.kai.matchengine.snapshot.delta;

/** {@link BookDeltaLog#tryAppend} 结果。 */
public enum BookDeltaAppendResult {
  /** 写入主 ring。 */
  RING,
  /** ring 满，写入 spill 区。 */
  SPILL,
  /** ring 与 spill 均满，本条丢弃并标记 GAP。 */
  GAP_DROPPED
}
