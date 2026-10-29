package com.kai.matchengine.snapshot.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kai.matchengine.orderbook.Side;
import org.junit.jupiter.api.Test;

class BookDeltaLogTest {

  @Test
  void tryAppend_poll_ringOrder() {
    BookDeltaLog log = new BookDeltaLog(4, 0);
    assertEquals(
        BookDeltaAppendResult.RING,
        log.tryAppend(BookDelta.restAdd(1, 1, 10, 1, Side.BID, 100, 5, 1, 1)));
    assertEquals(
        BookDeltaAppendResult.RING,
        log.tryAppend(BookDelta.restRemove(2, 1, 10, 2, 2)));

    assertEquals(2, log.pendingCount());
    BookDelta first = log.poll();
    assertNotNull(first);
    assertEquals(1L, first.matchOffset());
    assertEquals(BookDeltaType.REST_ADD, first.type());

    BookDelta second = log.poll();
    assertNotNull(second);
    assertEquals(2L, second.matchOffset());
    assertEquals(BookDeltaType.REST_REMOVE, second.type());
    assertNull(log.poll());
  }

  @Test
  void ringFull_usesSpill_withoutBlocking() {
    BookDeltaLog log = new BookDeltaLog(2, 2);
    log.tryAppend(BookDelta.metaRecentOrder(1, 1, 1));
    log.tryAppend(BookDelta.metaRecentOrder(2, 1, 2));
    assertEquals(BookDeltaAppendResult.SPILL, log.tryAppend(BookDelta.metaRecentOrder(3, 1, 3)));
    assertEquals(3, log.pendingCount());

    assertEquals(1L, log.poll().matchOffset());
    assertEquals(2L, log.poll().matchOffset());
    assertEquals(3L, log.poll().matchOffset());
  }

  @Test
  void overflow_marksGapAndDrops_withoutBlocking() {
    BookDeltaLog log = new BookDeltaLog(1, 1);
    assertEquals(BookDeltaAppendResult.RING, log.tryAppend(BookDelta.metaRecentOrder(1, 1, 1)));
    assertEquals(BookDeltaAppendResult.SPILL, log.tryAppend(BookDelta.metaRecentOrder(2, 1, 2)));
    assertEquals(
        BookDeltaAppendResult.GAP_DROPPED,
        log.tryAppend(BookDelta.metaRecentOrder(3, 1, 3)));

    assertTrue(log.gapActive());
    assertEquals(1L, log.gapDroppedCount());
    assertEquals(2, log.pendingCount());

    log.clearGap();
    assertFalse(log.gapActive());
  }

  @Test
  void poll_drainsRingBeforeSpill() {
    BookDeltaLog log = new BookDeltaLog(1, 2);
    log.tryAppend(BookDelta.metaRecentOrder(1, 1, 1));
    log.tryAppend(BookDelta.metaRecentOrder(2, 1, 2));

    assertEquals(1L, log.poll().matchOffset());
    assertEquals(2L, log.poll().matchOffset());
    assertNull(log.poll());
  }
}
