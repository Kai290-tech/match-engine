package com.kai.matchengine.orderbook;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class OrderNodePoolTest {

  @Test
  void acquireRelease_reusesNode() {
    OrderNodePool pool = new OrderNodePool(2);
    OrderNode a = pool.acquire(1, 10, Side.BID, 100, 5);
    assertNotNull(a);
    assertEquals(1, pool.allocatedCount());
    assertEquals(1, pool.inUseCount());
    pool.release(a);

    OrderNode b = pool.acquire(2, 20, Side.ASK, 200, 3);
    assertNotNull(b);
    assertEquals(1, pool.allocatedCount());
    assertEquals(1, pool.inUseCount());
    assertEquals(2, b.orderId());
    assertEquals(Side.ASK, b.side());
  }

  @Test
  void exhaust_returnsNull() {
    OrderNodePool pool = new OrderNodePool(1);
    assertNotNull(pool.acquire(1, 1, Side.BID, 1, 1));
    assertNull(pool.acquire(2, 1, Side.BID, 1, 1));
  }

  @Test
  void availableCount_tracksInUse() {
    OrderNodePool pool = new OrderNodePool(3);
    assertEquals(3, pool.availableCount());
    OrderNode a = pool.acquire(1, 1, Side.BID, 1, 1);
    assertEquals(2, pool.availableCount());
    pool.release(a);
    assertEquals(3, pool.availableCount());
  }

  @Test
  void book_respectsPoolCapacity() {
    OrderBook book = new OrderBook("X", new OrderNodePool(2));
    book.add(1, 1, Side.BID, 100, 1);
    book.add(2, 1, Side.BID, 99, 1);
    assertFalse(book.hasCapacityForNewOrder());
  }

  @Test
  void partialFill_keepsNodeCheckedOut_untilTerminal() {
    OrderNodePool pool = new OrderNodePool(10);
    OrderBook book = new OrderBook("X", pool);
    book.add(1, 1, Side.ASK, 100, 10);
    assertEquals(1, pool.inUseCount());

    book.reduce(book.get(1), 4);
    assertEquals(1, pool.inUseCount());
    assertTrue(book.contains(1));

    book.reduce(book.get(1), 6);
    assertEquals(0, pool.inUseCount());
    assertFalse(book.contains(1));
  }

  @Test
  void cancel_releasesToPool() {
    OrderNodePool pool = new OrderNodePool(10);
    OrderBook book = new OrderBook("X", pool);
    book.add(1, 1, Side.BID, 100, 1);
    assertEquals(1, pool.inUseCount());
    book.cancel(1);
    assertEquals(0, pool.inUseCount());
    assertEquals(1, pool.freeCount());
  }

  @Test
  void clear_releasesAllViaCancel() {
    OrderNodePool pool = new OrderNodePool(10);
    OrderBook book = new OrderBook("X", pool);
    book.add(1, 1, Side.BID, 100, 1);
    book.add(2, 1, Side.ASK, 101, 1);
    assertEquals(2, pool.inUseCount());
    book.clear();
    assertTrue(book.isEmpty());
    assertEquals(0, pool.inUseCount());
    assertEquals(2, pool.freeCount());
  }

  @Test
  void release_linkedNode_rejected() {
    OrderNodePool pool = new OrderNodePool(2);
    OrderBook book = new OrderBook("X", pool);
    OrderNode node = book.add(1, 1, Side.BID, 100, 1);
    assertThrows(IllegalStateException.class, () -> pool.release(node));
  }
}
