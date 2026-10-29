package com.kai.matchengine.orderbook;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OrderBookTest {

  private OrderBook book;

  @BeforeEach
  void setUp() {
    book = new OrderBook("BTC-USDT");
  }

  @Test
  void addBidAndAsk_updatesBest() {
    book.add(1, 100, Side.BID, 100, 5);
    book.add(2, 100, Side.BID, 101, 3);
    book.add(3, 200, Side.ASK, 105, 4);
    book.add(4, 200, Side.ASK, 104, 2);

    assertTrue(book.hasBestBid());
    assertTrue(book.hasBestAsk());
    assertEquals(101, book.bestBid());
    assertEquals(104, book.bestAsk());
    assertEquals(2, book.bestBidHead().orderId());
    assertEquals(4, book.bestAskHead().orderId());
    assertEquals(4, book.orderCount());
  }

  @Test
  void samePrice_fifoOrder() {
    book.add(1, 1, Side.ASK, 100, 1);
    book.add(2, 1, Side.ASK, 100, 1);
    book.add(3, 1, Side.ASK, 100, 1);

    assertEquals(1, book.bestAskHead().orderId());
    book.fillBestHead(Side.ASK, 1);
    assertEquals(2, book.bestAskHead().orderId());
    book.fillBestHead(Side.ASK, 1);
    assertEquals(3, book.bestAskHead().orderId());
  }

  @Test
  void cancel_middleOfQueue_keepsFifo() {
    book.add(1, 1, Side.BID, 100, 1);
    book.add(2, 1, Side.BID, 100, 1);
    book.add(3, 1, Side.BID, 100, 1);

    OrderNode cancelled = book.cancel(2);
    assertNotNull(cancelled);
    assertEquals(2, cancelled.orderId());
    assertFalse(book.contains(2));
    assertEquals(1, book.bestBidHead().orderId());
    assertEquals(2, book.bidLevelQty(100));
    book.fillBestHead(Side.BID, 1);
    assertEquals(3, book.bestBidHead().orderId());
    assertEquals(1, book.bidLevelQty(100));
  }

  @Test
  void cancel_bestLevel_refreshesBest() {
    book.add(1, 1, Side.BID, 100, 1);
    book.add(2, 1, Side.BID, 99, 1);

    book.cancel(1);
    assertEquals(99, book.bestBid());
    assertEquals(2, book.bestBidHead().orderId());

    book.cancel(2);
    assertFalse(book.hasBestBid());
    assertNull(book.bestBidHead());
    assertTrue(book.isEmpty());
  }

  @Test
  void reduce_partialThenFull() {
    book.add(1, 1, Side.ASK, 100, 10);

    assertEquals(4, book.reduce(book.get(1), 4));
    assertEquals(6, book.get(1).qty());
    assertEquals(6, book.askLevelQty(100));

    assertEquals(6, book.fillBestHead(Side.ASK, 100));
    assertFalse(book.contains(1));
    assertFalse(book.hasBestAsk());
  }

  @Test
  void duplicateOrderId_rejected() {
    book.add(1, 1, Side.BID, 100, 1);
    assertThrows(IllegalArgumentException.class, () -> book.add(1, 1, Side.BID, 101, 1));
  }

  @Test
  void cancel_missing_returnsNull() {
    assertNull(book.cancel(99));
  }

  @Test
  void depth_andRestingOrders_order() {
    book.add(1, 1, Side.BID, 100, 2);
    book.add(2, 1, Side.BID, 101, 3);
    book.add(3, 1, Side.BID, 100, 1);
    book.add(4, 2, Side.ASK, 105, 4);
    book.add(5, 2, Side.ASK, 104, 5);

    assertEquals(101, book.bidDepth().getFirst().priceTick());
    assertEquals(3, book.bidDepth().getFirst().totalQty());
    assertEquals(100, book.bidDepth().get(1).priceTick());
    assertEquals(3, book.bidDepth().get(1).totalQty());
    assertEquals(2, book.bidDepth().get(1).orderCount());

    assertEquals(104, book.askDepth().getFirst().priceTick());
    assertEquals(105, book.askDepth().get(1).priceTick());

    var resting = book.restingOrders();
    assertEquals(List.of(2L, 1L, 3L, 5L, 4L), resting.stream().map(RestingOrder::orderId).toList());
  }

  @Test
  void blankSymbol_rejected() {
    assertThrows(IllegalArgumentException.class, () -> new OrderBook(" "));
  }

  @Test
  void add_invalidQtyOrNullSide() {
    assertThrows(IllegalArgumentException.class, () -> book.add(1, 1, Side.BID, 100, 0));
    assertThrows(NullPointerException.class, () -> book.add(1, 1, null, 100, 1));
  }

  @Test
  void bestPrice_throwsWhenEmpty() {
    assertThrows(IllegalStateException.class, book::bestBid);
    assertThrows(IllegalStateException.class, book::bestAsk);
  }

  @Test
  void fillBestHead_empty_returnsZero() {
    assertEquals(0, book.fillBestHead(Side.BID, 10));
    assertEquals(0, book.fillBestHead(Side.ASK, 10));
  }

  @Test
  void reduce_full_setsNodeQtyZero() {
    book.add(1, 1, Side.ASK, 100, 3);
    OrderNode node = book.get(1);
    book.reduce(node, 3);
    assertEquals(0, node.qty());
    assertFalse(node.isLinked());
  }

  @Test
  void reduce_invalid_throws() {
    book.add(1, 1, Side.ASK, 100, 3);
    assertThrows(IllegalArgumentException.class, () -> book.reduce(book.get(1), 0));
    OrderNode gone = book.cancel(1);
    assertThrows(IllegalArgumentException.class, () -> book.reduce(gone, 1));
  }

  @Test
  void cancel_askBest_refreshes() {
    book.add(1, 1, Side.ASK, 100, 1);
    book.add(2, 1, Side.ASK, 101, 1);
    book.cancel(1);
    assertEquals(101, book.bestAsk());
    book.cancel(2);
    assertFalse(book.hasBestAsk());
  }

  @Test
  void clear_resetsState() {
    book.add(1, 1, Side.BID, 100, 1);
    book.add(2, 1, Side.ASK, 101, 1);
    book.clear();
    assertTrue(book.isEmpty());
    assertEquals(0, book.bidLevelCount());
    assertEquals(0, book.askLevelCount());
    assertFalse(book.hasBestBid());
    assertFalse(book.hasBestAsk());
  }

  @Test
  void levelQty_missingPrice_isZero() {
    assertEquals(0, book.bidLevelQty(100));
    assertEquals(0, book.askLevelQty(100));
  }
}
