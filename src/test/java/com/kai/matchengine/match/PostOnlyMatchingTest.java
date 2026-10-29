package com.kai.matchengine.match;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kai.matchengine.event.inbound.OrderType;
import com.kai.matchengine.event.inbound.PlaceOrder;
import com.kai.matchengine.event.inbound.QtyUnit;
import com.kai.matchengine.event.outbound.CommandResult;
import com.kai.matchengine.event.outbound.OrderUpdate;
import com.kai.matchengine.event.outbound.OrderUpdateType;
import com.kai.matchengine.event.outbound.OutboundEvent;
import com.kai.matchengine.event.outbound.RejectReason;
import com.kai.matchengine.event.outbound.Trade;
import com.kai.matchengine.orderbook.Side;
import java.util.List;
import org.junit.jupiter.api.Test;

/** ME-POST-001：F27 入簿，F28 会吃单则整单取消，F29 池满。 */
class PostOnlyMatchingTest {

  @Test
  void f27_restsAndJoinsFifo() {
    MatchingEngine engine = new MatchingEngine("S-1", () -> 1L);
    engine.apply(new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.POST_ONLY, 100, 1));
    engine.apply(new PlaceOrder(1, 2, 2, 1, Side.BID, OrderType.POST_ONLY, 100, 2));

    List<OutboundEvent> out =
        engine.apply(new PlaceOrder(1, 3, 3, 2, Side.ASK, OrderType.LIMIT, 100, 2));

    List<Trade> trades = out.stream().filter(Trade.class::isInstance).map(Trade.class::cast).toList();
    assertEquals(2, trades.size());
    assertEquals(1L, trades.get(0).makerOrderId());
    assertEquals(1L, trades.get(0).qty());
    assertEquals(2L, trades.get(1).makerOrderId());
    assertEquals(1L, trades.get(1).qty());
    assertEquals(100L, trades.get(0).priceTick());
    assertEquals(1L, engine.book().get(2).qty());
    assertFalse(engine.book().contains(1));
  }

  @Test
  void f27_askRestsAboveBestBid() {
    MatchingEngine engine = new MatchingEngine("S-1", () -> 1L);
    engine.apply(new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.LIMIT, 100, 4));

    List<OutboundEvent> out =
        engine.apply(new PlaceOrder(1, 2, 8, 2, Side.ASK, OrderType.POST_ONLY, 101, 3));

    assertTrue(out.stream().noneMatch(Trade.class::isInstance));
    CommandResult.Place place = placeResult(out);
    assertTrue(place.accepted());
    assertEquals(0L, place.filledQty());
    assertEquals(3L, place.remainingQty());
    assertTrue(place.resting());
    assertEquals(OrderUpdateType.RESTING, takerUpdate(out, 8L).type());
    assertEquals(101L, engine.book().bestAsk());
    assertEquals(3L, engine.book().get(8).qty());
  }

  @Test
  void f28_touchingPriceCancelsEntireOrder() {
    MatchingEngine engine = new MatchingEngine("S-1", () -> 1L);
    engine.apply(new PlaceOrder(1, 1, 1, 1, Side.ASK, OrderType.LIMIT, 100, 1));
    engine.apply(new PlaceOrder(1, 2, 2, 1, Side.ASK, OrderType.LIMIT, 150, 10));

    List<OutboundEvent> out =
        engine.apply(new PlaceOrder(1, 3, 30, 2, Side.BID, OrderType.POST_ONLY, 100, 10));

    assertTrue(out.stream().noneMatch(Trade.class::isInstance));
    CommandResult.Place place = placeResult(out);
    assertTrue(place.accepted());
    assertEquals(0L, place.filledQty());
    assertEquals(0L, place.remainingQty());
    assertFalse(place.resting());
    assertEquals(OrderUpdateType.CANCELLED_REMAINDER, takerUpdate(out, 30L).type());
    assertEquals(1L, engine.book().get(1).qty());
    assertEquals(10L, engine.book().get(2).qty());
    assertFalse(engine.book().contains(30));

    CommandResult.Place duplicate =
        placeResult(engine.apply(new PlaceOrder(1, 4, 30, 2, Side.BID, OrderType.POST_ONLY, 99, 1)));
    assertEquals(RejectReason.DUPLICATE_ORDER_ID, duplicate.rejectReason());
    assertEquals(2, engine.book().orderCount());
  }

  @Test
  void f29_bookFullRejectsRestAndStillCancelsACross() {
    MatchingEngine engine = new MatchingEngine("S-1", () -> 1L, 1);
    engine.apply(new PlaceOrder(1, 1, 1, 1, Side.ASK, OrderType.LIMIT, 100, 2));
    assertFalse(engine.book().hasCapacityForNewOrder());

    CommandResult.Place full =
        placeResult(engine.apply(new PlaceOrder(1, 2, 8, 2, Side.BID, OrderType.POST_ONLY, 99, 1)));
    assertEquals(RejectReason.BOOK_FULL, full.rejectReason());
    assertEquals(2L, engine.book().get(1).qty());

    List<OutboundEvent> crossed =
        engine.apply(new PlaceOrder(1, 3, 8, 2, Side.BID, OrderType.POST_ONLY, 100, 5));
    assertTrue(
        crossed.stream()
            .noneMatch(e -> e instanceof CommandResult.Place p && p.rejectReason() == RejectReason.BOOK_FULL));
    CommandResult.Place place = placeResult(crossed);
    assertTrue(place.accepted());
    assertEquals(0L, place.filledQty());
    assertFalse(place.resting());
    assertTrue(crossed.stream().noneMatch(Trade.class::isInstance));
    assertEquals(2L, engine.book().get(1).qty());
    assertFalse(engine.book().contains(8));

    CommandResult.Place duplicate =
        placeResult(engine.apply(new PlaceOrder(1, 4, 8, 2, Side.BID, OrderType.POST_ONLY, 99, 1)));
    assertEquals(RejectReason.DUPLICATE_ORDER_ID, duplicate.rejectReason());
  }

  @Test
  void f28_invalidPriceDoesNotEnterIdempotencyWindow() {
    MatchingEngine engine = new MatchingEngine("S-1", () -> 1L);
    CommandResult.Place rejected =
        placeResult(engine.apply(new PlaceOrder(1, 1, 7, 1, Side.BID, OrderType.POST_ONLY, 0, 1)));
    assertEquals(RejectReason.INVALID_PRICE, rejected.rejectReason());

    CommandResult.Place quote =
        placeResult(
            engine.apply(new PlaceOrder(1, 2, 7, 1, Side.BID, OrderType.POST_ONLY, 100, 1, QtyUnit.QUOTE)));
    assertEquals(RejectReason.INVALID_QTY, quote.rejectReason());

    CommandResult.Place accepted =
        placeResult(engine.apply(new PlaceOrder(1, 3, 7, 1, Side.BID, OrderType.POST_ONLY, 100, 1)));
    assertTrue(accepted.accepted());
    assertTrue(accepted.resting());
    assertEquals(1L, engine.book().get(7).qty());
  }

  private static CommandResult.Place placeResult(List<OutboundEvent> out) {
    return assertInstanceOf(CommandResult.Place.class, out.getLast());
  }

  private static OrderUpdate takerUpdate(List<OutboundEvent> out, long orderId) {
    return out.stream()
        .filter(OrderUpdate.class::isInstance)
        .map(OrderUpdate.class::cast)
        .filter(u -> u.orderId() == orderId)
        .findFirst()
        .orElseThrow();
  }
}
