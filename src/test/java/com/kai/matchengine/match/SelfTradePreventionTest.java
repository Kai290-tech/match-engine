package com.kai.matchengine.match;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kai.matchengine.event.inbound.OrderType;
import com.kai.matchengine.event.inbound.PlaceOrder;
import com.kai.matchengine.event.inbound.QtyUnit;
import com.kai.matchengine.event.outbound.CommandResult;
import com.kai.matchengine.event.outbound.OrderUpdateType;
import com.kai.matchengine.event.outbound.OutboundEvent;
import com.kai.matchengine.event.outbound.RejectReason;
import com.kai.matchengine.event.outbound.Trade;
import com.kai.matchengine.orderbook.Side;
import java.util.List;
import org.junit.jupiter.api.Test;

/** ME-STP-001：F30 先吃别人再停，F31 队首是自己，F32 FOK 不把挡在后面的量算进去。 */
class SelfTradePreventionTest {

  @Test
  void f30_limitFillsOthersThenStopsAtOwnOrder() {
    MatchingEngine engine = new MatchingEngine("S-1", () -> 1L);
    engine.apply(new PlaceOrder(1, 1, 1, 2, Side.ASK, OrderType.LIMIT, 100, 2));
    engine.apply(new PlaceOrder(1, 2, 2, 1, Side.ASK, OrderType.LIMIT, 100, 5));

    List<OutboundEvent> out =
        engine.apply(new PlaceOrder(1, 3, 30, 1, Side.BID, OrderType.LIMIT, 100, 6));

    List<Trade> trades = trades(out);
    assertEquals(1, trades.size());
    assertEquals(1L, trades.get(0).makerOrderId());
    assertEquals(2L, trades.get(0).qty());
    assertEquals(100L, trades.get(0).priceTick());
    CommandResult.Place place = placeResult(out);
    assertTrue(place.accepted());
    assertEquals(2L, place.filledQty());
    assertEquals(0L, place.remainingQty());
    assertFalse(place.resting());
    assertEquals(OrderUpdateType.CANCELLED_REMAINDER, takerUpdate(out, 30L));
    assertFalse(engine.book().contains(1));
    assertEquals(5L, engine.book().get(2).qty());
    assertFalse(engine.book().contains(30));
  }

  @Test
  void f30_marketQuoteStopsBeforeSpendingOnSelf() {
    MatchingEngine engine = new MatchingEngine("S-1", () -> 1L);
    engine.apply(new PlaceOrder(1, 1, 1, 2, Side.ASK, OrderType.LIMIT, 100, 2));
    engine.apply(new PlaceOrder(1, 2, 2, 1, Side.ASK, OrderType.LIMIT, 100, 5));

    List<OutboundEvent> out =
        engine.apply(new PlaceOrder(1, 3, 31, 1, Side.BID, OrderType.MARKET, 0, 1_000, QtyUnit.QUOTE));

    List<Trade> trades = trades(out);
    assertEquals(1, trades.size());
    assertEquals(2L, trades.get(0).qty());
    assertEquals(200L, trades.get(0).priceTick() * trades.get(0).qty());
    assertEquals(2L, placeResult(out).filledQty());
    assertEquals(5L, engine.book().get(2).qty());
  }

  @Test
  void f31_ownOrderAtHeadCancelsWithoutTrading() {
    MatchingEngine engine = new MatchingEngine("S-1", () -> 1L);
    engine.apply(new PlaceOrder(1, 1, 1, 1, Side.ASK, OrderType.LIMIT, 100, 5));

    List<OutboundEvent> out =
        engine.apply(new PlaceOrder(1, 2, 8, 1, Side.BID, OrderType.IOC, 100, 3));

    assertTrue(out.stream().noneMatch(Trade.class::isInstance));
    CommandResult.Place place = placeResult(out);
    assertTrue(place.accepted());
    assertEquals(0L, place.filledQty());
    assertEquals(0L, place.remainingQty());
    assertFalse(place.resting());
    assertEquals(5L, engine.book().get(1).qty());

    CommandResult.Place duplicate =
        placeResult(engine.apply(new PlaceOrder(1, 3, 8, 1, Side.BID, OrderType.LIMIT, 100, 1)));
    assertEquals(RejectReason.DUPLICATE_ORDER_ID, duplicate.rejectReason());
    assertEquals(5L, engine.book().get(1).qty());
  }

  @Test
  void postOnlyCrossingOwnBestPriceCancelsEntireOrder() {
    MatchingEngine engine = new MatchingEngine("S-1", () -> 1L);
    engine.apply(new PlaceOrder(1, 1, 1, 1, Side.ASK, OrderType.LIMIT, 100, 5));

    List<OutboundEvent> out =
        engine.apply(new PlaceOrder(1, 2, 9, 1, Side.BID, OrderType.POST_ONLY, 100, 3));

    assertTrue(out.stream().noneMatch(Trade.class::isInstance));
    CommandResult.Place place = placeResult(out);
    assertTrue(place.accepted());
    assertEquals(0L, place.filledQty());
    assertFalse(place.resting());
    assertEquals(5L, engine.book().get(1).qty());
  }

  @Test
  void f32_fokIgnoresQuantityBehindOwnOrder() {
    MatchingEngine engine = new MatchingEngine("S-1", () -> 1L);
    engine.apply(new PlaceOrder(1, 1, 1, 2, Side.ASK, OrderType.LIMIT, 100, 2));
    engine.apply(new PlaceOrder(1, 2, 2, 1, Side.ASK, OrderType.LIMIT, 100, 5));
    engine.apply(new PlaceOrder(1, 3, 3, 2, Side.ASK, OrderType.LIMIT, 100, 4));

    List<OutboundEvent> blocked =
        engine.apply(new PlaceOrder(1, 4, 40, 1, Side.BID, OrderType.FOK, 100, 4));
    assertTrue(blocked.stream().noneMatch(Trade.class::isInstance));
    assertEquals(0L, placeResult(blocked).filledQty());
    assertEquals(2L, engine.book().get(1).qty());
    assertEquals(5L, engine.book().get(2).qty());
    assertEquals(4L, engine.book().get(3).qty());

    List<OutboundEvent> filled =
        engine.apply(new PlaceOrder(1, 5, 41, 1, Side.BID, OrderType.FOK, 100, 2));
    List<Trade> trades = trades(filled);
    assertEquals(1, trades.size());
    assertEquals(1L, trades.get(0).makerOrderId());
    assertEquals(2L, trades.get(0).qty());
    assertEquals(2L, placeResult(filled).filledQty());
    assertFalse(engine.book().contains(1));
    assertEquals(5L, engine.book().get(2).qty());
    assertEquals(4L, engine.book().get(3).qty());
  }

  private static List<Trade> trades(List<OutboundEvent> out) {
    return out.stream().filter(Trade.class::isInstance).map(Trade.class::cast).toList();
  }

  private static CommandResult.Place placeResult(List<OutboundEvent> out) {
    return assertInstanceOf(CommandResult.Place.class, out.getLast());
  }

  private static OrderUpdateType takerUpdate(List<OutboundEvent> out, long orderId) {
    return out.stream()
        .filter(com.kai.matchengine.event.outbound.OrderUpdate.class::isInstance)
        .map(com.kai.matchengine.event.outbound.OrderUpdate.class::cast)
        .filter(u -> u.orderId() == orderId)
        .findFirst()
        .orElseThrow()
        .type();
  }
}
