package com.kai.matchengine.match;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kai.matchengine.event.inbound.OrderType;
import com.kai.matchengine.event.inbound.PlaceOrder;
import com.kai.matchengine.event.outbound.CommandResult;
import com.kai.matchengine.event.outbound.OutboundEvent;
import com.kai.matchengine.event.outbound.RejectReason;
import com.kai.matchengine.event.outbound.Trade;
import com.kai.matchengine.orderbook.Side;
import java.util.List;
import org.junit.jupiter.api.Test;

class BookFullMatchingTest {

  @Test
  void bookFull_doesNotPoisonRecentOrderIds() {
    MatchingEngine engine = new MatchingEngine("S-1", () -> 1L, 1);
    engine.apply(new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.LIMIT, 100, 1));

    CommandResult.Place rejected =
        (CommandResult.Place)
            engine.apply(new PlaceOrder(1, 2, 2, 2, Side.BID, OrderType.LIMIT, 99, 1)).getFirst();
    assertEquals(RejectReason.BOOK_FULL, rejected.rejectReason());

    engine.apply(new com.kai.matchengine.event.inbound.CancelOrder(1, 3, 1, 1));

    CommandResult.Place retry =
        (CommandResult.Place)
            engine
                .apply(new PlaceOrder(1, 4, 2, 2, Side.BID, OrderType.LIMIT, 98, 1))
                .getLast();
    assertTrue(retry.accepted(), "same orderId should not be DUPLICATE after BOOK_FULL");
  }

  @Test
  void bookFull_whenWouldPartialFill_rejectsWithoutTrades() {
    MatchingEngine engine = new MatchingEngine("S-1", () -> 1L, 2);
    engine.apply(new PlaceOrder(1, 1, 1, 1, Side.ASK, OrderType.LIMIT, 100, 5));
    engine.apply(new PlaceOrder(1, 2, 2, 2, Side.BID, OrderType.LIMIT, 99, 5));

    List<OutboundEvent> out =
        engine.apply(new PlaceOrder(1, 3, 3, 10, Side.BID, OrderType.LIMIT, 100, 10));

    assertEquals(1, out.size());
    CommandResult.Place place = assertInstanceOf(CommandResult.Place.class, out.getFirst());
    assertEquals(RejectReason.BOOK_FULL, place.rejectReason());
    assertTrue(out.stream().noneMatch(Trade.class::isInstance));
  }

  @Test
  void bookFull_rejectsLimitTakerEvenWhenFullyFillable() {
    MatchingEngine engine = new MatchingEngine("S-1", () -> 1L, 1);
    engine.apply(new PlaceOrder(1, 1, 1, 1, Side.ASK, OrderType.LIMIT, 100, 3));

    List<OutboundEvent> out =
        engine.apply(new PlaceOrder(1, 2, 2, 2, Side.BID, OrderType.LIMIT, 100, 3));

    CommandResult.Place place = assertInstanceOf(CommandResult.Place.class, out.getFirst());
    assertEquals(RejectReason.BOOK_FULL, place.rejectReason());
    assertTrue(out.stream().noneMatch(Trade.class::isInstance));
  }
}
