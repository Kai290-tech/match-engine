package com.kai.matchengine.outbound;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kai.matchengine.event.inbound.OrderType;
import com.kai.matchengine.event.inbound.PlaceOrder;
import com.kai.matchengine.event.outbound.CommandResult;
import com.kai.matchengine.event.outbound.RejectReason;
import com.kai.matchengine.event.outbound.Trade;
import com.kai.matchengine.match.MatchingEngine;
import com.kai.matchengine.orderbook.Side;
import com.kai.matchengine.sbe.outbound.CommandResultPlaceDecoder;
import com.kai.matchengine.sbe.outbound.MessageHeaderDecoder;
import com.kai.matchengine.sbe.outbound.TradeDecoder;
import java.util.List;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

class SbeOutboundEncoderTest {

  private final SbeOutboundEncoder encoder = new SbeOutboundEncoder();
  private final MessageHeaderDecoder header = new MessageHeaderDecoder();

  @Test
  void roundTrip_trade_and_placeResult() {
    OutboundBatch batch = new OutboundBatch("match_event", 0, 9L, 42, List.of());
    byte[] tradeBytes =
        encoder.encode(
            batch,
            new Trade(1, 2, "ETHUSDT", 10, 11, 20, 21, 100, 5, Side.BID));
    assertTrue(SbeOutboundDecoder.isTrade(tradeBytes));
    assertEquals(42, SbeOutboundDecoder.tradeSymbolId(tradeBytes));

    TradeDecoder tradeDecoder = new TradeDecoder();
    tradeDecoder.wrapAndApplyHeader(new UnsafeBuffer(tradeBytes), 0, header);
    assertEquals(10, tradeDecoder.takerOrderId());
    assertEquals(5, tradeDecoder.qty());

    byte[] placeBytes =
        encoder.encode(
            batch,
            CommandResult.Place.ok(99L, 1L, "ETHUSDT", 1L, 0L, 1L, true));
    assertTrue(SbeOutboundDecoder.isAcceptedPlace(placeBytes));

    CommandResultPlaceDecoder placeDecoder = new CommandResultPlaceDecoder();
    placeDecoder.wrapAndApplyHeader(new UnsafeBuffer(placeBytes), 0, header);
    assertEquals(99L, placeDecoder.clientRequestId());
    assertEquals(1, placeDecoder.resting());
  }

  @Test
  void matchingEngine_bookFull_rejects() {
    MatchingEngine engine = new MatchingEngine("S-1", () -> 1L, 1);
    engine.apply(new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.LIMIT, 100, 1));
    var out =
        engine.apply(new PlaceOrder(1, 2, 2, 2, Side.BID, OrderType.LIMIT, 99, 1));
    CommandResult.Place place = (CommandResult.Place) out.getFirst();
    assertEquals(RejectReason.BOOK_FULL, place.rejectReason());
  }
}
