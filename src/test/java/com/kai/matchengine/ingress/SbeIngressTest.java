package com.kai.matchengine.ingress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kai.matchengine.event.inbound.CancelOrder;
import com.kai.matchengine.event.inbound.OrderType;
import com.kai.matchengine.event.inbound.PlaceOrder;
import com.kai.matchengine.event.inbound.QtyUnit;
import com.kai.matchengine.match.EngineRegistry;
import com.kai.matchengine.match.SystemTimeSource;
import com.kai.matchengine.orderbook.Side;
import com.kai.matchengine.sbe.MessageHeaderEncoder;
import com.kai.matchengine.sbe.PlaceOrderEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SbeIngressTest {

  private SbeMatchEventCodec codec;
  private SbeMatchEventEncoder encoder;
  private IngressValidator validator;

  @BeforeEach
  void setUp() {
    codec = new SbeMatchEventCodec();
    encoder = new SbeMatchEventEncoder();
    validator = new IngressValidator();
  }

  @Test
  void encodeDecode_placeOrder_roundTrip() throws Exception {
    PlaceOrder original =
        new PlaceOrder(1, 100L, 100L, 9L, Side.BID, OrderType.LIMIT, 12345L, 10L);
    PlaceOrder decoded = assertInstanceOf(PlaceOrder.class, codec.decode(encoder.encode(original)));
    assertEquals(original, decoded);
  }

  @Test
  void encodeDecode_cancelOrder_roundTrip() throws Exception {
    CancelOrder original = new CancelOrder(2, 88L, 88L, 7L);
    CancelOrder decoded =
        assertInstanceOf(CancelOrder.class, codec.decode(encoder.encode(original)));
    assertEquals(original, decoded);
  }

  @Test
  void decode_empty_fails() {
    assertEquals(
        IngressRejectReason.EMPTY_PAYLOAD,
        assertThrows(IngressException.class, () -> codec.decode(new byte[0])).reason());
  }

  @Test
  void decode_truncatedFrame_fails() throws Exception {
    byte[] full = encoder.encode(new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.LIMIT, 100, 1));
    byte[] truncated = Arrays.copyOf(full, MessageHeaderEncoder.ENCODED_LENGTH + 4);
    assertEquals(
        IngressRejectReason.DECODE_FAILED,
        assertThrows(IngressException.class, () -> codec.decode(truncated)).reason());
  }

  @Test
  void decode_wrongSchemaId_fails() {
    byte[] bytes = encoder.encode(new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.LIMIT, 100, 1));
    UnsafeBuffer buf = new UnsafeBuffer(bytes);
    MessageHeaderEncoder header = new MessageHeaderEncoder();
    header.wrap(buf, 0);
    header.schemaId(999);
    assertEquals(
        IngressRejectReason.DECODE_FAILED,
        assertThrows(IngressException.class, () -> codec.decode(bytes)).reason());
  }

  @Test
  void encodeDecode_createHaltResume_roundTrip() throws Exception {
    var create = new com.kai.matchengine.event.inbound.CreateSymbol(3, 1L, "BTCUSDT");
    assertEquals(create, codec.decode(encoder.encode(create)));
    var halt = new com.kai.matchengine.event.inbound.HaltSymbol(3, 2L);
    assertEquals(halt, codec.decode(encoder.encode(halt)));
    var resume = new com.kai.matchengine.event.inbound.ResumeSymbol(3, 3L);
    assertEquals(resume, codec.decode(encoder.encode(resume)));
  }

  @Test
  void validator_unknownSymbolId_allowedStructurally_zeroRejected() throws Exception {
    // 未知 symbol 在 apply 侧以 CommandResult 拒绝，ingress 只拦结构错误
    validator.validate(new PlaceOrder(99, 1, 1, 1, Side.ASK, OrderType.LIMIT, 100, 1));
    assertEquals(
        IngressRejectReason.INVALID_SYMBOL,
        assertThrows(
                IngressException.class,
                () ->
                    validator.validate(new PlaceOrder(0, 1, 1, 1, Side.ASK, OrderType.LIMIT, 100, 1)))
            .reason());
  }

  @Test
  void validator_invalidQtyAndLimitPrice() {
    assertEquals(
        IngressRejectReason.INVALID_QTY,
        assertThrows(
                IngressException.class,
                () ->
                    validator.validate(new PlaceOrder(1, 1, 1, 1, Side.ASK, OrderType.LIMIT, 100, 0)))
            .reason());
    assertEquals(
        IngressRejectReason.INVALID_PRICE,
        assertThrows(
                IngressException.class,
                () ->
                    validator.validate(new PlaceOrder(1, 1, 1, 1, Side.ASK, OrderType.LIMIT, 0, 1)))
            .reason());
  }

  @Test
  void validator_market_ignoresPrice() throws Exception {
    validator.validate(new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.MARKET, 0, 5));
    validator.validate(new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.MARKET, -1, 5));
  }

  @Test
  void validator_cancelUnknownSymbol_structurallyOk() throws Exception {
    validator.validate(new CancelOrder(9, 1, 1, 1));
  }

  @Test
  void validator_createSymbol_requiresCode() {
    assertEquals(
        IngressRejectReason.INVALID_SYMBOL_CODE,
        assertThrows(
                IngressException.class,
                () ->
                    validator.validate(
                        new com.kai.matchengine.event.inbound.CreateSymbol(1, 1L, "  ")))
            .reason());
  }

  @Test
  void validator_place_doesNotRequirePreRegisteredSymbol() throws Exception {
    EngineRegistry registry = new EngineRegistry(SystemTimeSource.INSTANCE);
    registry.register(7);
    validator.validate(new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.LIMIT, 100, 1));
    validator.validate(new PlaceOrder(7, 1, 1, 1, Side.BID, OrderType.LIMIT, 100, 1));
  }

  @Test
  void processor_acceptsPlaceAndCancel_rejectsBad() {
    List<ValidatedInbound> accepted = new ArrayList<>();
    MatchEventProcessor processor = new MatchEventProcessor(codec, validator, accepted::add);

    assertTrue(
        processor.process(
            encoder.encode(new PlaceOrder(1, 10, 10, 1, Side.BID, OrderType.LIMIT, 100, 5)),
            "match_event",
            0,
            42L));
    assertTrue(
        processor.process(
            encoder.encode(new CancelOrder(1, 10, 10, 1)), "match_event", 0, 43L));
    assertEquals(2, accepted.size());
    assertInstanceOf(CancelOrder.class, accepted.get(1).event());

    // 未知 symbol 结构合法，会进入下游（业务拒绝在 apply）
    assertTrue(
        processor.process(
            encoder.encode(new PlaceOrder(9, 11, 11, 1, Side.BID, OrderType.LIMIT, 100, 5)),
            "match_event",
            0,
            44L));
    assertEquals(3, processor.acceptedCount());
    assertEquals(0, processor.rejectedCount());
  }

  @Test
  void processor_handlerFailure_isFatal() {
    java.util.concurrent.atomic.AtomicReference<Throwable> fatal =
        new java.util.concurrent.atomic.AtomicReference<>();
    MatchEventProcessor processor =
        new MatchEventProcessor(
            codec,
            validator,
            inbound -> {
              throw new IllegalStateException("boom");
            },
            (message, cause) -> fatal.set(cause));
    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () ->
                processor.process(
                    encoder.encode(new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.LIMIT, 100, 1)),
                    "t",
                    0,
                    1L));
    assertEquals("boom", thrown.getCause().getMessage());
    assertTrue(fatal.get() instanceof IllegalStateException);
    assertEquals(0, processor.acceptedCount());
    assertEquals(0, processor.rejectedCount());
  }

  @Test
  void processor_garbageBytes_rejected() {
    MatchEventProcessor processor = new MatchEventProcessor(codec, validator, inbound -> {});
    assertFalse(processor.process(new byte[] {1, 2, 3}, "match_event", 0, 1L));
    assertEquals(1, processor.rejectedCount());
  }

  @Test
  void fixedOffsetStore() {
    assertEquals(7, new FixedOffsetStore(7).loadStartingOffset("match_event", 0));
    assertThrows(IllegalArgumentException.class, () -> new FixedOffsetStore(-1));
  }

  @Test
  void encode_blockLength_matchesPlaceTemplate() {
    byte[] bytes = encoder.encode(new PlaceOrder(1, 1, 1, 1, Side.ASK, OrderType.LIMIT, 1, 1));
    assertEquals(
        MessageHeaderEncoder.ENCODED_LENGTH + PlaceOrderEncoder.BLOCK_LENGTH, bytes.length);
  }

  @Test
  void decode_nullPayload_fails() {
    assertEquals(
        IngressRejectReason.EMPTY_PAYLOAD,
        assertThrows(IngressException.class, () -> codec.decode(null)).reason());
  }

  @Test
  void decode_headerOnly_fails() {
    byte[] headerOnly = new byte[MessageHeaderEncoder.ENCODED_LENGTH];
    assertEquals(
        IngressRejectReason.DECODE_FAILED,
        assertThrows(IngressException.class, () -> codec.decode(headerOnly)).reason());
  }

  @Test
  void decode_unknownTemplateId_fails() {
    byte[] bytes = encoder.encode(new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.LIMIT, 100, 1));
    UnsafeBuffer buf = new UnsafeBuffer(bytes);
    MessageHeaderEncoder header = new MessageHeaderEncoder();
    header.wrap(buf, 0);
    header.templateId(9999);
    assertEquals(
        IngressRejectReason.UNKNOWN_TEMPLATE,
        assertThrows(IngressException.class, () -> codec.decode(bytes)).reason());
  }

  @Test
  void validator_negativeSymbolId_andNegativeQty() {
    assertEquals(
        IngressRejectReason.INVALID_SYMBOL,
        assertThrows(
                IngressException.class,
                () ->
                    validator.validate(
                        new PlaceOrder(-1, 1, 1, 1, Side.ASK, OrderType.LIMIT, 100, 1)))
            .reason());
    assertEquals(
        IngressRejectReason.INVALID_QTY,
        assertThrows(
                IngressException.class,
                () ->
                    validator.validate(
                        new PlaceOrder(1, 1, 1, 1, Side.ASK, OrderType.LIMIT, 100, -1)))
            .reason());
  }

  @Test
  void validator_nullSideOrOrderType() {
    assertEquals(
        IngressRejectReason.INVALID_ENUM,
        assertThrows(
                IngressException.class,
                () -> validator.validate(new PlaceOrder(1, 1, 1, 1, null, OrderType.LIMIT, 100, 1)))
            .reason());
    assertEquals(
        IngressRejectReason.INVALID_ENUM,
        assertThrows(
                IngressException.class,
                () -> validator.validate(new PlaceOrder(1, 1, 1, 1, Side.BID, null, 100, 1)))
            .reason());
  }

  @Test
  void validator_limitNegativePrice_rejected() {
    assertEquals(
        IngressRejectReason.INVALID_PRICE,
        assertThrows(
                IngressException.class,
                () ->
                    validator.validate(new PlaceOrder(1, 1, 1, 1, Side.ASK, OrderType.LIMIT, -5, 1)))
            .reason());
  }

  @Test
  void validator_cancel_symbolIdZero() {
    assertEquals(
        IngressRejectReason.INVALID_SYMBOL,
        assertThrows(IngressException.class, () -> validator.validate(new CancelOrder(0, 1, 1, 1)))
            .reason());
  }

  @Test
  void validator_createThenPlace_structurallyIndependentOfRegistry() throws Exception {
    EngineRegistry registry = new EngineRegistry(SystemTimeSource.INSTANCE);
    validator.validate(new PlaceOrder(3, 1, 1, 1, Side.BID, OrderType.LIMIT, 100, 1));
    registry.register(3);
    validator.validate(new PlaceOrder(3, 1, 1, 1, Side.BID, OrderType.LIMIT, 100, 1));
  }

  @Test
  void processor_rejectDoesNotInvokeFatal() {
    java.util.concurrent.atomic.AtomicBoolean fatal = new java.util.concurrent.atomic.AtomicBoolean();
    MatchEventProcessor processor =
        new MatchEventProcessor(
            codec, validator, inbound -> {}, (message, cause) -> fatal.set(true));
    assertFalse(processor.process(new byte[] {9, 9, 9}, "t", 0, 1L));
    assertFalse(fatal.get());
    assertEquals(1, processor.rejectedCount());
  }

  @Test
  void processor_handlerError_isFatal() {
    java.util.concurrent.atomic.AtomicReference<Throwable> fatal =
        new java.util.concurrent.atomic.AtomicReference<>();
    MatchEventProcessor processor =
        new MatchEventProcessor(
            codec,
            validator,
            inbound -> {
              throw new AssertionError("oom-like");
            },
            (message, cause) -> fatal.set(cause));
    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () ->
                processor.process(
                    encoder.encode(new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.LIMIT, 100, 1)),
                    "t",
                    0,
                    1L));
    assertTrue(thrown.getCause() instanceof AssertionError);
    assertTrue(fatal.get() instanceof AssertionError);
    assertEquals(0, processor.acceptedCount());
  }


  @Test
  void clientRequestId_roundTrip() throws Exception {
    PlaceOrder place = new PlaceOrder(1, 555L, 100L, 9L, Side.BID, OrderType.LIMIT, 12345L, 10L);
    PlaceOrder decodedPlace = assertInstanceOf(PlaceOrder.class, codec.decode(encoder.encode(place)));
    assertEquals(555L, decodedPlace.clientRequestId());

    CancelOrder cancel = new CancelOrder(2, 777L, 88L, 7L);
    CancelOrder decodedCancel =
        assertInstanceOf(CancelOrder.class, codec.decode(encoder.encode(cancel)));
    assertEquals(777L, decodedCancel.clientRequestId());
  }

  @Test
  void encodeDecode_ioc_roundTrip() throws Exception {
    PlaceOrder original =
        new PlaceOrder(1, 8L, 80L, 9L, Side.ASK, OrderType.IOC, 250L, 4L);
    PlaceOrder decoded = assertInstanceOf(PlaceOrder.class, codec.decode(encoder.encode(original)));
    assertEquals(OrderType.IOC, decoded.orderType());
    assertEquals(250L, decoded.priceTick());
    assertEquals(4L, decoded.qty());
  }

  @Test
  void encodeDecode_postOnly_roundTrip() throws Exception {
    PlaceOrder original = new PlaceOrder(1, 13L, 130L, 4L, Side.ASK, OrderType.POST_ONLY, 80L, 6L);
    PlaceOrder decoded = assertInstanceOf(PlaceOrder.class, codec.decode(encoder.encode(original)));
    assertEquals(OrderType.POST_ONLY, decoded.orderType());
    assertEquals(80L, decoded.priceTick());
    assertEquals(QtyUnit.BASE, decoded.qtyUnit());
    validator.validate(decoded);
    assertEquals(
        IngressRejectReason.INVALID_PRICE,
        assertThrows(
                IngressException.class,
                () -> validator.validate(new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.POST_ONLY, 0, 1)))
            .reason());
  }

  @Test
  void encodeDecode_fok_roundTrip() throws Exception {
    PlaceOrder original = new PlaceOrder(1, 9L, 90L, 4L, Side.BID, OrderType.FOK, 80L, 6L);
    PlaceOrder decoded = assertInstanceOf(PlaceOrder.class, codec.decode(encoder.encode(original)));
    assertEquals(OrderType.FOK, decoded.orderType());
    assertEquals(80L, decoded.priceTick());
    assertEquals(6L, decoded.qty());
  }

  @Test
  void validator_ioc_requiresPositivePrice() throws Exception {
    validator.validate(new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.IOC, 100, 1));
    assertEquals(
        IngressRejectReason.INVALID_PRICE,
        assertThrows(
                IngressException.class,
                () -> validator.validate(new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.IOC, 0, 1)))
            .reason());
    assertEquals(
        IngressRejectReason.INVALID_PRICE,
        assertThrows(
                IngressException.class,
                () -> validator.validate(new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.FOK, 0, 1)))
            .reason());
  }

  @Test
  void encodeDecode_marketBidQuote_roundTrip() throws Exception {
    PlaceOrder original =
        new PlaceOrder(1, 11L, 110L, 4L, Side.BID, OrderType.MARKET, 0, 10_000, QtyUnit.QUOTE);
    PlaceOrder decoded = assertInstanceOf(PlaceOrder.class, codec.decode(encoder.encode(original)));
    assertEquals(QtyUnit.QUOTE, decoded.qtyUnit());
    assertEquals(10_000L, decoded.qty());
    validator.validate(decoded);
  }

  @Test
  void encodeDecode_omittedQtyUnit_isBase() throws Exception {
    PlaceOrder original = new PlaceOrder(1, 12L, 120L, 4L, Side.ASK, OrderType.MARKET, 0, 5);
    PlaceOrder decoded = assertInstanceOf(PlaceOrder.class, codec.decode(encoder.encode(original)));
    assertEquals(QtyUnit.BASE, decoded.qtyUnit());
    validator.validate(decoded);
  }

  @Test
  void validator_quoteUnit_onlyMarketBid() throws Exception {
    validator.validate(new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.MARKET, 0, 100, QtyUnit.QUOTE));
    assertEquals(
        IngressRejectReason.INVALID_QTY,
        assertThrows(
                IngressException.class,
                () ->
                    validator.validate(
                        new PlaceOrder(1, 1, 1, 1, Side.ASK, OrderType.MARKET, 0, 100, QtyUnit.QUOTE)))
            .reason());
    assertEquals(
        IngressRejectReason.INVALID_QTY,
        assertThrows(
                IngressException.class,
                () ->
                    validator.validate(
                        new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.LIMIT, 100, 1, QtyUnit.QUOTE)))
            .reason());
  }

}
