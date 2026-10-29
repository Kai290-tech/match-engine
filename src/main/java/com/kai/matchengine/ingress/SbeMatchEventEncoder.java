package com.kai.matchengine.ingress;

import com.kai.matchengine.event.inbound.CancelOrder;
import com.kai.matchengine.event.inbound.CreateSymbol;
import com.kai.matchengine.event.inbound.HaltSymbol;
import com.kai.matchengine.event.inbound.MatchEvent;
import com.kai.matchengine.event.inbound.OrderType;
import com.kai.matchengine.event.inbound.PlaceOrder;
import com.kai.matchengine.event.inbound.ResumeSymbol;
import com.kai.matchengine.event.inbound.UpdateSymbol;
import com.kai.matchengine.orderbook.Side;
import com.kai.matchengine.sbe.CancelOrderEncoder;
import com.kai.matchengine.sbe.CreateSymbolEncoder;
import com.kai.matchengine.sbe.HaltSymbolEncoder;
import com.kai.matchengine.sbe.MessageHeaderEncoder;
import com.kai.matchengine.sbe.PlaceOrderEncoder;
import com.kai.matchengine.sbe.ResumeSymbolEncoder;
import com.kai.matchengine.sbe.UpdateSymbolEncoder;
import org.agrona.concurrent.UnsafeBuffer;

/** 测试 / 造数用：领域事件 → SBE bytes。 */
public final class SbeMatchEventEncoder {

  private final MessageHeaderEncoder header = new MessageHeaderEncoder();
  private final PlaceOrderEncoder placeEncoder = new PlaceOrderEncoder();
  private final CancelOrderEncoder cancelEncoder = new CancelOrderEncoder();
  private final CreateSymbolEncoder createEncoder = new CreateSymbolEncoder();
  private final HaltSymbolEncoder haltEncoder = new HaltSymbolEncoder();
  private final ResumeSymbolEncoder resumeEncoder = new ResumeSymbolEncoder();
  private final UpdateSymbolEncoder updateEncoder = new UpdateSymbolEncoder();
  private final UnsafeBuffer buffer = new UnsafeBuffer();

  public byte[] encode(MatchEvent event) {
    return switch (event) {
      case PlaceOrder p -> encodePlace(p);
      case CancelOrder c -> encodeCancel(c);
      case CreateSymbol c -> encodeCreate(c);
      case UpdateSymbol u -> encodeUpdate(u);
      case HaltSymbol h -> encodeHalt(h);
      case ResumeSymbol r -> encodeResume(r);
    };
  }

  public byte[] encodePlace(PlaceOrder p) {
    byte[] bytes = new byte[MessageHeaderEncoder.ENCODED_LENGTH + PlaceOrderEncoder.BLOCK_LENGTH];
    buffer.wrap(bytes);
    PlaceOrderEncoder body = placeEncoder.wrapAndApplyHeader(buffer, 0, header);
    body.symbolId(p.symbolId());
    body.clientRequestId(p.clientRequestId());
    body.orderId(p.orderId());
    body.userId(p.userId());
    body.side(mapSide(p.side()));
    body.orderType(mapOrderType(p.orderType()));
    body.priceTick(p.priceTick());
    body.qty(p.qty());
    return bytes;
  }

  public byte[] encodeCancel(CancelOrder c) {
    byte[] bytes = new byte[MessageHeaderEncoder.ENCODED_LENGTH + CancelOrderEncoder.BLOCK_LENGTH];
    buffer.wrap(bytes);
    CancelOrderEncoder body = cancelEncoder.wrapAndApplyHeader(buffer, 0, header);
    body.symbolId(c.symbolId());
    body.clientRequestId(c.clientRequestId());
    body.orderId(c.orderId());
    body.userId(c.userId());
    return bytes;
  }

  public byte[] encodeCreate(CreateSymbol c) {
    byte[] bytes =
        new byte[MessageHeaderEncoder.ENCODED_LENGTH + CreateSymbolEncoder.BLOCK_LENGTH];
    buffer.wrap(bytes);
    CreateSymbolEncoder body = createEncoder.wrapAndApplyHeader(buffer, 0, header);
    body.symbolId(c.symbolId());
    body.clientRequestId(c.clientRequestId());
    body.code(c.code() == null ? "" : c.code());
    return bytes;
  }

  public byte[] encodeUpdate(UpdateSymbol u) {
    byte[] bytes =
        new byte[MessageHeaderEncoder.ENCODED_LENGTH + UpdateSymbolEncoder.BLOCK_LENGTH];
    buffer.wrap(bytes);
    UpdateSymbolEncoder body = updateEncoder.wrapAndApplyHeader(buffer, 0, header);
    body.symbolId(u.symbolId());
    body.clientRequestId(u.clientRequestId());
    body.code(u.code() == null ? "" : u.code());
    return bytes;
  }

  public byte[] encodeHalt(HaltSymbol h) {
    byte[] bytes = new byte[MessageHeaderEncoder.ENCODED_LENGTH + HaltSymbolEncoder.BLOCK_LENGTH];
    buffer.wrap(bytes);
    HaltSymbolEncoder body = haltEncoder.wrapAndApplyHeader(buffer, 0, header);
    body.symbolId(h.symbolId());
    body.clientRequestId(h.clientRequestId());
    return bytes;
  }

  public byte[] encodeResume(ResumeSymbol r) {
    byte[] bytes =
        new byte[MessageHeaderEncoder.ENCODED_LENGTH + ResumeSymbolEncoder.BLOCK_LENGTH];
    buffer.wrap(bytes);
    ResumeSymbolEncoder body = resumeEncoder.wrapAndApplyHeader(buffer, 0, header);
    body.symbolId(r.symbolId());
    body.clientRequestId(r.clientRequestId());
    return bytes;
  }

  private static com.kai.matchengine.sbe.Side mapSide(Side side) {
    return side == Side.BID ? com.kai.matchengine.sbe.Side.BID : com.kai.matchengine.sbe.Side.ASK;
  }

  private static com.kai.matchengine.sbe.OrderType mapOrderType(OrderType type) {
    return switch (type) {
      case LIMIT -> com.kai.matchengine.sbe.OrderType.LIMIT;
      case MARKET -> com.kai.matchengine.sbe.OrderType.MARKET;
      case IOC -> com.kai.matchengine.sbe.OrderType.IOC;
    };
  }
}
