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
import com.kai.matchengine.sbe.CancelOrderDecoder;
import com.kai.matchengine.sbe.CreateSymbolDecoder;
import com.kai.matchengine.sbe.HaltSymbolDecoder;
import com.kai.matchengine.sbe.MessageHeaderDecoder;
import com.kai.matchengine.sbe.PlaceOrderDecoder;
import com.kai.matchengine.sbe.ResumeSymbolDecoder;
import com.kai.matchengine.sbe.UpdateSymbolDecoder;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * SBE 入站解码：bytes → {@link MatchEvent}。
 *
 * <p><b>单线程使用</b>：内部复用 buffer/decoder，禁止多线程共享同一实例。
 */
public final class SbeMatchEventCodec {

  private final MessageHeaderDecoder header = new MessageHeaderDecoder();
  private final PlaceOrderDecoder placeDecoder = new PlaceOrderDecoder();
  private final CancelOrderDecoder cancelDecoder = new CancelOrderDecoder();
  private final CreateSymbolDecoder createDecoder = new CreateSymbolDecoder();
  private final HaltSymbolDecoder haltDecoder = new HaltSymbolDecoder();
  private final ResumeSymbolDecoder resumeDecoder = new ResumeSymbolDecoder();
  private final UpdateSymbolDecoder updateDecoder = new UpdateSymbolDecoder();
  private final UnsafeBuffer buffer = new UnsafeBuffer();

  public MatchEvent decode(byte[] payload) throws IngressException {
    if (payload == null || payload.length == 0) {
      throw new IngressException(IngressRejectReason.EMPTY_PAYLOAD, "empty kafka payload");
    }
    buffer.wrap(payload);
    return decode(buffer, 0, payload.length);
  }

  public MatchEvent decode(DirectBuffer src, int offset, int length) throws IngressException {
    if (src == null || length <= 0) {
      throw new IngressException(IngressRejectReason.EMPTY_PAYLOAD, "empty buffer");
    }
    if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
      throw new IngressException(
          IngressRejectReason.DECODE_FAILED, "payload shorter than sbe header");
    }
    try {
      header.wrap(src, offset);
      final int schemaId = header.schemaId();
      if (schemaId != PlaceOrderDecoder.SCHEMA_ID) {
        throw new IngressException(
            IngressRejectReason.DECODE_FAILED, "unexpected schemaId=" + schemaId);
      }
      final int templateId = header.templateId();
      final int blockLength = header.blockLength();
      final int version = header.version();
      final int actingOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH;
      final int frameNeed = MessageHeaderDecoder.ENCODED_LENGTH + blockLength;
      if (length < frameNeed) {
        throw new IngressException(
            IngressRejectReason.DECODE_FAILED,
            "payload length=" + length + " < required=" + frameNeed);
      }

      if (templateId == PlaceOrderDecoder.TEMPLATE_ID) {
        placeDecoder.wrap(src, actingOffset, blockLength, version);
        return toPlace(placeDecoder);
      }
      if (templateId == CancelOrderDecoder.TEMPLATE_ID) {
        cancelDecoder.wrap(src, actingOffset, blockLength, version);
        return toCancel(cancelDecoder);
      }
      if (templateId == CreateSymbolDecoder.TEMPLATE_ID) {
        createDecoder.wrap(src, actingOffset, blockLength, version);
        return toCreate(createDecoder);
      }
      if (templateId == HaltSymbolDecoder.TEMPLATE_ID) {
        haltDecoder.wrap(src, actingOffset, blockLength, version);
        return toHalt(haltDecoder);
      }
      if (templateId == ResumeSymbolDecoder.TEMPLATE_ID) {
        resumeDecoder.wrap(src, actingOffset, blockLength, version);
        return toResume(resumeDecoder);
      }
      if (templateId == UpdateSymbolDecoder.TEMPLATE_ID) {
        updateDecoder.wrap(src, actingOffset, blockLength, version);
        return toUpdate(updateDecoder);
      }
      throw new IngressException(
          IngressRejectReason.UNKNOWN_TEMPLATE, "unknown templateId=" + templateId);
    } catch (IngressException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new IngressException(IngressRejectReason.DECODE_FAILED, "sbe decode failed", e);
    }
  }

  private static PlaceOrder toPlace(PlaceOrderDecoder d) throws IngressException {
    Side side = mapSide(d.side());
    OrderType orderType = mapOrderType(d.orderType());
    return new PlaceOrder(
        d.symbolId(),
        d.clientRequestId(),
        d.orderId(),
        d.userId(),
        side,
        orderType,
        d.priceTick(),
        d.qty());
  }

  private static CancelOrder toCancel(CancelOrderDecoder d) {
    return new CancelOrder(d.symbolId(), d.clientRequestId(), d.orderId(), d.userId());
  }

  private static CreateSymbol toCreate(CreateSymbolDecoder d) {
    return new CreateSymbol(d.symbolId(), d.clientRequestId(), trimCode(d.code()));
  }

  private static UpdateSymbol toUpdate(UpdateSymbolDecoder d) {
    return new UpdateSymbol(d.symbolId(), d.clientRequestId(), trimCode(d.code()));
  }

  private static HaltSymbol toHalt(HaltSymbolDecoder d) {
    return new HaltSymbol(d.symbolId(), d.clientRequestId());
  }

  private static ResumeSymbol toResume(ResumeSymbolDecoder d) {
    return new ResumeSymbol(d.symbolId(), d.clientRequestId());
  }

  private static String trimCode(String raw) {
    if (raw == null) {
      return "";
    }
    int end = raw.length();
    while (end > 0 && raw.charAt(end - 1) == '\0') {
      end--;
    }
    return raw.substring(0, end).trim();
  }

  private static Side mapSide(com.kai.matchengine.sbe.Side side) throws IngressException {
    if (side == null || side == com.kai.matchengine.sbe.Side.NULL_VAL) {
      throw new IngressException(IngressRejectReason.INVALID_ENUM, "side missing");
    }
    return switch (side) {
      case BID -> Side.BID;
      case ASK -> Side.ASK;
      default -> throw new IngressException(IngressRejectReason.INVALID_ENUM, "side=" + side);
    };
  }

  private static OrderType mapOrderType(com.kai.matchengine.sbe.OrderType type)
      throws IngressException {
    if (type == null || type == com.kai.matchengine.sbe.OrderType.NULL_VAL) {
      throw new IngressException(IngressRejectReason.INVALID_ENUM, "orderType missing");
    }
    return switch (type) {
      case LIMIT -> OrderType.LIMIT;
      case MARKET -> OrderType.MARKET;
      default -> throw new IngressException(IngressRejectReason.INVALID_ENUM, "orderType=" + type);
    };
  }
}
