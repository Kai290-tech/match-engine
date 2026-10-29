package com.kai.matchengine.outbound;

import com.kai.matchengine.event.inbound.InboundEventType;
import com.kai.matchengine.sbe.outbound.BookUpdateDecoder;
import com.kai.matchengine.sbe.outbound.CommandResultCancelDecoder;
import com.kai.matchengine.sbe.outbound.CommandResultPlaceDecoder;
import com.kai.matchengine.sbe.outbound.CommandResultSymbolDecoder;
import com.kai.matchengine.sbe.outbound.MessageHeaderDecoder;
import com.kai.matchengine.sbe.outbound.OrderUpdateDecoder;
import com.kai.matchengine.sbe.outbound.TradeDecoder;
import org.agrona.concurrent.UnsafeBuffer;

/** 出站 SBE 解码辅助（测试 / 下游集成参考）。 */
public final class SbeOutboundDecoder {

  private static final MessageHeaderDecoder HEADER = new MessageHeaderDecoder();
  private static final TradeDecoder TRADE = new TradeDecoder();
  private static final CommandResultPlaceDecoder PLACE = new CommandResultPlaceDecoder();
  private static final CommandResultCancelDecoder CANCEL = new CommandResultCancelDecoder();
  private static final CommandResultSymbolDecoder SYMBOL = new CommandResultSymbolDecoder();

  private SbeOutboundDecoder() {}

  public static int templateId(byte[] payload) {
    if (payload == null || payload.length < MessageHeaderDecoder.ENCODED_LENGTH) {
      return -1;
    }
    UnsafeBuffer buffer = new UnsafeBuffer(payload);
    HEADER.wrap(buffer, 0);
    return HEADER.templateId();
  }

  public static boolean isTrade(byte[] payload) {
    return templateId(payload) == TradeDecoder.TEMPLATE_ID;
  }

  public static int tradeSymbolId(byte[] payload) {
    TRADE.wrapAndApplyHeader(new UnsafeBuffer(payload), 0, HEADER);
    return TRADE.symbolId();
  }

  public static boolean isAcceptedPlace(byte[] payload) {
    if (templateId(payload) != CommandResultPlaceDecoder.TEMPLATE_ID) {
      return false;
    }
    PLACE.wrapAndApplyHeader(new UnsafeBuffer(payload), 0, HEADER);
    return PLACE.accepted() == 1;
  }

  public static boolean isAcceptedCancel(byte[] payload) {
    if (templateId(payload) != CommandResultCancelDecoder.TEMPLATE_ID) {
      return false;
    }
    CANCEL.wrapAndApplyHeader(new UnsafeBuffer(payload), 0, HEADER);
    return CANCEL.accepted() == 1;
  }

  public static boolean isAcceptedSymbolEvent(byte[] payload, InboundEventType type) {
    if (templateId(payload) != CommandResultSymbolDecoder.TEMPLATE_ID) {
      return false;
    }
    SYMBOL.wrapAndApplyHeader(new UnsafeBuffer(payload), 0, HEADER);
    if (SYMBOL.accepted() != 1) {
      return false;
    }
    return type.name().equals(SYMBOL.eventType().name());
  }

  public static boolean isBookUpdate(byte[] payload) {
    return templateId(payload) == BookUpdateDecoder.TEMPLATE_ID;
  }

  public static boolean isOrderUpdate(byte[] payload) {
    return templateId(payload) == OrderUpdateDecoder.TEMPLATE_ID;
  }
}
