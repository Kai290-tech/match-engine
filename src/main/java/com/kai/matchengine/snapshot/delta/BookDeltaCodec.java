package com.kai.matchengine.snapshot.delta;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/** {@link BookDelta} 列表 ↔ gzip JSON（batch section payload）。 */
public final class BookDeltaCodec {

  public static final String ENCODING_JSON_GZIP = "json_gzip";

  public byte[] encode(List<BookDelta> deltas) {
    if (deltas == null || deltas.isEmpty()) {
      return gzip("[]".getBytes(StandardCharsets.UTF_8));
    }
    StringBuilder sb = new StringBuilder(deltas.size() * 48);
    sb.append('[');
    for (int i = 0; i < deltas.size(); i++) {
      if (i > 0) {
        sb.append(',');
      }
      appendDelta(sb, deltas.get(i));
    }
    sb.append(']');
    return gzip(sb.toString().getBytes(StandardCharsets.UTF_8));
  }

  public List<BookDelta> decode(byte[] payload) {
    if (payload == null || payload.length == 0) {
      return List.of();
    }
    String json = new String(gunzip(payload), StandardCharsets.UTF_8).trim();
    if (json.equals("[]")) {
      return List.of();
    }
  return parseArray(json);
  }

  private static void appendDelta(StringBuilder sb, BookDelta d) {
    sb.append("{\"o\":").append(d.matchOffset());
    sb.append(",\"s\":").append(d.symbolId());
    sb.append(",\"t\":\"").append(d.type()).append('\"');
    sb.append(",\"id\":").append(d.orderId());
    sb.append(",\"u\":").append(d.userId());
    sb.append(",\"sd\":").append(d.side());
    sb.append(",\"p\":").append(d.priceTick());
    sb.append(",\"q\":").append(d.qty());
    sb.append(",\"e\":").append(d.eventId());
    sb.append(",\"bv\":").append(d.bookVersion());
    sb.append('}');
  }

  private static List<BookDelta> parseArray(String json) {
    if (!json.startsWith("[") || !json.endsWith("]")) {
      throw new IllegalArgumentException("invalid delta batch json");
    }
    String body = json.substring(1, json.length() - 1).trim();
    if (body.isEmpty()) {
      return List.of();
    }
    List<BookDelta> out = new ArrayList<>();
    int i = 0;
    while (i < body.length()) {
      if (body.charAt(i) == ',') {
        i++;
        continue;
      }
      if (body.charAt(i) != '{') {
        throw new IllegalArgumentException("invalid delta entry at " + i);
      }
      int end = body.indexOf('}', i);
      if (end < 0) {
        throw new IllegalArgumentException("unclosed delta entry");
      }
      out.add(parseObject(body.substring(i, end + 1)));
      i = end + 1;
    }
    return List.copyOf(out);
  }

  private static BookDelta parseObject(String obj) {
    long o = longField(obj, "\"o\":");
    int s = (int) longField(obj, "\"s\":");
    BookDeltaType t = BookDeltaType.valueOf(stringField(obj, "\"t\":\""));
    long id = longField(obj, "\"id\":");
    long u = longField(obj, "\"u\":");
    int sd = (int) longField(obj, "\"sd\":");
    long p = longField(obj, "\"p\":");
    long q = longField(obj, "\"q\":");
    long e = longField(obj, "\"e\":");
    long bv = longField(obj, "\"bv\":");
    return new BookDelta(o, s, t, id, u, sd, p, q, e, bv);
  }

  private static long longField(String obj, String key) {
    int idx = obj.indexOf(key);
    if (idx < 0) {
      throw new IllegalArgumentException("missing field " + key);
    }
    int start = idx + key.length();
    int end = start;
    while (end < obj.length() && (Character.isDigit(obj.charAt(end)) || obj.charAt(end) == '-')) {
      end++;
    }
    return Long.parseLong(obj.substring(start, end));
  }

  private static String stringField(String obj, String key) {
    int idx = obj.indexOf(key);
    if (idx < 0) {
      throw new IllegalArgumentException("missing field " + key);
    }
    int start = idx + key.length();
    int end = obj.indexOf('\"', start);
    return obj.substring(start, end);
  }

  private static byte[] gzip(byte[] raw) {
    try {
      ByteArrayOutputStream bos = new ByteArrayOutputStream(raw.length);
      try (GZIPOutputStream gzip = new GZIPOutputStream(bos)) {
        gzip.write(raw);
      }
      return bos.toByteArray();
    } catch (IOException e) {
      throw new IllegalStateException("gzip encode failed", e);
    }
  }

  private static byte[] gunzip(byte[] compressed) {
    try {
      ByteArrayInputStream bis = new ByteArrayInputStream(compressed);
      try (GZIPInputStream gzip = new GZIPInputStream(bis)) {
        return gzip.readAllBytes();
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("gzip decode failed", e);
    }
  }
}
