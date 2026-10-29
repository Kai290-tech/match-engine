package com.kai.matchengine.snapshot;

import com.kai.matchengine.match.SymbolStatus;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/** EngineSnapshot ↔ section payloads（json_gzip）。 */
public final class SnapshotCodec {

  public static final String ENCODING_JSON_GZIP = "json_gzip";
  public static final String SECTION_ENGINE_META = "ENGINE_META";
  public static final String SECTION_MATCH_SYMBOL = "MATCH_SYMBOL";

  public byte[] encodeMeta(EngineSnapshot snapshot) {
    String json =
        "{"
            + "\"v\":"
            + snapshot.schemaVersion()
            + ",\"topic\":\""
            + esc(snapshot.matchTopic())
            + "\",\"partition\":"
            + snapshot.matchPartition()
            + ",\"offset\":"
            + snapshot.lastFullyProcessedOffset()
            + ",\"preparedAt\":"
            + snapshot.preparedAtMillis()
            + ",\"symbolIds\":"
            + symbolIdsJson(snapshot)
            + "}";
    return gzip(json.getBytes(StandardCharsets.UTF_8));
  }

  /** 从 ENGINE_META section 解析 symbolId 列表（load 校验用）。 */
  public List<Integer> decodeMetaSymbolIds(byte[] gzipJson) {
    String json = new String(gunzip(gzipJson), StandardCharsets.UTF_8);
    return parseIntArray(json, "symbolIds");
  }

  public byte[] encodeSymbol(EngineSnapshot.SymbolState state) {
    StringBuilder sb =
        new StringBuilder(160 + state.orders().size() * 32 + state.recentOrderIds().size() * 12);
    sb.append("{\"v\":3,\"symbolId\":")
        .append(state.symbolId())
        .append(",\"code\":\"")
        .append(esc(state.code()))
        .append("\",\"status\":\"")
        .append(state.status().name())
        .append("\",\"eventId\":")
        .append(state.eventId())
        .append(",\"bookVersion\":")
        .append(state.bookVersion())
        .append(",\"orders\":[");
    for (int i = 0; i < state.orders().size(); i++) {
      if (i > 0) {
        sb.append(',');
      }
      EngineSnapshot.RestingOrderState o = state.orders().get(i);
      sb.append('[')
          .append(o.orderId())
          .append(',')
          .append(o.userId())
          .append(',')
          .append(o.side())
          .append(',')
          .append(o.priceTick())
          .append(',')
          .append(o.qty())
          .append(']');
    }
    sb.append("],\"recentOrderIds\":[");
    for (int i = 0; i < state.recentOrderIds().size(); i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append(state.recentOrderIds().get(i));
    }
    sb.append("]}");
    return gzip(sb.toString().getBytes(StandardCharsets.UTF_8));
  }

  public EngineSnapshot.SymbolState decodeSymbol(byte[] gzipJson) {
    String json = new String(gunzip(gzipJson), StandardCharsets.UTF_8);
    int symbolId = (int) longField(json, "symbolId");
    long eventId = longField(json, "eventId");
    long bookVersion = longField(json, "bookVersion");
    String code = stringField(json, "code");
    if (code == null || code.isBlank()) {
      code = "S-" + symbolId;
    }
    String statusRaw = stringField(json, "status");
    SymbolStatus status =
        statusRaw == null || statusRaw.isBlank()
            ? SymbolStatus.TRADING
            : SymbolStatus.valueOf(statusRaw);
    List<EngineSnapshot.RestingOrderState> orders = parseOrders(json);
    List<Long> recentOrderIds = parseLongArray(json, "recentOrderIds");
    return new EngineSnapshot.SymbolState(
        symbolId, code, status, eventId, bookVersion, orders, recentOrderIds);
  }

  private static String symbolIdsJson(EngineSnapshot snapshot) {
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < snapshot.symbols().size(); i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append(snapshot.symbols().get(i).symbolId());
    }
    sb.append(']');
    return sb.toString();
  }

  private static List<EngineSnapshot.RestingOrderState> parseOrders(String json) {
    int start = json.indexOf("\"orders\":[");
    if (start < 0) {
      return List.of();
    }
    start = json.indexOf('[', start + 8);
    int end = indexOfMatchingBracket(json, start);
    if (end < 0) {
      return List.of();
    }
    String body = json.substring(start + 1, end).trim();
    if (body.isEmpty()) {
      return List.of();
    }
    List<EngineSnapshot.RestingOrderState> out = new ArrayList<>();
    int i = 0;
    while (i < body.length()) {
      int a = body.indexOf('[', i);
      if (a < 0) {
        break;
      }
      int b = body.indexOf(']', a);
      String[] parts = body.substring(a + 1, b).split(",");
      out.add(
          new EngineSnapshot.RestingOrderState(
              Long.parseLong(parts[0].trim()),
              Long.parseLong(parts[1].trim()),
              Integer.parseInt(parts[2].trim()),
              Long.parseLong(parts[3].trim()),
              Long.parseLong(parts[4].trim())));
      i = b + 1;
    }
    return out;
  }

  private static List<Long> parseLongArray(String json, String name) {
    String key = "\"" + name + "\":[";
    int start = json.indexOf(key);
    if (start < 0) {
      return List.of();
    }
    start = json.indexOf('[', start);
    int end = indexOfMatchingBracket(json, start);
    if (end < 0) {
      return List.of();
    }
    String body = json.substring(start + 1, end).trim();
    if (body.isEmpty()) {
      return List.of();
    }
    String[] parts = body.split(",");
    List<Long> out = new ArrayList<>(parts.length);
    for (String p : parts) {
      String t = p.trim();
      if (!t.isEmpty()) {
        out.add(Long.parseLong(t));
      }
    }
    return out;
  }

  private static List<Integer> parseIntArray(String json, String name) {
    List<Long> raw = parseLongArray(json, name);
    List<Integer> out = new ArrayList<>(raw.size());
    for (Long v : raw) {
      out.add(v.intValue());
    }
    return out;
  }

  private static int indexOfMatchingBracket(String json, int openIdx) {
    if (openIdx < 0 || openIdx >= json.length() || json.charAt(openIdx) != '[') {
      return -1;
    }
    int depth = 0;
    for (int i = openIdx; i < json.length(); i++) {
      char c = json.charAt(i);
      if (c == '[') {
        depth++;
      } else if (c == ']') {
        depth--;
        if (depth == 0) {
          return i;
        }
      }
    }
    return -1;
  }

  private static long longField(String json, String name) {
    String key = "\"" + name + "\":";
    int i = json.indexOf(key);
    if (i < 0) {
      throw new IllegalArgumentException("missing field " + name);
    }
    int s = i + key.length();
    int e = s;
    while (e < json.length()) {
      char c = json.charAt(e);
      if (c == ',' || c == '}' || c == ']') {
        break;
      }
      e++;
    }
    return Long.parseLong(json.substring(s, e).trim());
  }

  private static String stringField(String json, String name) {
    String key = "\"" + name + "\":\"";
    int i = json.indexOf(key);
    if (i < 0) {
      return null;
    }
    int s = i + key.length();
    StringBuilder out = new StringBuilder();
    for (int e = s; e < json.length(); e++) {
      char c = json.charAt(e);
      if (c == '\\' && e + 1 < json.length()) {
        out.append(json.charAt(e + 1));
        e++;
        continue;
      }
      if (c == '"') {
        return out.toString();
      }
      out.append(c);
    }
    return out.toString();
  }

  private static String esc(String s) {
    return s.replace("\\", "\\\\").replace("\"", "\\\"");
  }

  static byte[] gzip(byte[] raw) {
    try {
      ByteArrayOutputStream bos = new ByteArrayOutputStream(raw.length);
      try (GZIPOutputStream gzip = new GZIPOutputStream(bos)) {
        gzip.write(raw);
      }
      return bos.toByteArray();
    } catch (IOException e) {
      throw new IllegalStateException("gzip failed", e);
    }
  }

  static byte[] gunzip(byte[] compressed) {
    try (GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(compressed));
        ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      in.transferTo(out);
      return out.toByteArray();
    } catch (IOException e) {
      throw new IllegalStateException("gunzip failed", e);
    }
  }
}
