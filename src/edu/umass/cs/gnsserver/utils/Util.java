
package edu.umass.cs.gnsserver.utils;

import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.nio.JSONPacket;
import java.net.InetSocketAddress;
import java.text.DecimalFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class Util {

  private static final DecimalFormat decimalFormat = new DecimalFormat("#.#");
  private static final double ALPHA = 0.05;


  public static final String df(double d) {
    return decimalFormat.format(d);
  }


  public static final String mu(double d) {
    return decimalFormat.format(d * 1000) + "us";
  } // milli to microseconds


  public static final double movingAverage(double sample, double historicalAverage, double alpha) {
    return (1 - alpha) * historicalAverage + alpha * sample;
  }


  public static final double movingAverage(double sample, double historicalAverage) {
    return movingAverage(sample, historicalAverage, ALPHA);
  }


  public static final double movingAverage(long sample, double historicalAverage) {
    return movingAverage((double) sample, historicalAverage);
  }


  public static final double movingAverage(long sample, double historicalAverage, double alpha) {
    return movingAverage((double) sample, historicalAverage, alpha);
  }


  public static int roundToInt(double d) {
    return (int) Math.round(d);
  }


  public static Map<String, String> parseURIQueryString(String query) {
    Map<String, String> result = new HashMap<>();
    URLQueryStringParser parser = new URLQueryStringParser(query);
    while (parser.next()) {
      result.put(parser.getName(), parser.getValue());
    }
    return result;
  }


  public static JSONObject parseURIQueryStringIntoJSONObject(String query) throws JSONException {
    JSONObject json = new JSONObject();
    URLQueryStringParser parser = new URLQueryStringParser(query);
    while (parser.next()) {
      GNSConfig.getLogger().log(Level.FINE, "PARSE: {0}", parser.getValue());
      json.put(parser.getName(), JSONParseString(parser.getValue()));
    }
    return json;
  }

  private static Object JSONParseString(String string) throws JSONException {
    if (JSONPacket.couldBeJSONObject(string)) {
      return new JSONObject(string);
    } else if (JSONPacket.couldBeJSONArray(string)) {
      return new JSONArray(string);
    } else {
      return string;
    }
  }


  public static int[] setToIntArray(Set<Integer> set) {
    int[] array = new int[set.size()];
    int i = 0;
    for (int id : set) {
      array[i++] = id;
    }
    return array;
  }


  public static String setOfNodeIdToString(Set<?> nodeIds) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (Object x : nodeIds) {
      if (first) {
        sb.append(x.toString());
        first = false;
      } else {
        sb.append(":").append(x.toString());
      }
    }
    return sb.toString();
  }


  public static <K, V extends Comparable<? super V>> Map<K, V> sortByValueIncreasing(Map<K, V> map) {
    List<Map.Entry<K, V>> list
            = new LinkedList<>(map.entrySet());
    Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
      @Override
      public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
        return (o1.getValue()).compareTo(o2.getValue());
      }
    });

    Map<K, V> result = new LinkedHashMap<>();
    for (Map.Entry<K, V> entry : list) {
      result.put(entry.getKey(), entry.getValue());
    }
    return result;
  }


  public static <K, V extends Comparable<? super V>> Map<K, V> sortByValueDecreasing(Map<K, V> map) {
    List<Map.Entry<K, V>> list
            = new LinkedList<>(map.entrySet());
    Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
      @Override
      public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
        return (o2.getValue()).compareTo(o1.getValue());
      }
    });

    Map<K, V> result = new LinkedHashMap<>();
    for (Map.Entry<K, V> entry : list) {
      result.put(entry.getKey(), entry.getValue());
    }
    return result;
  }


  public static InetSocketAddress getInetSocketAddressFromString(String s) {
    s = s.replaceAll("[^0-9.:]", "");
    String[] tokens = s.split(":");
    if (tokens.length < 2) {
      return null;
    }
    return new InetSocketAddress(tokens[0], Integer.valueOf(tokens[1]));
  }

//  private static final String NON_THIN = "[^iIl1\\.,']";
//
//  private static int textWidth(String str) {
//    return (int) (str.length() - str.replaceAll(NON_THIN, "").length() / 2);
//  }
  // close enough
  private static final String SAMPLE_EXPLANATION = " [555000 more chars] ...";
  private static final int EXPLANATION_SIZE = SAMPLE_EXPLANATION.length();


  public static String ellipsize(String text, int max) {
    return text.substring(0, max - EXPLANATION_SIZE) + " [" + (text.length() - max) + " more chars] ...";
//    if (textWidth(text) <= max) {
//      return text;
//    }
//    int end = text.lastIndexOf(' ', max - explanationSize);
//    if (end == -1) {
//      return text.substring(0, max - explanationSize) + " [" + (text.length() - max) + " more chars] ...";
//    }
//    int newEnd = end;
//    do {
//      end = newEnd;
//      newEnd = text.indexOf(' ', end + 1);
//      if (newEnd == -1) {
//        newEnd = text.length();
//      }
//    } while (textWidth(text.substring(0, newEnd) + sampleExplanation) < max);
//    return text.substring(0, end) + " [" + (text.length() - max) + " more chars] ...";
  }


  public String stackTraceToString() {
    StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
    return (stackTrace.length > 2 ? stackTrace[2].toString() + "\n   " : "")
            + (stackTrace.length > 3 ? stackTrace[3].toString() + "\n   " : "")
            + (stackTrace.length > 4 ? stackTrace[4].toString() + "\n   " : "")
            + (stackTrace.length > 5 ? stackTrace[5].toString() + "\n   " : "")
            + (stackTrace.length > 6 ? stackTrace[6].toString() + "\n   " : "")
            + (stackTrace.length > 7 ? stackTrace[7].toString() + "\n   " : "")
            + (stackTrace.length > 8 ? stackTrace[8].toString() + "\n" : "");
  }
}
