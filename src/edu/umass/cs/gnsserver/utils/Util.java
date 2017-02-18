/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy
 *
 */
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

/**
 * Various generic static utility methods.
 */
public class Util {

  private static final DecimalFormat decimalFormat = new DecimalFormat("#.#");
  private static final double ALPHA = 0.05;

  /**
   * Format as "#.#".
   *
   * @param d
   * @return a decimal formatted string
   */
  public static final String df(double d) {
    return decimalFormat.format(d);
  }

  /**
   * Format as "#.#" microseconds.
   *
   * @param d
   * @return a decimal formatted as microseconds string
   */
  public static final String mu(double d) {
    return decimalFormat.format(d * 1000) + "us";
  } // milli to microseconds

  /**
   * Moving average.
   *
   * @param sample
   * @param historicalAverage
   * @param alpha
   * @return the moving average as a double
   */
  public static final double movingAverage(double sample, double historicalAverage, double alpha) {
    return (1 - alpha) * historicalAverage + alpha * sample;
  }

  /**
   * Moving average.
   *
   * @param sample
   * @param historicalAverage
   * @return the moving average as a double
   */
  public static final double movingAverage(double sample, double historicalAverage) {
    return movingAverage(sample, historicalAverage, ALPHA);
  }

  /**
   * Moving average.
   *
   * @param sample
   * @param historicalAverage
   * @return the moving average as a double
   */
  public static final double movingAverage(long sample, double historicalAverage) {
    return movingAverage((double) sample, historicalAverage);
  }

  /**
   * Moving average.
   *
   * @param sample
   * @param historicalAverage
   * @param alpha
   * @return the moving average as a double
   */
  public static final double movingAverage(long sample, double historicalAverage, double alpha) {
    return movingAverage((double) sample, historicalAverage, alpha);
  }

  /**
   * Round.
   *
   * @param d
   * @return the value rounded to an integer
   */
  public static int roundToInt(double d) {
    return (int) Math.round(d);
  }

  /**
   * Parses a URI string into a map of strings to strings.
   *
   * @param query
   * @return a map of string to string
   */
  public static Map<String, String> parseURIQueryString(String query) {
    Map<String, String> result = new HashMap<>();
    URLQueryStringParser parser = new URLQueryStringParser(query);
    while (parser.next()) {
      result.put(parser.getName(), parser.getValue());
    }
    return result;
  }

  /**
   * 
   * @param query
   * @return a JSONObject
   * @throws JSONException 
   */
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

  /**
   * Converts a set of integers into an array of integers.
   *
   * @param set
   * @return an integer array
   */
  public static int[] setToIntArray(Set<Integer> set) {
    int[] array = new int[set.size()];
    int i = 0;
    for (int id : set) {
      array[i++] = id;
    }
    return array;
  }

  /**
   * Converts a set of NodeIds to a string.
   *
   * @param nodeIds
   * @return a string
   */
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

  /**
   * * Sorts a Map into a LinkedHashMap in increasing order.
   *
   * @param <K>
   * @param <V>
   * @param map
   * @return the map sorted in ascending order
   */
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

  /**
   * Sorts a Map into a LinkedHashMap in decreasing order.
   *
   * @param <K>
   * @param <V>
   * @param map
   * @return the map sorted in descending order
   */
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

  /**
   * Returns a InetSocketAddress parsed from a string.
   *
   * @param s
   * @return an InetSocketAddress parsed from the string
   */
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

  /**
   * Clips the text at length.
   * Max should be more than 20 or so or you'll have issues. You've been warned.
   *
   * @param text
   * @param max
   * @return a string
   */
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

  /**
   * Returns a stack trace.
   *
   * @return the stack trace as a string
   */
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
