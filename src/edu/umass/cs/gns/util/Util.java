/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.util;

import java.net.InetSocketAddress;
import java.text.DecimalFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

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
   * @return
   */
  public static final String df(double d) {
    return decimalFormat.format(d);
  }

  /**
   * Format as "#.#" microseconds.
   * 
   * @param d
   * @return
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
   * @return
   */
  public static final double movingAverage(double sample, double historicalAverage, double alpha) {
    return (1 - alpha) * ((double) historicalAverage) + alpha * ((double) sample);
  }

  /**
   * Moving average.
   * 
   * @param sample
   * @param historicalAverage
   * @return
   */
  public static final double movingAverage(double sample, double historicalAverage) {
    return movingAverage(sample, historicalAverage, ALPHA);
  }

  /**
   * Moving average.
   * 
   * @param sample
   * @param historicalAverage
   * @return
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
   * @return
   */
  public static final double movingAverage(long sample, double historicalAverage, double alpha) {
    return movingAverage((double) sample, historicalAverage, alpha);
  }

  /**
   * Round.
   * 
   * @param d
   * @return
   */
  public static int roundToInt(double d) {
    return (int) Math.round(d);
  }

  /**
   * Parses a URI string into a map of strings to strings.
   * 
   * @param query
   * @return
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
   * @param set
   * @return
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
   * @return
   */
  public static <K, V extends Comparable<? super V>> Map<K, V>sortByValueIncreasing(Map<K, V> map) {
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
   * @return
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

  private static final String CHARACTERS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  private static final Random rnd = new Random(System.currentTimeMillis());

  /**
   * Returns a random string of length.
   * @param length
   * @return
   */
  public static String randomString(int length) {
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      sb.append(CHARACTERS.charAt(rnd.nextInt(CHARACTERS.length())));
    }
    return sb.toString();
  }

  /**
   * Returns a InetSocketAddress parsed from a string.
   * 
   * @param s
   * @return
   */
  public static InetSocketAddress getInetSocketAddressFromString(String s) {
    s = s.replaceAll("[^0-9.:]", "");
    String[] tokens = s.split(":");
    if (tokens.length < 2) {
      return null;
    }
    return new InetSocketAddress(tokens[0], Integer.valueOf(tokens[1]));
  }

  // cute little hack to show us where
  private String stackTraceToString() {
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
