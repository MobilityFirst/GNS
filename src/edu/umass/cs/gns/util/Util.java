/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.util;

import edu.umass.cs.gns.main.GNS;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.text.DecimalFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

/**
 * Various generic static utility methods.
 */
public class Util {

  public static final DecimalFormat decimalFormat = new DecimalFormat("#.#");
  public static final double ALPHA = 0.05;

  public static final String df(double d) {
    return decimalFormat.format(d);
  }

  public static final String mu(double d) {
    return decimalFormat.format(d * 1000) + "us";
  } // milli to microseconds

  public static final double movingAverage(double sample, double historicalAverage, double alpha) {
    return (1 - alpha) * ((double) historicalAverage) + alpha * ((double) sample);
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

  public static String refreshKey(String id) {
    return (id.toString() + (int) (Math.random() * Integer.MAX_VALUE));
  }

  public static int roundToInt(double d) {
    return (int) Math.round(d);
  }

  public static void assertAssertionsEnabled() {
    boolean assertOn = false;
    // *assigns* true if assertions are on.
    assert assertOn = true;
    if (!assertOn) {
      throw new RuntimeException("Asserts not enabled; enable assertions using the '-ea' JVM option");
    }
  }

  @SuppressWarnings("unchecked")
  public static Object createObject(String className, Object... arguments) {
    Object object;
    Class[] types = new Class[arguments.length];
    for (int i = 0; i < arguments.length; i++) {
      types[i] = arguments[i].getClass();
    }
    try {
      Class theClass = Class.forName(className);
      Constructor constructor = theClass.getConstructor(types);
      object = constructor.newInstance(arguments);
      return object;
    } catch (NoSuchMethodException e) {
      GNS.getLogger().severe("Problem creating instance: " + e);
    } catch (ClassNotFoundException e) {
      GNS.getLogger().severe("Problem creating instance: " + e);
    } catch (InstantiationException e) {
      GNS.getLogger().severe("Problem creating instance: " + e);
    } catch (IllegalAccessException e) {
      GNS.getLogger().severe("Problem creating instance: " + e);
    } catch (IllegalArgumentException e) {
      GNS.getLogger().severe("Problem creating instance: " + e);
    } catch (InvocationTargetException e) {
      GNS.getLogger().severe("Problem creating instance: " + e);
    }
    return null;
  }

  public static Map<String, String> parseURIQueryString(String query) {
    Map<String, String> result = new HashMap<String, String>();
    URLQueryStringParser parser = new URLQueryStringParser(query);
    while (parser.next()) {
      result.put(parser.getName(), parser.getValue());
    }
    return result;
  }

  public static String prefix(String str, int prefixLength) {
    if (str == null || str.length() <= prefixLength) {
      return str;
    }
    return str.substring(0, prefixLength);
  }

  public static Set<Integer> arrayToIntSet(int[] array) {
    TreeSet<Integer> set = new TreeSet<Integer>();
    for (int i = 0; i < array.length; i++) {
      set.add(array[i]);
    }
    return set;
  }

  public static Set<String> nodeIdSetToStringSet(Set set) {
    Set<String> result = new HashSet<String>();
    for (Object id : set) {
      result.add(id.toString());
    }
    return result;
  }

  public static int[] setToIntArray(Set<Integer> set) {
    int[] array = new int[set.size()];
    int i = 0;
    for (int id : set) {
      array[i++] = id;
    }
    return array;
  }

  public static Object[] setToNodeIdArray(Set set) {
    Object[] array = new Object[set.size()];
    int i = 0;
    for (Object id : set) {
      array[i++] = id;
    }
    return array;
  }

  public static Integer[] setToIntegerArray(Set<Integer> set) {
    Integer[] array = new Integer[set.size()];
    int i = 0;
    for (Integer id : set) {
      array[i++] = id;
    }
    return array;
  }

  public static int[] stringToIntArray(String string) {
    string = string.replaceAll("\\[", "").replaceAll("\\]", "").replaceAll("\\s", "");
    String[] tokens = string.split(",");
    int[] array = new int[tokens.length];
    for (int i = 0; i < array.length; i++) {
      array[i] = Integer.parseInt(tokens[i]);
    }
    return array;
  }

  public static Integer[] intToIntegerArray(int[] array) {
    if (array == null) {
      return null;
    } else if (array.length == 0) {
      return new Integer[0];
    }
    Integer[] retarray = new Integer[array.length];
    int i = 0;
    for (int member : array) {
      retarray[i++] = member;
    }
    return retarray;
  }

  public static Integer[] objectToIntegerArray(Object[] objects) {
    if (objects == null) {
      return null;
    } else if (objects.length == 0) {
      return new Integer[0];
    }
    Integer[] array = new Integer[objects.length];
    int i = 0;
    for (Object obj : objects) {
      array[i++] = (Integer) obj;
    }
    return array;
  }

  public static Set<String> arrayOfIntToStringSet(int[] array) {
    Set<String> set = new HashSet<String>();
    for (Integer member : array) {
      set.add(member.toString());
    }
    return set;
  }

  public static String arrayOfIntToString(int[] array) {
    String s = "[";
    for (int i = 0; i < array.length; i++) {
      s += array[i];
      s += (i < array.length - 1 ? "," : "]");
    }
    return s;
  }

  public static boolean contains(int member, int[] array) {
    for (int i = 0; i < array.length; i++) {
      if (array[i] == member) {
        return true;
      }
    }
    return false;
  }

  public static Set<String> arrayOfNodeIdsToStringSet(Object[] array) {
    Set<String> set = new HashSet<String>();
    for (Object member : array) {
      set.add(member.toString());
    }
    return set;
  }

  // FIXME: Is there a sublinear method to return a random member from a set?
  public static Object selectRandom(Collection<?> set) {
    int random = (int) (Math.random() * set.size());
    Iterator<?> iterator = set.iterator();
    Object randomNode = null;
    for (int i = 0; i <= random && iterator.hasNext(); i++) {
      randomNode = iterator.next();
    }
    return randomNode;
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
        sb.append(":" + x.toString());
      }
    }
    return sb.toString();
  }

  public static <K, V extends Comparable<? super V>> Map<K, V>
          sortByValueIncreasing(Map<K, V> map) {
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

  public static <K, V extends Comparable<? super V>> Map<K, V>
          sortByValueDecreasing(Map<K, V> map) {
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

  static final String CHARACTERS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  static Random rnd = new Random(System.currentTimeMillis());

  public static String randomString(int len) {
    StringBuilder sb = new StringBuilder(len);
    for (int i = 0; i < len; i++) {
      sb.append(CHARACTERS.charAt(rnd.nextInt(CHARACTERS.length())));
    }
    return sb.toString();
  }

  public static InetSocketAddress getInetSocketAddressFromString(String s) {
    s = s.replaceAll("[^0-9.:]", "");
    String[] tokens = s.split(":");
    if (tokens.length < 2) {
      return null;
    }
    return new InetSocketAddress(tokens[0], Integer.valueOf(tokens[1]));
  }

  public void assertEnabled() {
    try {
      assert (false);
    } catch (Exception e) {
      return;
    }
    throw new RuntimeException("Asserts not enabled; exiting");
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

  public static void main(String[] args) {
    System.out.println(Util.getInetSocketAddressFromString((new InetSocketAddress(InetAddress.getLoopbackAddress(), 0)).toString()).getHostString());
  }
}
