package edu.umass.cs.gns.util;

import edu.umass.cs.gns.main.GNS;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Various generic static utility methods.
 *
 * @author Hardeep Uppal, Westy
 */
public class Util {

  public static int roundToInt(double d) {
    return (int) Math.round(d);
  }

  public static Object createObject(String className, Object ... arguments) {
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
    QueryStringParser parser = new QueryStringParser(query);
    while (parser.next()) {
      result.put(parser.getName(), parser.getValue());
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
}
