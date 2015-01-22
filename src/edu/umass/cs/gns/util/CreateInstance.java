/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.util;

import edu.umass.cs.gns.main.GNS;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

/**
 * A nice little class that helps us create class instances on the fly.
 *
 * @author	Westy (westy@cs.umass.edu)
 */
public class CreateInstance {

  /**
   * Creates an instance from a class name, argument list and type list. 
   * Looks up and invokes the correct constructor.
   * 
   * @param name - the name of the class to create an instance of
   * @param argsList - a list of arguments
   * @param argTypes - a list of fully qualified class names
   * @return 
   */
  public static Object createInstance(String name, List<Object> argsList, List<String> argTypes) {
    Class<?> definition;
    Class<?>[] argsClass = new Class[argsList.size()];
    Object[] args = new Object[argsList.size()];
    Constructor<?> argsConstructor;

    try {
      for (int i = 0; i < argsList.size(); i++) {
        argsClass[i] = Class.forName(argTypes.get(i));
        args[i] = argsList.get(i);
      }
      definition = Class.forName(name);
      argsConstructor = definition.getConstructor(argsClass);
      return createObject(argsConstructor, args);
    } catch (ClassNotFoundException e) {
      System.out.println(e);
      return null;
    } catch (NoSuchMethodException e) {
      System.out.println(e);
      return null;
    }
  }

  private static Object createObject(Constructor constructor, Object[] arguments) {
    GNS.getLogger().info("Constructor: " + constructor.toString());
    Object object = null;
    try {
      object = constructor.newInstance(arguments);
      GNS.getLogger().info("Object: " + object.toString());
      return object;
    } catch (InstantiationException e) {
      System.out.println(e);
    } catch (IllegalAccessException e) {
      System.out.println(e);
    } catch (IllegalArgumentException e) {
      System.out.println(e);
    } catch (InvocationTargetException e) {
      System.out.println(e);
    }
    return object;
  }
}
