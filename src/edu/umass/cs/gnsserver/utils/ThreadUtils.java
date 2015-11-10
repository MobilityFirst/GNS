/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gnsserver.utils;

import edu.umass.cs.gnsserver.main.GNS;

/**
 * Sleep without worries.
 * 
 * @author westy
 */
public class ThreadUtils {

  /**
   * Sleep for ms millesconds
   * @param ms
   */
  public static void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (Exception c) {
      GNS.getLogger().severe("Error sleeping :" + c);
    }
  }
  
}
