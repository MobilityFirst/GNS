/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gnrs.util;

import edu.umass.cs.gnrs.main.GNS;

/**
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
      GNS.getLogger().severe("error sleeping :" + c);
    }
  }
  
}
