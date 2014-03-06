/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gns.util;

import edu.umass.cs.gns.main.GNS;

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
      GNS.getLogger().severe("Error sleeping :" + c);
    }
  }
  
}
