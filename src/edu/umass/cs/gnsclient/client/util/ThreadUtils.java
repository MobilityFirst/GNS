/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gnsclient.client.util;

import edu.umass.cs.gnsclient.client.GNSClient;

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
      GNSClient.getLogger().severe("Error sleeping :" + c);
    }
  }
}
