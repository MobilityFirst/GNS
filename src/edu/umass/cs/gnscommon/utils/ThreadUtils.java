
package edu.umass.cs.gnscommon.utils;

import edu.umass.cs.gnsserver.main.GNSConfig;
import java.util.logging.Level;


public class ThreadUtils {


  public static void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (Exception c) {
      GNSConfig.getLogger().log(Level.SEVERE, 
              "Error sleeping :{0}", c);
    }
  }
  
}
