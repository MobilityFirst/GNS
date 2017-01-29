
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport;

import edu.umass.cs.gnscommon.GNSProtocol;


public class InternalField {


  public static String makeInternalFieldString(String string) {
    return GNSProtocol.INTERNAL_PREFIX.toString() + string;
  }


  public static boolean isInternalField(String key) {
    return key.startsWith(GNSProtocol.INTERNAL_PREFIX.toString());
  }


  public static int getPrefixLength() {
    return GNSProtocol.INTERNAL_PREFIX.toString().length();
  }
}
