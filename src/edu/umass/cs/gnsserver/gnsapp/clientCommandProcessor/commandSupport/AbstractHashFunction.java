
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport;

import edu.umass.cs.gnscommon.utils.ByteUtils;


public abstract class AbstractHashFunction implements HashFunction {
  

  @Override
  public long hashToLong(String key) {
    // assumes the first byte is the most significant
    return ByteUtils.byteArrayToLong(hash(key));
  }
  
}
