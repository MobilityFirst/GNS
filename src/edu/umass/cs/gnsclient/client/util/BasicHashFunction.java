
package edu.umass.cs.gnsclient.client.util;

import edu.umass.cs.gnscommon.utils.ByteUtils;



public abstract class BasicHashFunction implements HashFunction {
  

  @Override
  public long hashToLong(String key) {
    // assumes the first byte is the most significant
    return ByteUtils.byteArrayToLong(hash(key));
  }
  
}
