/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport;

import edu.umass.cs.gns.utils.ByteUtils;

/**
 *
 * @author westy
 */
public abstract class AbstractHashFunction implements HashFunction {
  
  /**
   *  Hashes a string to a long.
   * 
   * @param key
   * @return a long
   */
  @Override
  public long hashToLong(String key) {
    // assumes the first byte is the most significant
    return ByteUtils.byteArrayToLong(hash(key));
  }
  
}
