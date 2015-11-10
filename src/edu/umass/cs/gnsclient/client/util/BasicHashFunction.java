/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gnsclient.client.util;


/**
 *
 * @author westy
 */
public abstract class BasicHashFunction implements HashFunction {
  
  @Override
  public long hashToLong(String key) {
    // assumes the first byte is the most significant
    return ByteUtils.byteArrayToLong(hash(key));
  }
  
}
