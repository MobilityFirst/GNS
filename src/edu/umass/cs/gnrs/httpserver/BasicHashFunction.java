/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gnrs.httpserver;

import edu.umass.cs.gnrs.util.MoreUtils;

/**
 *
 * @author westy
 */
public abstract class BasicHashFunction implements HashFunction {
  
  @Override
  public long hashToLong(String key) {
    // assumes the first byte is the most significant
    return MoreUtils.byteArrayToLong(hash(key));
  }
  
}
