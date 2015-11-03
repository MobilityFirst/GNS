/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport;

/**
 *
 * @author westy
 */
public interface HashFunction {
  
  /**
   * Hashes a string to a byte array.
   * 
   * @param key
   * @return a byte array
   */
  public byte[] hash(String key);
   
   // public byte[] hash(byte[] bytes);
   
  /**
   * Hashes a string to a long value.
   * 
   * @param key
   * @return a long
   */
     
   public long hashToLong(String key);
  
}
