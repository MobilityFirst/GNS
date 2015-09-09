/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport;

/**
 *
 * @author westy
 */
public interface HashFunction {
  
  /**
   * Hashes a string to a byte array.
   * 
   * @param key
   * @return
   */
  public byte[] hash(String key);
   
   // public byte[] hash(byte[] bytes);
   
  /**
   * Hashes a string to a long value.
   * 
   * @param key
   * @return
   */
     
   public long hashToLong(String key);
  
}
