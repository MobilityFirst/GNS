/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gnrs.httpserver;

/**
 *
 * @author westy
 */
public interface HashFunction {
  
   public byte[] hash(String key);
   
   // public byte[] hash(byte[] bytes);
   
   public long hashToLong(String key);
  
}
