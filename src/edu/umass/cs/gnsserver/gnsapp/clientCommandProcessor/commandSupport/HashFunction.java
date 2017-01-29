
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport;


public interface HashFunction {
  

  public byte[] hash(String key);
   
   // public byte[] hash(byte[] bytes);
   

     
   public long hashToLong(String key);
  
}
