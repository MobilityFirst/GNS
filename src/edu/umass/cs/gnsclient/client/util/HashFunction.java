
package edu.umass.cs.gnsclient.client.util;


public interface HashFunction
{


  public byte[] hash(String key);

  // public byte[] hash(byte[] bytes);



  public long hashToLong(String key);

}
