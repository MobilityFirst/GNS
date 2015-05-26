/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport;

import edu.umass.cs.gns.main.GNS;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
/**
 *
 * @author westy
 */
public class SHA1HashFunction extends BasicHashFunction {

  MessageDigest hashfunction;

  private SHA1HashFunction() {

    try {
      hashfunction = MessageDigest.getInstance("SHA1");
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
      GNS.getLogger().severe("Error: " + e);
    }
  }

  /**
   * Hash the string.
   * 
   * @param key
   * @return
   */
  @Override
  public synchronized byte[] hash(String key) {
    hashfunction.update(key.getBytes());
    return hashfunction.digest();

  }
  
  /**
   * Hash the byte array.
   * 
   * @param bytes
   * @return
   */
  public synchronized byte[] hash(byte[] bytes) {
    hashfunction.update(bytes);
    return hashfunction.digest();
  }

  /**
   * Returns the single instance of the SHA1HashFunction.
   * 
   * @return
   */
  public static SHA1HashFunction getInstance() {
    return SHA1HashFunctionHolder.INSTANCE;
  }

  private static class SHA1HashFunctionHolder {

    private static final SHA1HashFunction INSTANCE = new SHA1HashFunction();
  }
}
