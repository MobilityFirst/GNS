/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport;

import edu.umass.cs.gnsserver.main.GNS;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
/**
 *
 * @author westy
 */
public class SHA1HashFunction extends AbstractHashFunction {

  private MessageDigest messageDigest;

  private SHA1HashFunction() {

    try {
      messageDigest = MessageDigest.getInstance("SHA1");
    } catch (NoSuchAlgorithmException e) {
      GNS.getLogger().severe("Problem initializing digest: " + e);
    }
  }

  /**
   * Hash the string.
   * 
   * @param key
   * @return a byte array
   */
  @Override
  public synchronized byte[] hash(String key) {
    messageDigest.update(key.getBytes());
    return messageDigest.digest();

  }
  
  /**
   * Hash the byte array.
   * 
   * @param bytes
   * @return a byte array
   */
  public synchronized byte[] hash(byte[] bytes) {
    messageDigest.update(bytes);
    return messageDigest.digest();
  }

  /**
   * Returns the single instance of the SHA1HashFunction.
   * 
   * @return a SHA1HashFunction instance
   */
  public static SHA1HashFunction getInstance() {
    return SHA1HashFunctionHolder.INSTANCE;
  }

  private static class SHA1HashFunctionHolder {

    private static final SHA1HashFunction INSTANCE = new SHA1HashFunction();
  }
}
