
package edu.umass.cs.gnsclient.client.util;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


public class SHA1HashFunction extends BasicHashFunction {

  private MessageDigest hashfunction;

  private SHA1HashFunction() {
    try {
      hashfunction = MessageDigest.getInstance("SHA1");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }


  @Override
  public synchronized byte[] hash(String key) {
    try {
      hashfunction.update(key.getBytes("UTF-8"));
      return hashfunction.digest();
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }

  }


  public synchronized byte[] hash(byte[] bytes) {
    hashfunction.update(bytes);
    return hashfunction.digest();
  }


  public static SHA1HashFunction getInstance() {
    return SHA1HashFunctionHolder.INSTANCE;
  }

  private static class SHA1HashFunctionHolder {

    private static final SHA1HashFunction INSTANCE = new SHA1HashFunction();
  }
}
