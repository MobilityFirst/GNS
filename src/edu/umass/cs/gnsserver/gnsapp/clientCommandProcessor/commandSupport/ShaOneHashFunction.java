
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport;

import edu.umass.cs.gnsserver.main.GNSConfig;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


public class ShaOneHashFunction extends AbstractHashFunction {

  private MessageDigest messageDigest;

  private ShaOneHashFunction() {

    try {
      messageDigest = MessageDigest.getInstance("SHA1");
    } catch (NoSuchAlgorithmException e) {
      GNSConfig.getLogger().severe("Problem initializing digest: " + e);
    }
  }


  @Override
  public synchronized byte[] hash(String key) {
    try {
      messageDigest.update(key.getBytes("UTF-8"));
      return messageDigest.digest();
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }

  }


  public synchronized byte[] hash(byte[] bytes) {
    messageDigest.update(bytes);
    return messageDigest.digest();
  }


  public static ShaOneHashFunction getInstance() {
    return SHA1HashFunctionHolder.INSTANCE;
  }

  private static class SHA1HashFunctionHolder {

    private static final ShaOneHashFunction INSTANCE = new ShaOneHashFunction();
  }
}
