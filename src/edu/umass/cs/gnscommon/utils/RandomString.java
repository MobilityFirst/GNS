
package edu.umass.cs.gnscommon.utils;

import java.util.Random;


public class RandomString {

  private static final Random RANDOM = new Random(System.currentTimeMillis());
  private static final String CHARACTERS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";


  public static String randomString(int len) {
    StringBuilder sb = new StringBuilder(len);
    for (int i = 0; i < len; i++) {
      sb.append(CHARACTERS.charAt(RANDOM.nextInt(CHARACTERS.length())));
    }
    return sb.toString();
  }
  
}
