
package edu.umass.cs.gnsclient.client.util;

import edu.umass.cs.gnscommon.utils.Base64;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


public class Password {

  private static final String SALT = "42shabiz";


  public static String encryptAndEncodePassword(String password, String alias) throws NoSuchAlgorithmException {
    MessageDigest md = MessageDigest.getInstance("SHA-256");
    md.update((password + SALT + alias).getBytes());
    return Base64.encodeToString(md.digest(), false);
  }

}
