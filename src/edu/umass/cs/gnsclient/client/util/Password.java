/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsclient.client.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 *
 * @author westy
 */
public class Password {

  // Code is duplicated in the client.
  // From the PHP code hash('sha256', $password . "42shabiz" . $username);
  private static final String SALT = "42shabiz";

  public static byte[] encryptPassword(String password, String alias) throws NoSuchAlgorithmException {
    MessageDigest md = MessageDigest.getInstance("SHA-256");
    md.update((password + SALT + alias).getBytes());
    return md.digest();
  }

}
