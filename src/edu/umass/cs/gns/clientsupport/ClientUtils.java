/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.clientsupport;

import edu.umass.cs.gns.util.ByteUtils;

/**
 *
 * @author westy
 */
public class ClientUtils {
  
  /**
   * Uses a hash function to generate a GUID from a public key string.
   * This code is duplicated in client so if you 
   * change it you should change it there as well.
   * 
   * @param keyBytes
   * @return 
   */
  public static String createGuidFromPublicKey(byte[] keyBytes) {
    byte[] publicKeyDigest = SHA1HashFunction.getInstance().hash(keyBytes);
    return ByteUtils.toHex(publicKeyDigest);
  }
}
