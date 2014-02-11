/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.clientprotocol;

import edu.umass.cs.gns.util.ByteUtils;

/**
 *
 * @author westy
 */
public class ClientUtils {
  
  /**
   * Uses a hash function to generate a GUID from a public key string.
   * 
   * @param publicKey
   * @return 
   */
  public static String createGuidFromPublicKey(String publicKey) {
    byte[] publicKeyDigest = SHA1HashFunction.getInstance().hash(publicKey.getBytes());
    return ByteUtils.toHex(publicKeyDigest);
  }
  
}
