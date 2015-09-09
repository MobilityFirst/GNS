/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport;

import edu.umass.cs.gns.util.Base64;
import edu.umass.cs.gns.util.ByteUtils;
import java.util.HashSet;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONException;

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
   * @return a guid string
   */
  public static String createGuidStringFromPublicKey(byte[] keyBytes) {
    byte[] publicKeyDigest = SHA1HashFunction.getInstance().hash(keyBytes);
    return ByteUtils.toHex(publicKeyDigest);
  }

  /**
   * Creates a hexidecimal guid string by hashing a public key.
   * The input string is assumed to base64 encoded. 
   * 
   * @param publicKey
   * @return a guid string
   * @throws IllegalArgumentException
   */
  public static String createGuidStringFromPublicKey(String publicKey) throws IllegalArgumentException {
    byte[] publickeyBytes = Base64.decode(publicKey);
    if (publickeyBytes == null) { // bogus public key
      throw new IllegalArgumentException();
    }
    byte[] publicKeyDigest = SHA1HashFunction.getInstance().hash(publickeyBytes);
    return ByteUtils.toHex(publicKeyDigest);
  }

  /**
   * Converts a JSONArray of publicKeys to a JSONArray of guids using
   * @link{createGuidStringFromPublicKey}.
   * 
   * @param publicKeys
   * @return JSONArray of guids
   * @throws JSONException
   */
  public static JSONArray convertPublicKeysToGuids(JSONArray publicKeys) throws JSONException {
    JSONArray guids = new JSONArray();
    for (int i = 0; i < publicKeys.length(); i++) {
      try {
        guids.put(createGuidStringFromPublicKey(publicKeys.getString(i)));
      } catch (IllegalArgumentException e) {
        // ignore any bogus publicKeys
      }
    }
    return guids;
  }
  
  /**
   * Converts a set of publicKeys to a set of guids using
   * @link{createGuidStringFromPublicKey}.
   * 
   * @param publicKeys
   * @return set of guids
   */
  public static Set<String> convertPublicKeysToGuids(Set<String> publicKeys) {
    Set<String> guids = new HashSet<>();
    for (String publicKey : publicKeys) {
      try {
        guids.add(createGuidStringFromPublicKey(publicKey));
      } catch (IllegalArgumentException e) {
        // ignore any bogus publicKeys
      }
    }
    return guids;
  }

  /**
   * Finds a public key that corresponds to a guid in a set of public keys.
   *
   * @param guid
   * @param publicKeys
   * @return a public key
   */
  public static String findPublicKeyForGuid(String guid, Set<String> publicKeys) {
    if (guid != null) {
      for (String publicKey : publicKeys) {
        try {
          if (guid.equals(createGuidStringFromPublicKey(publicKey))) {
            return publicKey;
          }
        } catch (IllegalArgumentException e) {
          // ignore any bogus publicKeys
        }
      }
    }
    return null;
  }

  /**
   * Returns true if a public key that corresponds to a guid is in a set of public keys.
   * 
   * @param guid
   * @param publicKeys
   * @return true or false
   */
  public static boolean publicKeyListContainsGuid(String guid, Set<String> publicKeys) {
    return findPublicKeyForGuid(guid, publicKeys) != null;
  }
}
