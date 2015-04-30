/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.clientsupport;

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
   * @return
   */
  public static String createGuidStringFromPublicKey(byte[] keyBytes) {
    byte[] publicKeyDigest = SHA1HashFunction.getInstance().hash(keyBytes);
    return ByteUtils.toHex(publicKeyDigest);
  }

  public static String createGuidStringFromPublicKey(String publicKey) throws IllegalArgumentException {
    byte[] publickeyBytes = Base64.decode(publicKey);
    if (publickeyBytes == null) { // bogus public key
      throw new IllegalArgumentException();
    }
    byte[] publicKeyDigest = SHA1HashFunction.getInstance().hash(publickeyBytes);
    return ByteUtils.toHex(publicKeyDigest);
  }

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

  public static String findPublicKeyForGuid(String guid, Set<String> publicKeys) {
    if (guid != null) {
      for (Object publicKey : publicKeys) {
        try {
          if (guid.equals(createGuidStringFromPublicKey((String) publicKey))) {
            return (String) publicKey;
          }
        } catch (IllegalArgumentException e) {
          // ignore any bogus publicKeys
        }
      }
    }
    return null;
  }

  public static boolean publicKeyListContainsGuid(String guid, Set<String> publicKeys) {
    return findPublicKeyForGuid(guid, publicKeys) != null;
  }
}
