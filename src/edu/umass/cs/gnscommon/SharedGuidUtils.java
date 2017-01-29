
package edu.umass.cs.gnscommon;

import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.ShaOneHashFunction;
import org.json.JSONArray;
import org.json.JSONException;

import javax.xml.bind.DatatypeConverter;
import java.util.HashSet;
import java.util.Set;


public class SharedGuidUtils {


  public static String createGuidStringFromPublicKey(byte[] keyBytes) {
    byte[] publicKeyDigest = ShaOneHashFunction.getInstance().hash(keyBytes);
    return DatatypeConverter.printHexBinary(publicKeyDigest);
    //return ByteUtils.toHex(publicKeyDigest);
  }


  public static String createGuidStringFromBase64PublicKey(String publicKey) throws IllegalArgumentException {
    byte[] publickeyBytes = Base64.decode(publicKey);
    if (publickeyBytes == null) { // bogus public key
      throw new IllegalArgumentException();
    }
    return createGuidStringFromPublicKey(publickeyBytes);
  }


  public static JSONArray convertPublicKeysToGuids(JSONArray publicKeys) throws JSONException {
    JSONArray guids = new JSONArray();
    for (int i = 0; i < publicKeys.length(); i++) {
      // Special case
      try {
        if (publicKeys.getString(i).equals(GNSProtocol.ALL_GUIDS.toString())) {
          guids.put(GNSProtocol.ALL_GUIDS.toString());
        } else {
          guids.put(createGuidStringFromBase64PublicKey(publicKeys.getString(i)));
        }
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
        guids.add(createGuidStringFromBase64PublicKey(publicKey));
      } catch (IllegalArgumentException e) {
        // ignore any bogus publicKeys
      }
    }
    return guids;
  }


  public static String findPublicKeyForGuid(String guid, Set<String> publicKeys) {
    if (guid != null) {
      for (String publicKey : publicKeys) {
        try {
          if (guid.equals(createGuidStringFromBase64PublicKey(publicKey))) {
            return publicKey;
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
