
package edu.umass.cs.gnsclient.client.util;

import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnscommon.exceptions.client.EncryptionException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import org.json.JSONException;
import org.json.JSONObject;


public class BasicGuidEntry {


  public final String entityName;


  public final String guid;


  public final PublicKey publicKey;


  public BasicGuidEntry(String entityName, String guid, PublicKey publicKey) throws EncryptionException {
    this.entityName = entityName;
    this.guid = guid;
    this.publicKey = publicKey;
  }


  public BasicGuidEntry(String entityName, String guid, String encodedPublicKey) throws EncryptionException {
    this.entityName = entityName;
    this.guid = guid;
    this.publicKey = generatePublicKey(encodedPublicKey);
  }
  

  public BasicGuidEntry (JSONObject json) throws JSONException, EncryptionException {
    this.entityName = json.getString(GNSProtocol.GUID_RECORD_NAME.toString());
    this.guid = json.getString(GNSProtocol.GUID_RECORD_GUID.toString());
    this.publicKey = generatePublicKey(json.getString(GNSProtocol.GUID_RECORD_PUBLICKEY.toString()));
  } 


  public String getEntityName() {
    return entityName;
  }


  public String getGuid() {
    return guid;
  }


  public PublicKey getPublicKey() {
    return publicKey;
  }
  

  public String getPublicKeyString() {
    byte[] publicKeyBytes = publicKey.getEncoded();
    return Base64.encodeToString(publicKeyBytes, false);
  }

  @Override
  public String toString() {
    return entityName + " (" + guid + ")";
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || !(o instanceof BasicGuidEntry)) {
      return false;
    }
    BasicGuidEntry other = (BasicGuidEntry) o;
    if (entityName == null && other.getEntityName() != null) {
      return false;
    }
    if (entityName != null && !entityName.equals(other.getEntityName())) {
      return false;
    }
    return !publicKey.equals(other.getPublicKey());
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 97 * hash + (this.entityName != null ? this.entityName.hashCode() : 0);
    hash = 97 * hash + (this.guid != null ? this.guid.hashCode() : 0);
    hash = 97 * hash + (this.publicKey != null ? this.publicKey.hashCode() : 0);
    return hash;
  }
  
  private static PublicKey generatePublicKey(String encodedPublic)
          throws EncryptionException {
    byte[] encodedPublicKey = Base64.decode(encodedPublic);

    try {
      KeyFactory keyFactory = KeyFactory.getInstance(GNSProtocol.RSA_ALGORITHM.toString());
      X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(
              encodedPublicKey);
      return keyFactory.generatePublic(publicKeySpec);

    } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
      throw new EncryptionException("Failed to generate keypair", e);
    }
  }

}
