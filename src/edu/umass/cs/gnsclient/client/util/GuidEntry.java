
package edu.umass.cs.gnsclient.client.util;

import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.exceptions.client.EncryptionException;
import edu.umass.cs.gnscommon.utils.Base64;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;


public class GuidEntry extends BasicGuidEntry implements Serializable {

  private static final long serialVersionUID = 2326392043474125897L;
  private final PrivateKey privateKey;


  public GuidEntry(String entityName, String guid, PublicKey publicKey,
          PrivateKey privateKey) throws EncryptionException {
    super(entityName, guid, publicKey);
    this.privateKey = privateKey;
  }


  public GuidEntry(ObjectInputStream s) throws IOException, EncryptionException {
    //readObject(s);
	  super(s.readUTF(), s.readUTF(), s.readUTF());
	  this.privateKey = generatePrivateKey(s.readUTF());
  }


  public PrivateKey getPrivateKey() {
    return privateKey;
  }


  @Override
  public boolean equals(Object o) {
    if (o == null || !(o instanceof GuidEntry)) {
      return false;
    }
    GuidEntry other = (GuidEntry) o;
    if (entityName == null && other.getEntityName() != null) {
      return false;
    }
    if (entityName != null && !entityName.equals(other.getEntityName())) {
      return false;
    }
    if (!publicKey.equals(other.getPublicKey())) {
      return false;
    }
    if (privateKey == null && other.getPrivateKey() != null) {
      return false;
    }
    if (privateKey == null) {
      return true;
    } else {
      return privateKey.equals(other.privateKey);
    }
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 17 * hash + (this.entityName != null ? this.entityName.hashCode() : 0);
    hash = 17 * hash + (this.guid != null ? this.guid.hashCode() : 0);
    hash = 17 * hash + (this.publicKey != null ? this.publicKey.hashCode() : 0);
    hash = 17 * hash + (this.privateKey != null ? this.privateKey.hashCode() : 0);
    return hash;
  }


  public void writeObject(ObjectOutputStream s) throws IOException {
    s.writeUTF(entityName);
    s.writeUTF(guid);
    s.writeUTF(Base64.encodeToString(publicKey.getEncoded(), true));
    s.writeUTF(Base64.encodeToString(privateKey.getEncoded(), true));
  }


  private static PrivateKey generatePrivateKey(String encodedPrivate)
          throws EncryptionException {
    byte[] encodedPrivateKey = Base64.decode(encodedPrivate);

    try {
      return KeyFactory.getInstance(GNSProtocol.RSA_ALGORITHM.toString()).generatePrivate(new PKCS8EncodedKeySpec(
              encodedPrivateKey));
    } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
      throw new EncryptionException("Failed to generate keypair", e);
    }
  }


  // Test code

public static void main(String[] args) throws IOException, Exception {

  }

}
