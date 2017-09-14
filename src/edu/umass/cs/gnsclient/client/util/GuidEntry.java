/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy, Emmanuel Cecchet
 *
 */
package edu.umass.cs.gnsclient.client.util;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientCommands;

import edu.umass.cs.gnscommon.GNSProtocol;
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

import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnscommon.exceptions.client.EncryptionException;

import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 * This class defines a GuidEntry which contains the alias, guid, public and private key.
 * It encapsulates the values the client needs to send and receive queries from the server.
 * Most or all of the client calls use this data structure.
 * This class is Serializable so that it can be read and written to the local file system.
 * IT WILL NEVER BE WRITTEN REMOTELY AS WE DON'T TRANSMIT OR STORE PRIVATE KEYS REMOTELY.
 *
 */
public class GuidEntry extends BasicGuidEntry implements Serializable {

  private static final long serialVersionUID = 2326392043474125897L;
  private final PrivateKey privateKey;

  /**
   * Creates a new <code>GuidEntry</code> object
   *
   * @param entityName entity name (usually an email)
   * @param guid Guid generated by the GNS
   * @param publicKey public key
   * @param privateKey private key
 * @throws EncryptionException 
   */
  public GuidEntry(String entityName, String guid, PublicKey publicKey,
          PrivateKey privateKey) throws EncryptionException {
    super(entityName, guid, publicKey);
    this.privateKey = privateKey;
  }

  /**
   * Creates a new <code>GuidEntry</code> object from an input stream (see
   * {@link #writeObject(ObjectOutputStream)}
   *
   * @param s the stream to read from
   * @throws IOException if an error occurs
 * @throws EncryptionException 
   */
  public GuidEntry(ObjectInputStream s) throws IOException, EncryptionException {
    //readObject(s);
	  super(s.readUTF(), s.readUTF(), s.readUTF());
	  this.privateKey = generatePrivateKey(s.readUTF());
  }

  /**
   * Returns the privateKey value.
   *
   * @return Returns the privateKey.
   */
  public PrivateKey getPrivateKey() {
    return privateKey;
  }

  /**
   * @param o
   * @return @see java.lang.Object#equals(java.lang.Object)
   */
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

  /**
   * Write this GNSProtocol.GUID.toString() entry to an output stream
   *
   * @param s the stream to write to
   * @throws IOException if an error occurs
   */
  public void writeObject(ObjectOutputStream s) throws IOException {
    s.writeUTF(entityName);
    s.writeUTF(guid);
    s.writeUTF(Base64.encodeToString(publicKey.getEncoded(), true));
    s.writeUTF(Base64.encodeToString(privateKey.getEncoded(), true));
  }

  // arun: removed this method to make all fields final
//  private void readObject(ObjectInputStream s) throws IOException {
//    entityName = s.readUTF();
//    guid = s.readUTF();
//    KeyPair keypair;
//    try {
//      keypair = generateKeyPair(s.readUTF(), s.readUTF());
//      publicKey = keypair.getPublic();
//      privateKey = keypair.getPrivate();
//    } catch (EncryptionException e) {
//      throw new IOException(e);
//    }
//  }

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


  @SuppressWarnings("unused")
  private static KeyPair generateKeyPair(String encodedPublic, String encodedPrivate)
          throws EncryptionException {
    byte[] encodedPublicKey = Base64.decode(encodedPublic);
    byte[] encodedPrivateKey = Base64.decode(encodedPrivate);

    try {
      KeyFactory keyFactory = KeyFactory.getInstance(GNSProtocol.RSA_ALGORITHM.toString());
      X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(
              encodedPublicKey);
      PublicKey thePublicKey = keyFactory.generatePublic(publicKeySpec);

      PKCS8EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(
              encodedPrivateKey);
      PrivateKey thePrivateKey = keyFactory.generatePrivate(privateKeySpec);
      return new KeyPair(thePublicKey, thePrivateKey);
    } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
      throw new EncryptionException("Failed to generate keypair", e);
    }
  }

  // Test code
  /**
 * @param args
 * @throws IOException
 * @throws Exception
 */
public static void main(String[] args) throws IOException, Exception {
    String name = "testGuid@gigapaxos.net";
    String password = "123";
    String file_name = "guid";

    GNSClientCommands client = new GNSClientCommands(new GNSClient());

    GuidEntry guidEntry = client.accountGuidCreate(name, password);

    FileOutputStream fos = new FileOutputStream(file_name);
    ObjectOutputStream os = new ObjectOutputStream(fos);
    guidEntry.writeObject(os);
    os.flush(); // Important to flush

    FileInputStream fis = new FileInputStream(file_name);
    ObjectInputStream ois = new ObjectInputStream(fis);

    GuidEntry newEntry = new GuidEntry(ois);
    System.out.println(newEntry);
  }

}
