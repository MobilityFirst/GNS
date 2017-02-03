
package edu.umass.cs.gnsclient.client.util;

import edu.umass.cs.gnsclient.client.util.keystorage.AbstractKeyStorage;
import edu.umass.cs.gnsclient.client.util.keystorage.IOSKeyStorage;
import edu.umass.cs.gnsclient.client.util.keystorage.JavaPreferencesKeyStore;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.exceptions.client.EncryptionException;
import edu.umass.cs.gnscommon.utils.ByteUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.LinkedList;
import java.util.List;


public class IOSKeyPairUtils {

  private static final String GUID = "GU";
  private static final String PUBLIC = "PU";
  private static final String PRIVATE = "PR";
  private static final String DEFAULT_GUID = "D_GUID";
  private static final String DEFAULT_GNS = "D_GNS";

  private static final Object SINGLETON_OBJ_LOCK = new Object();

  private static AbstractKeyStorage keyStorageObj = null;


  public static GuidEntry getGuidEntry(String gnsName, String username) {
    if (username == null) {
      return null;
    }

    createSingleton();

    String guid = keyStorageObj.get(generateKey(gnsName, username, GUID), "");
    String publicString = keyStorageObj.get(generateKey(gnsName, username, PUBLIC), "");
    String privateString = keyStorageObj.get(generateKey(gnsName, username, PRIVATE), "");
    if (!guid.isEmpty() && !publicString.isEmpty() && !privateString.isEmpty()) {
      try {
        byte[] encodedPublicKey = ByteUtils.hexStringToByteArray(publicString);
        byte[] encodedPrivateKey = ByteUtils.hexStringToByteArray(privateString);

        System.out.println( " PrivateString is " + privateString);
        System.out.println( "\n PublicString is " + publicString);

        KeyFactory keyFactory = KeyFactory.getInstance(GNSProtocol.RSA_ALGORITHM.toString());
        X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(encodedPublicKey);
        PublicKey publicKey = keyFactory.generatePublic(publicKeySpec);
        PKCS8EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(encodedPrivateKey);
        PrivateKey privateKey = keyFactory.generatePrivate(privateKeySpec);
        return new GuidEntry(username, guid, publicKey, privateKey);
      } catch (NoSuchAlgorithmException | InvalidKeySpecException | EncryptionException e) {
        System.out.println(e.toString());
        return null;
      }
    } else {
      return null;
    }
  }



  public static void saveKeyPair(String gnsName, String username, String guid, KeyPair keyPair) {

    createSingleton();
    String publicString = ByteUtils.toHex(keyPair.getPublic().getEncoded());
    String privateString = ByteUtils.toHex(keyPair.getPrivate().getEncoded());

    keyStorageObj.put(generateKey(gnsName, username, PUBLIC), publicString);
    keyStorageObj.put(generateKey(gnsName, username, PRIVATE), privateString);
    keyStorageObj.put(generateKey(gnsName, username, GUID), guid);
  }


  @Deprecated
  public static void setDefaultGns(String gnsHostPort) {

      createSingleton();
      keyStorageObj.put(DEFAULT_GNS, gnsHostPort);

  }


  @Deprecated
  public static String getDefaultGns() {

      createSingleton();
      return keyStorageObj.get(DEFAULT_GNS, null);

  }


  @Deprecated
  public static void removeDefaultGns() {

      createSingleton();
      keyStorageObj.remove(DEFAULT_GNS);

  }





  private static String generateKey(String gnsName, String username, String suffix) {
    String string = gnsName + "#" + username + "-" + suffix;
    if (string.length() > 2048) {
      return string.substring(string.length() - 2048);
    } else {
      return string;
    }
  }

  private static void createSingleton() {

    if (keyStorageObj == null) {
      synchronized (SINGLETON_OBJ_LOCK) {
        if (keyStorageObj == null) {
            keyStorageObj = new JavaPreferencesKeyStore();
        }
      }
    }
    assert (keyStorageObj != null);
  }


  public static void main(String args[]) throws Exception{


  }

}
