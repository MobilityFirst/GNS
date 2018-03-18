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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.LinkedList;
import java.util.List;
import java.util.prefs.BackingStoreException;

import edu.umass.cs.gnscommon.SharedGuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.EncryptionException;
import edu.umass.cs.gnsclient.client.GNSClientConfig;
import edu.umass.cs.gnsclient.client.util.keystorage.AbstractKeyStorage;
import edu.umass.cs.gnsclient.client.util.keystorage.JavaPreferencesKeyStore;
import edu.umass.cs.gnsclient.client.util.keystorage.SimpleKeyStore;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.utils.Config;
import org.apache.cassandra.config.EncryptionOptions;

import javax.xml.bind.DatatypeConverter;


/**
 * @author westy
 */
public class KeyPairUtils {

  /**
   * Check whether we are on an Android platform or not
   */
  public static final boolean IS_ANDROID = System.getProperty("java.vm.name").equalsIgnoreCase("Dalvik");

  //private static final SimpleKeyStore KEYSTORE = new SimpleKeyStore();
  //private static Preferences KEYSTORE = Preferences.userRoot().node(KeyPairUtils.class.getName());
  // Added these and made the shorter because there is a maximum key length limit!
  private static final String GUID = "GU";
  private static final String PUBLIC = "PU";
  private static final String PRIVATE = "PR";
  private static final String DEFAULT_GUID = "D_GUID";
  private static final String DEFAULT_GNS = "D_GNS";

  private static final Object SINGLETON_OBJ_LOCK = new Object();

  private static AbstractKeyStorage keyStorageObj = null;

  /**
   * Retrieves the public/private key pair for the given user.
   *
   * @param gnsName the name of the GNS instance (e.g. "server.gns.name:8080")
   * @param username the user name
   * @return the GNSProtocol.GUID.toString() entry if found, null otherwise
   */
  public static GuidEntry getGuidEntry(String gnsName, String username) {
    if (username == null) {
      return null;
    }

    if (IS_ANDROID) {
      return KeyPairUtilsAndroid.getGuidEntryFromPreferences(gnsName, username);
    }

    createSingleton();

    String guid = keyStorageObj.get(generateKey(gnsName, username, GUID), "");
    String publicString = keyStorageObj.get(generateKey(gnsName, username, PUBLIC), "");
    String privateString = keyStorageObj.get(generateKey(gnsName, username, PRIVATE), "");
    if (!guid.isEmpty() && !publicString.isEmpty() && !privateString.isEmpty()) {
      try {
        byte[] encodedPublicKey = DatatypeConverter.parseHexBinary(publicString);
        //byte[] encodedPublicKey = ByteUtils.hexStringToByteArray(publicString);
        byte[] encodedPrivateKey = DatatypeConverter.parseHexBinary(privateString);
        //byte[] encodedPrivateKey = ByteUtils.hexStringToByteArray(privateString);
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


  /**
   * Remove the public/private key pair from preferences for the given user.
   *
   * @param gnsName the name of the GNS instance (e.g. "server.gns.name:8080")
   * @param username the user name
   */
  public static void removeKeyPair(String gnsName, String username) {
    if (IS_ANDROID) {
      KeyPairUtilsAndroid.removeKeyPairFromPreferences(gnsName, username);
      return;
    }

    createSingleton();

    keyStorageObj.remove(generateKey(gnsName, username, PUBLIC));
    keyStorageObj.remove(generateKey(gnsName, username, PRIVATE));
    keyStorageObj.remove(generateKey(gnsName, username, GUID));
  }

  /**
   * Saves the public/private key pair to preferences for the given user.
   *
   * @param gnsName the name of the GNS instance (e.g. "server.gns.name:8080")
   * @param username the user name or human readable name
   */
  public static GuidEntry generateAndSaveKeyPair(String gnsName, String username)
          throws NoSuchAlgorithmException, EncryptionException {

    KeyPair keyPair = KeyPairGenerator.getInstance(GNSProtocol.RSA_ALGORITHM.toString())
            .generateKeyPair();
    String guid = SharedGuidUtils.createGuidStringFromPublicKey(keyPair
            .getPublic().getEncoded());
    GuidEntry guidEntry = new GuidEntry(gnsName, guid, keyPair.getPublic(), keyPair.getPrivate());
    saveKeyPair(gnsName, username, guid, keyPair);
    return guidEntry;
  }

  /**
   * Saves the public/private key pair to preferences for the given user.
   *
   * @param gnsName the name of the GNS instance (e.g. "server.gns.name:8080")
   * @param username the user name or human readable name
   * @param guid the GNSProtocol.GUID.toString() value
   * @param keyPair public and private keys for that GNSProtocol.GUID.toString()
   */
  public static void saveKeyPair(String gnsName, String username, String guid, KeyPair keyPair) {
    if (IS_ANDROID) {
      KeyPairUtilsAndroid.saveKeyPairToPreferences(gnsName, username, guid, keyPair);
      return;
    }

    createSingleton();

    String publicString =  DatatypeConverter.printHexBinary(keyPair.getPublic().getEncoded());
    String privateString =  DatatypeConverter.printHexBinary(keyPair.getPrivate().getEncoded());
    //String publicString = ByteUtils.toHex(keyPair.getPublic().getEncoded());
    //String privateString = ByteUtils.toHex(keyPair.getPrivate().getEncoded());
    keyStorageObj.put(generateKey(gnsName, username, PUBLIC), publicString);
    keyStorageObj.put(generateKey(gnsName, username, PRIVATE), privateString);
    keyStorageObj.put(generateKey(gnsName, username, GUID), guid);
  }

  /**
   * A useful tool when you want to use an existing guid with another gns server.
   *
   * @param sourceGNSName
   * @param username
   * @param destGNSName
   */
  public static void copyKeyPair(String sourceGNSName, String username, String destGNSName) {
    GuidEntry entry = getGuidEntry(sourceGNSName, username);
    saveKeyPair(destGNSName, entry.getEntityName(), entry.getGuid(),
            new KeyPair(entry.getPublicKey(), entry.getPrivateKey()));
  }

  /**
   * Set the default GNSProtocol.GUID.toString() to use in the user preferences (usually the account
 GNSProtocol.GUID.toString())
   *
   * @param gnsName the name of the GNS instance (e.g. "server.gns.name:8080")
   * @param username the alias of the default GNSProtocol.GUID.toString() to use
   */
  public static void setDefaultGuidEntry(String gnsName, String username) {
    if (IS_ANDROID) {
      KeyPairUtilsAndroid.setDefaultGuidEntryToPreferences(gnsName, username);
    } else {

      createSingleton();
      keyStorageObj.put(gnsName + DEFAULT_GUID, username);
    }
  }

  /**
   * Set the default GNSProtocol.GUID.toString() to use in the user preferences (usually the account
 GNSProtocol.GUID.toString())
   *
   * @param gnsName the name of the GNS instance (e.g. "server.gns.name:8080")
   */
  public static void removeDefaultGuidEntry(String gnsName) {
    if (IS_ANDROID) {
      KeyPairUtilsAndroid.removeDefaultGuidEntryFromPreferences(gnsName);
    } else {

      createSingleton();
      keyStorageObj.remove(gnsName + DEFAULT_GUID);
    }
  }

  /**
   * Return the GNSProtocol.GUID.toString() entry for the default GNSProtocol.GUID.toString() set in the user preferences (if
 any)
   *
   * @param gnsName the name of the GNS instance (e.g. "server.gns.name:8080")
   * @return the default GNSProtocol.GUID.toString() entry or null if not set or invalid
   */
  public static GuidEntry getDefaultGuidEntry(String gnsName) {
    if (IS_ANDROID) {
      return KeyPairUtilsAndroid.getDefaultGuidEntryFromPreferences(gnsName);
    } else {
      createSingleton();
      System.out.println(gnsName);
      return getGuidEntry(gnsName, keyStorageObj.get(gnsName + DEFAULT_GUID, null));
    }
  }

  /**
   * Sets the default GNS to use in the user preferences. Syntax to use is with
   * a colon between the host and port so that the string can be re-used as is
   * in the GnsConnect command.
   *
   * @param gnsHostPort a string of host:port:disableSSLBoolean
   */
  @Deprecated
  public static void setDefaultGns(String gnsHostPort) {
    if (IS_ANDROID) {
      KeyPairUtilsAndroid.setDefaultGnsToPreferences(gnsHostPort);
    } else {
      createSingleton();
      keyStorageObj.put(DEFAULT_GNS, gnsHostPort);
    }
  }

  /**
   * Return the default GNS Host:Port:disableSSLBoolean as a String if defined (null if not
   * defined)
   *
   * @return the default GNS saved in the user prefences as a string of host:port:disableSSLBoolean
   */
  @Deprecated
  public static String getDefaultGns() {
    if (IS_ANDROID) {
      return KeyPairUtilsAndroid.getDefaultGnsFromPreferences();
    } else {
      createSingleton();
      return keyStorageObj.get(DEFAULT_GNS, null);
    }
  }

  /**
   * Remove the default GNS to use in the user preferences.
   */
  @Deprecated
  public static void removeDefaultGns() {
    if (IS_ANDROID) {
      KeyPairUtilsAndroid.removeDefaultGnsFromPreferences();
    } else {
      createSingleton();
      keyStorageObj.remove(DEFAULT_GNS);
    }
  }

  /**
   * Reads a private key from a PKCS#8 formatted file
   *
   * @param filename
   * @return the private key
   * @throws IOException
   * @throws InvalidKeySpecException
   * @throws NoSuchAlgorithmException
   */
  public static PrivateKey getPrivateKeyFromPKCS8File(String filename) throws IOException, InvalidKeySpecException,
          NoSuchAlgorithmException {
    File f = new File(filename);
    byte[] keyBytes;
    try (DataInputStream dis = new DataInputStream(new FileInputStream(f))) {
      keyBytes = new byte[(int) f.length()];
      dis.readFully(keyBytes);
    }

    PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);
    KeyFactory kf = KeyFactory.getInstance(GNSProtocol.RSA_ALGORITHM.toString());
    return kf.generatePrivate(spec);
  }

  /**
   * Saves a private key to a PKCS#8 formatted file
   *
   * @param privateKey private key to save
   * @param filename file to save the key to
   * @return true if the key is written
   */
  public static boolean writePrivateKeyToPKCS8File(PrivateKey privateKey, String filename) {
    byte[] keyBytes = privateKey.getEncoded();
    FileOutputStream fos = null;
    DataOutputStream dos = null;
    try {
      fos = new FileOutputStream(filename);
      dos = new DataOutputStream(fos);
      dos.write(keyBytes);
    } catch (FileNotFoundException e) {
      System.out.println("File not found: " + e);
      return false;
    } catch (IOException ioe) {
      System.out.println("Error while writing to file: " + ioe);
      return false;
    } finally {
      try {
        if (dos != null) {
          dos.close();
        }
        if (fos != null) {
          fos.close();
        }
      } catch (Exception e) {
        System.out.println("Error while closing streams: " + e);
      }
    }
    return true;
  }

  public static String publicKeyToBase64ForGuid(GuidEntry guid) {
    byte[] publicKeyBytes = guid.getPublicKey().getEncoded();
    return Base64.encodeToString(publicKeyBytes, false);
  }
  /**
   * Return the list of all GUIDs stored locally that belong to a particular GNS
   * instance.
   *
   * This seems kind of hokey.
   *
   * @param gnsName the GNS host:port
   * @return all matching GUIDs
   */
  public static List<GuidEntry> getAllGuids(String gnsName) {
    if (IS_ANDROID) {
      return KeyPairUtilsAndroid.getAllGuids(gnsName);
    }
    createSingleton();

    List<GuidEntry> guids = new LinkedList<>();
    //try {

    String[] keys = keyStorageObj.keys();
    for (String key : keys) {
      if ((gnsName == null || key.startsWith(gnsName + "#")) && key.endsWith(GUID)) {
        //System.out.println(key);
        //userPreferences.remove(key);
        String actualGNSName = gnsName != null ? gnsName : key.substring(0, key.indexOf('#'));
        String userName = key.substring(key.indexOf('#') + 1, key.lastIndexOf('-'));
        GuidEntry guid = getGuidEntry(actualGNSName, userName);
        if (guid != null) // (will be null for default)
        {
          guids.add(guid);
        }
      }
    }
//    } catch (BackingStoreException e) {
//      e.printStackTrace();
//    }

    return guids;
  }

  // limits the key to Preferences.MAX_KEY_LENGTH characters, dropping them off the front
  private static String generateKey(String gnsName, String username, String suffix) {
    String string = gnsName + "#" + username + "-" + suffix;
    if (string.length() > SimpleKeyStore.MAX_KEY_LENGTH) {
      return string.substring(string.length() - SimpleKeyStore.MAX_KEY_LENGTH);
    } else {
      return string;
    }
  }

  private static void createSingleton() {
    // Doing a check before so that the lock is 
    // not unnecessarily acquired.
    if (keyStorageObj == null) {
      synchronized (SINGLETON_OBJ_LOCK) {
        if (keyStorageObj == null) {
          if (Config.getGlobalBoolean(GNSClientConfig.GNSCC.USE_JAVA_PREFERENCE)) {
            keyStorageObj = new JavaPreferencesKeyStore();
          } else {
            // if java preferences not enabled then use DerbyDB
            keyStorageObj = new SimpleKeyStore();
          }
        }
      }
    }
    assert (keyStorageObj != null);
  }


 

  /**
   *
   * @param args
   * @throws BackingStoreException
   */
  public static void main(String args[]) throws BackingStoreException {
    createSingleton();
    if ((args.length == 1) && "-clear".equals(args[0])) {
      keyStorageObj.clear();
      //keyStore.removeNode();
      System.out.println(keyStorageObj.toString() + " cleared.");
    } else if ((args.length == 1) && "-size".equals(args[0])) {
      System.out.println(keyStorageObj.toString() + " contains " + keyStorageObj.keys().length + " keys");
    } else if (args.length < 3) {
      List<GuidEntry> allGuids = getAllGuids(args.length > 0 ? args[0] : null);
      for (GuidEntry entry : allGuids) {
        System.out.println(entry.toString());
      }
    } else if (args.length == 3) {
      copyKeyPair(args[0], args[1], args[2]);
    }
    System.exit(0);
  }

}
