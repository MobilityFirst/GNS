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
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.LinkedList;
import java.util.List;
import java.util.prefs.BackingStoreException;
import edu.umass.cs.gnscommon.utils.ByteUtils;
import edu.umass.cs.gnscommon.GnsProtocol;
import edu.umass.cs.gnsclient.client.GuidEntry;

/**
 * @author westy
 */
public class KeyPairUtils {

  /**
   * Check whether we are on an Android platform or not
   */
  public static final boolean isAndroid = System.getProperty("java.vm.name").equalsIgnoreCase("Dalvik");

  private static SimpleKeyStore keyStore = new SimpleKeyStore();
  //private static Preferences keyStore = Preferences.userRoot().node(KeyPairUtils.class.getName());
 
  // Added these and made the shorter because there is a maximum key length limit!
  private static final String GUID = "GU";
  private static final String PUBLIC = "PU";
  private static final String PRIVATE = "PR";
  private static final String DEFAULT_GUID = "D_GUID";
  private static final String DEFAULT_GNS = "D_GNS";

  /**
   * Retrieves the public/private key pair for the given user.
   *
   * @param gnsName the name of the GNS instance (e.g. "server.gns.name:8080")
   * @param username the user name
   * @return the GUID entry if found, null otherwise
   */
  public static GuidEntry getGuidEntry(String gnsName, String username) {
    if (username == null) {
      return null;
    }

    if (isAndroid) {
      return KeyPairUtilsAndroid.getGuidEntryFromPreferences(gnsName, username);
    }

    String guid = keyStore.get(generateKey(gnsName, username, GUID), "");
    String publicString = keyStore.get(generateKey(gnsName, username, PUBLIC), "");
    String privateString = keyStore.get(generateKey(gnsName, username, PRIVATE), "");
    if (!guid.isEmpty() && !publicString.isEmpty() && !privateString.isEmpty()) {
      try {
        byte[] encodedPublicKey = ByteUtils.hexStringToByteArray(publicString);
        byte[] encodedPrivateKey = ByteUtils.hexStringToByteArray(privateString);
        KeyFactory keyFactory = KeyFactory.getInstance(GnsProtocol.RSA_ALGORITHM);
        X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(encodedPublicKey);
        PublicKey publicKey = keyFactory.generatePublic(publicKeySpec);
        PKCS8EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(encodedPrivateKey);
        PrivateKey privateKey = keyFactory.generatePrivate(privateKeySpec);
        return new GuidEntry(username, guid, publicKey, privateKey);
      } catch (NoSuchAlgorithmException e) {
        System.out.println(e.toString());
        return null;
      } catch (InvalidKeySpecException e) {
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
    if (isAndroid) {
      KeyPairUtilsAndroid.removeKeyPairFromPreferences(gnsName, username);
      return;
    }
    keyStore.remove(generateKey(gnsName, username, PUBLIC));
    keyStore.remove(generateKey(gnsName, username, PRIVATE));
    keyStore.remove(generateKey(gnsName, username, GUID));
  }

  /**
   * Saves the public/private key pair to preferences for the given user.
   *
   * @param gnsName the name of the GNS instance (e.g. "server.gns.name:8080")
   * @param username the user name or human readable name
   * @param guid the GUID value
   * @param keyPair public and private keys for that GUID
   */
  public static void saveKeyPair(String gnsName, String username, String guid, KeyPair keyPair) {
    if (isAndroid) {
      KeyPairUtilsAndroid.saveKeyPairToPreferences(gnsName, username, guid, keyPair);
      return;
    }
    String publicString = ByteUtils.toHex(keyPair.getPublic().getEncoded());
    String privateString = ByteUtils.toHex(keyPair.getPrivate().getEncoded());
    keyStore.put(generateKey(gnsName, username, PUBLIC), publicString);
    keyStore.put(generateKey(gnsName, username, PRIVATE), privateString);
    keyStore.put(generateKey(gnsName, username, GUID), guid);
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
   * Set the default GUID to use in the user preferences (usually the account
   * GUID)
   *
   * @param gnsName the name of the GNS instance (e.g. "server.gns.name:8080")
   * @param username the alias of the default GUID to use
   */
  public static void setDefaultGuidEntry(String gnsName, String username) {
    if (isAndroid) {
      KeyPairUtilsAndroid.setDefaultGuidEntryToPreferences(gnsName, username);
    } else {
      keyStore.put(gnsName + DEFAULT_GUID, username);
    }
  }
  
  /**
   * Set the default GUID to use in the user preferences (usually the account
   * GUID)
   *
   * @param gnsName the name of the GNS instance (e.g. "server.gns.name:8080")
   */
  public static void removeDefaultGuidEntry(String gnsName) {
    if (isAndroid) {
      KeyPairUtilsAndroid.removeDefaultGuidEntryFromPreferences(gnsName);
    } else {
      keyStore.remove(gnsName + DEFAULT_GUID);
    }
  }

  /**
   * Return the GUID entry for the default GUID set in the user preferences (if
   * any)
   *
   * @param gnsName the name of the GNS instance (e.g. "server.gns.name:8080")
   * @return the default GUID entry or null if not set or invalid
   */
  public static GuidEntry getDefaultGuidEntry(String gnsName) {
    if (isAndroid) {
      return KeyPairUtilsAndroid.getDefaultGuidEntryFromPreferences(gnsName);
    } else {
      return getGuidEntry(gnsName, keyStore.get(gnsName + DEFAULT_GUID, null));
    }
  }

  /**
   * Sets the default GNS to use in the user preferences. Syntax to use is with
   * a colon between the host and port so that the string can be re-used as is
   * in the GnsConnect command.
   *
   * @param gnsHostPort a string of host:port:disableSSLBoolean
   */
  public static void setDefaultGns(String gnsHostPort) {
    if (isAndroid) {
      KeyPairUtilsAndroid.setDefaultGnsToPreferences(gnsHostPort);
    } else {
      keyStore.put(DEFAULT_GNS, gnsHostPort);
    }
  }

  /**
   * Return the default GNS Host:Port:disableSSLBoolean as a String if defined (null if not
   * defined)
   *
   * @return the default GNS saved in the user prefences as a string of host:port:disableSSLBoolean
   */
  public static String getDefaultGns() {
    if (isAndroid) {
      return KeyPairUtilsAndroid.getDefaultGnsFromPreferences();
    } else {
      return keyStore.get(DEFAULT_GNS, null);
    }
  }
  
  /**
   * Remove the default GNS to use in the user preferences.
   */
  public static void removeDefaultGns() {
    if (isAndroid) {
      KeyPairUtilsAndroid.removeDefaultGnsFromPreferences();
    } else {
      keyStore.remove(DEFAULT_GNS);
    }
  }
  

  /**
   * Reads a private key from a PKCS#8 formatted file
   *
   * @param filename
   * @return
   * @throws IOException
   * @throws InvalidKeySpecException
   * @throws NoSuchAlgorithmException
   */
  public static PrivateKey getPrivateKeyFromPKCS8File(String filename) throws IOException, InvalidKeySpecException,
          NoSuchAlgorithmException {
    File f = new File(filename);
    DataInputStream dis = new DataInputStream(new FileInputStream(f));
    byte[] keyBytes = new byte[(int) f.length()];
    dis.readFully(keyBytes);
    dis.close();

    PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);
    KeyFactory kf = KeyFactory.getInstance(GnsProtocol.RSA_ALGORITHM);
    return kf.generatePrivate(spec);
  }

  /**
   * Saves a private key to a PKCS#8 formatted file
   *
   * @param privateKey private key to save
   * @param filename file to save the key to
   */
  public static void writePrivateKeyToPKCS8File(PrivateKey privateKey, String filename) {
    byte[] keyBytes = privateKey.getEncoded();
    FileOutputStream fos = null;
    DataOutputStream dos = null;
    try {
      fos = new FileOutputStream(filename);
      dos = new DataOutputStream(fos);
      dos.write(keyBytes);
    } catch (FileNotFoundException e) {
      System.out.println("File not found" + e);
    } catch (IOException ioe) {
      System.out.println("Error while writing to file" + ioe);
    } finally {
      try {
        if (dos != null) {
          dos.close();
        }
        if (fos != null) {
          fos.close();
        }
      } catch (Exception e) {
        System.out.println("Error while closing streams" + e);
      }
    }
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
    if (isAndroid) {
      return KeyPairUtilsAndroid.getAllGuids(gnsName);
    }

    List<GuidEntry> guids = new LinkedList<GuidEntry>();
    //try {
      String[] keys = keyStore.keys();
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
      return string.substring(string.length() -  SimpleKeyStore.MAX_KEY_LENGTH);
    } else {
      return string;
    }
  }

  public static void main(String args[]) throws BackingStoreException {
    if ((args.length == 1) && "-clear".equals(args[0])) {
      keyStore.clear();
      //keyStore.removeNode();
      System.out.println(keyStore.toString() + " cleared.");
    } else if ((args.length == 1) && "-size".equals(args[0])) {
      System.out.println(keyStore.toString() + " contains " + keyStore.keys().length + " keys");
    } else {
      if (args.length < 3) {
        List<GuidEntry> allGuids = getAllGuids(args.length > 0 ? args[0] : null);
        for (GuidEntry entry : allGuids) {
          System.out.println(entry.toString());
        }
      } else if (args.length == 3) {
        copyKeyPair(args[0], args[1], args[2]);
      }
    }
    System.exit(0);
  }

}
