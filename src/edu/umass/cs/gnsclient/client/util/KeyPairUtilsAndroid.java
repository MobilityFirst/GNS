
package edu.umass.cs.gnsclient.client.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
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

import android.os.Environment;
import android.util.Log;
import edu.umass.cs.gnscommon.exceptions.client.EncryptionException;
import edu.umass.cs.gnscommon.GNSProtocol;
import javax.xml.bind.DatatypeConverter;


public class KeyPairUtilsAndroid {

  // directory to store GNS keys
  private static final String GNS_KEY_DIR = Environment.getExternalStorageDirectory().toString() + "/GNS";
  // GNS keys file on external storage
  private static final String GNS_KEYS_FILENAME = "GNSKeys.txt";
  // stores the default account to use for each GNS
  private static final String DEFAULT_GUIDS_FILENAME = "defaultGUIDs.txt";
  // stores the default GNS
  private static final String DEFAULT_GNS_FILENAME = "defaultGNS.txt";


  public static GuidEntry getGuidEntryFromPreferences(String gnsName, String username) {
    if (username == null) {
      return null;
    }

    return readGuidEntryFromFile(gnsName, username);
  }


  public static void removeKeyPairFromPreferences(String gnsName, String username) {
    if (getGuidEntryFromPreferences(gnsName, username) == null) {
      return;
    }

    // Entry exists, make a copy of the file omitting the entry to remove
    // Create Folder
    File gnsFolder = new File(GNS_KEY_DIR);
    String extStorageDirectory = gnsFolder.toString();

    try {
      File origFile = new File(extStorageDirectory, GNS_KEYS_FILENAME);
      File destFile = new File(extStorageDirectory, "gnscopyremove");
      BufferedReader br;
      try (FileOutputStream fOut = new FileOutputStream(destFile); 
              OutputStreamWriter myOutWriter = new OutputStreamWriter(fOut)) {
        br = new BufferedReader(new FileReader(origFile));
        String aliasKey = gnsName + "@" + username;
        String line;
        while ((line = br.readLine()) != null) {
          if (line.equals(aliasKey)) { // skip that entry
            br.readLine();
            br.readLine();
            br.readLine();
            continue;
          }
          myOutWriter.append(line.trim() + "\n");
        }       }
      br.close();

      // Copy over, swap files
      origFile.delete();
      destFile.renameTo(origFile);
    } catch (Exception e) {
      Log.e(KeyPairUtilsAndroid.class.getName(), "Failed to remove entry", e);
      e.printStackTrace();
    }
  }


  public static void saveKeyPairToPreferences(String gnsName, String username, String guid, KeyPair keyPair) {
    String aliasKey = gnsName + "@" + username;
    String publicString =  DatatypeConverter.printHexBinary(keyPair.getPublic().getEncoded());
    String privateString =  DatatypeConverter.printHexBinary(keyPair.getPrivate().getEncoded());
    //String publicString = ByteUtils.toHex(keyPair.getPublic().getEncoded());
    //String privateString = ByteUtils.toHex(keyPair.getPrivate().getEncoded());

    if (readGuidEntryFromFile(gnsName, username) != null) // entry already there just return
    {
      return;
    }

    // store in the file
    appendToFile(aliasKey, guid, publicString, privateString);
  }


  public static void setDefaultGuidEntryToPreferences(String gnsName, String username) {
    String aliasKey = gnsName + "@default-guid";

    // adding order
    // alias,
    // username,
    // Create Folder
    File gnsFolder = new File(GNS_KEY_DIR);
    gnsFolder.mkdirs();

    // Save the path as a string value
    String extStorageDirectory = gnsFolder.toString();

    try {
      File file = new File(extStorageDirectory, DEFAULT_GUIDS_FILENAME);
      file.createNewFile();
      try (FileOutputStream fOut = new FileOutputStream(file);
              OutputStreamWriter myOutWriter = new OutputStreamWriter(fOut)) {

        // not appending here, overwritng previous entries
        myOutWriter.write(aliasKey + "\n");
        myOutWriter.write(username + "\n");

      }
    } catch (Exception e) {
      Log.e(KeyPairUtilsAndroid.class.getName(), "Could not create file", e);
      e.printStackTrace();
    }
  }


  public static void removeDefaultGuidEntryFromPreferences(String gnsName) {
    String aliasKey = gnsName + "@default-guid";

    // adding order
    // alias,
    // username,
    // Create Folder
    File gnsFolder = new File(GNS_KEY_DIR);
    gnsFolder.mkdirs();

    // Save the path as a string value
    String extStorageDirectory = gnsFolder.toString();

    try {
      File file = new File(extStorageDirectory, DEFAULT_GUIDS_FILENAME);
      if (file.exists()) {
        file.delete();
      }
    } catch (Exception e) {
      Log.e(KeyPairUtilsAndroid.class.getName(), "Could not create file", e);
      e.printStackTrace();
    }
  }


  public static GuidEntry getDefaultGuidEntryFromPreferences(String gnsName) {
    String aliasKey = gnsName + "@default-guid";

    File gnsFolder = new File(GNS_KEY_DIR);

    // Save the path as a string value
    String extStorageDirectory = gnsFolder.toString();

    File file = new File(extStorageDirectory, DEFAULT_GUIDS_FILENAME);

    try {
      BufferedReader br = new BufferedReader(new FileReader(file));
      String line;

      while ((line = br.readLine()) != null) {

        // found the correct alias, read next three lines for guid, public key,
        // private key
        if (line.equals(aliasKey)) {
          String userName = br.readLine();
          return readGuidEntryFromFile(gnsName, userName);

          // break;
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
      // You'll need to add proper error handling here
    }
    return null;
  }


  public static void setDefaultGnsToPreferences(String gnsHostPort) {
    // Create Folder
    File gnsFolder = new File(GNS_KEY_DIR);
    gnsFolder.mkdirs();

    // Save the path as a string value
    String extStorageDirectory = gnsFolder.toString();

    try {
      File file = new File(extStorageDirectory, DEFAULT_GNS_FILENAME);
      if (file.exists()) {
        file.delete();
      }
      file.createNewFile();
      try (FileOutputStream fOut = new FileOutputStream(file);
              OutputStreamWriter myOutWriter = new OutputStreamWriter(fOut)) {

        // not appending here, overwritng previous entries
        myOutWriter.write(gnsHostPort);
      }
    } catch (Exception e) {
      e.printStackTrace();
      Log.e(KeyPairUtilsAndroid.class.getName(), "Could not create file", e);
    }
  }


  public static void removeDefaultGnsFromPreferences() {
    // Create Folder
    File gnsFolder = new File(GNS_KEY_DIR);
    gnsFolder.mkdirs();

    // Save the path as a string value
    String extStorageDirectory = gnsFolder.toString();

    try {
      File file = new File(extStorageDirectory, DEFAULT_GNS_FILENAME);
      if (file.exists()) {
        file.delete();
      }

    } catch (Exception e) {
      e.printStackTrace();
      Log.e(KeyPairUtilsAndroid.class.getName(), "Could not create file", e);
    }
  }


  public static String getDefaultGnsFromPreferences() {
    File gnsFolder = new File(GNS_KEY_DIR);
    String extStorageDirectory = gnsFolder.toString();

    File file = new File(extStorageDirectory, DEFAULT_GNS_FILENAME);

    try {
      String line;
      try (BufferedReader br = new BufferedReader(new FileReader(file))) {
        line = br.readLine();
      }
      if (line != null) {
        return line.trim();
      }
    } catch (IOException e) {
      e.printStackTrace();
      // You'll need to add proper error handling here
    }
    return null;
  }


  public static List<GuidEntry> getAllGuids(String gnsName) {
    List<GuidEntry> guids = new LinkedList<>();

    File gnsFolder = new File(GNS_KEY_DIR);

    // Save the path as a string value
    String extStorageDirectory = gnsFolder.toString();

    File file = new File(extStorageDirectory, GNS_KEYS_FILENAME);

    try {
      BufferedReader br = new BufferedReader(new FileReader(file));
      String line;

      while ((line = br.readLine()) != null) {
        String aliasKey = line;
        String guid = br.readLine();
        String publicString = br.readLine();
        String privateString = br.readLine();

        if (aliasKey.contains(gnsName) && !publicString.isEmpty() && !privateString.isEmpty()) {
          try {
            byte[] encodedPublicKey = DatatypeConverter.parseHexBinary(publicString);
            byte[] encodedPrivateKey = DatatypeConverter.parseHexBinary(privateString);
            //byte[] encodedPublicKey = ByteUtils.hexStringToByteArray(publicString);
            //byte[] encodedPrivateKey = ByteUtils.hexStringToByteArray(privateString);
            KeyFactory keyFactory = KeyFactory.getInstance(GNSProtocol.RSA_ALGORITHM.toString());
            X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(encodedPublicKey);
            PublicKey publicKey = keyFactory.generatePublic(publicKeySpec);
            PKCS8EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(encodedPrivateKey);
            PrivateKey privateKey = keyFactory.generatePrivate(privateKeySpec);

            // Strip gnsName from stored alias to only return the entity name
            guids.add(new GuidEntry(aliasKey.substring(gnsName.length() + 1), guid, publicKey, privateKey));
          } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            Log.e(KeyPairUtilsAndroid.class.getName(), "Cannot decode keys", e);
          } catch (EncryptionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
      // You'll need to add proper error handling here
    }

    return guids;
  }

  private static void appendToFile(String alias, String guid, String publicKey, String privateKey) {
    // adding order
    // alias,
    // guid,
    // publicKey,
    // privateKey

    // Create Folder
    File gnsFolder = new File(GNS_KEY_DIR);
    gnsFolder.mkdirs();

    // Save the path as a string value
    String extStorageDirectory = gnsFolder.toString();

    try {
      File file = new File(extStorageDirectory, GNS_KEYS_FILENAME);
      file.createNewFile();
      try (FileOutputStream fOut = new FileOutputStream(file, true);
              OutputStreamWriter myOutWriter = new OutputStreamWriter(fOut)) {

        myOutWriter.append(alias + "\n");
        myOutWriter.append(guid + "\n");
        myOutWriter.append(publicKey + "\n");
        myOutWriter.append(privateKey + "\n");

      }
    } catch (Exception e) {
      e.printStackTrace();
      Log.e(KeyPairUtilsAndroid.class.getName(), "Could not create file", e);
    }
  }

  private static GuidEntry readGuidEntryFromFile(String gnsName, String username) {
    File gnsFolder = new File(GNS_KEY_DIR);
    // Save the path as a string value
    String extStorageDirectory = gnsFolder.toString();

    File file = new File(extStorageDirectory, GNS_KEYS_FILENAME);
    String aliasKey = gnsName + "@" + username;

    try {
      BufferedReader br = new BufferedReader(new FileReader(file));
      String line;

      while ((line = br.readLine()) != null) {

        if (line.equals(aliasKey)) {
          // found the correct alias, read next three lines for guid, public
          // key,
          // private key
          String guid = br.readLine();
          String publicString = br.readLine();
          String privateString = br.readLine();

          if (!publicString.isEmpty() && !privateString.isEmpty()) {
            try {
              byte[] encodedPublicKey = DatatypeConverter.parseHexBinary(publicString);
              byte[] encodedPrivateKey = DatatypeConverter.parseHexBinary(privateString);
//              byte[] encodedPublicKey = ByteUtils.hexStringToByteArray(publicString);
//              byte[] encodedPrivateKey = ByteUtils.hexStringToByteArray(privateString);
              KeyFactory keyFactory = KeyFactory.getInstance(GNSProtocol.RSA_ALGORITHM.toString());
              X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(encodedPublicKey);
              PublicKey publicKey = keyFactory.generatePublic(publicKeySpec);
              PKCS8EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(encodedPrivateKey);
              PrivateKey privateKey = keyFactory.generatePrivate(privateKeySpec);
              return new GuidEntry(username, guid, publicKey, privateKey);
            } catch (NoSuchAlgorithmException | InvalidKeySpecException | EncryptionException e) {
              Log.e(KeyPairUtilsAndroid.class.getName(), "Cannot decode keys", e);
            }
          }

          break;
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
      // You'll need to add proper error handling here
    }

    return null;
  }
}
