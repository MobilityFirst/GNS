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
 *  Initial developer(s): Abhigyan Sharma, Westy
 *
 */
package edu.umass.cs.gnscommon;

import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.ShaOneHashFunction;
import java.util.HashSet;
import java.util.Set;
import javax.xml.bind.DatatypeConverter;
import org.json.JSONArray;
import org.json.JSONException;

// rt imports
import java.security.cert.*;
import java.io.*;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.security.Principal;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.KeyFactory;
import java.io.ByteArrayInputStream;

/**
 *
 * @author westy
 */
public class SharedGuidUtils {

  /**
   * Uses a hash function to generate a GNSProtocol.GUID.toString() from a public key string.
   * This code is duplicated in client so if you
   * change it you should change it there as well.
   *
   * @param keyBytes
   * @return a guid string
   */
  public static String createGuidStringFromPublicKey(byte[] keyBytes) {
    byte[] publicKeyDigest = ShaOneHashFunction.getInstance().hash(keyBytes);
    return DatatypeConverter.printHexBinary(publicKeyDigest);
    //return ByteUtils.toHex(publicKeyDigest);
  }

	public static final int HASH_LENGTH = 40; // SHA1
	public static boolean couldBeGUID(String s) {
  	return s.toUpperCase().matches("[0-9A-F]*") && s.length()==HASH_LENGTH;
  }

  /**
   * Creates a hexidecimal guid string by hashing a public key.
   * The input string is assumed to base64 encoded.
   *
   * @param publicKey
   * @return a guid string
   * @throws IllegalArgumentException
   */
  public static String createGuidStringFromBase64PublicKey(String publicKey) throws IllegalArgumentException {
    byte[] publickeyBytes = Base64.decode(publicKey);
    if (publickeyBytes == null) { // bogus public key
      throw new IllegalArgumentException();
    }
    return createGuidStringFromPublicKey(publickeyBytes);
  }

  /**
   * Converts a JSONArray of publicKeys to a JSONArray of guids using
   * {@link #createGuidStringFromBase64PublicKey}.
   *
   * @param publicKeys
   * @return JSONArray of guids
   * @throws JSONException
   */
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

  /**
   * Converts a set of publicKeys to a set of guids using
   * {@link #createGuidStringFromBase64PublicKey}.
   *
   * @param publicKeys
   * @return set of guids
   */
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
  
  /**
   * Finds a public key that corresponds to a guid in a set of public keys.
   *
   * @param guid
   * @param publicKeys
   * @return a public key
   */
  public static String findPublicKeyForGuid(String guid, JSONArray publicKeys) {
    if (guid != null && publicKeys != null) {
      for (int i=0; i<publicKeys.length(); i++) {    	 
        try {
        	String publicKey = publicKeys.getString(i);
	        if (guid.equals(createGuidStringFromBase64PublicKey(publicKey))) {
	          return publicKey;
	        }
        } catch (IllegalArgumentException | JSONException e) {
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


   /**
 * Helper function get common name from certificate object
 * 
 * @param cert
 * @return commonName
 */
  public static String getNameFromCertificate(X509Certificate cert) {
    Principal principal = cert.getSubjectDN();
    String unformatted_string = principal.getName();
    String[] split = unformatted_string.split(",");
    String alias = "";
    for (String x : split) {
      if (x.contains("CN=")) {
        alias = x.replace("CN=","");
      }
    }
    return alias;
  }

  /**
   * Helper function to get public key from certifcate
   * @param cert
   * @return publickey 
   */
  public static PublicKey getPublicKeyFromCertificate(X509Certificate cert) {

    PublicKey publicKey = cert.getPublicKey();
    return publicKey;
  }


  /**
   * Helper function to load private key from file 
   * 
   * @param privateKeyFileName
   * @return privateKeyObject 
   */
  public static PrivateKey loadPrivateKeyFromFile(String privateKeyFileName) throws FileNotFoundException, IOException,
                    NoSuchAlgorithmException, InvalidKeySpecException {
    
    // read private key from file 
    InputStream inputStream = new FileInputStream(privateKeyFileName);
    BufferedReader buf = new BufferedReader(new InputStreamReader(inputStream));

    String line = buf.readLine();
    StringBuilder sb = new StringBuilder();

    while(line != null) {
      sb.append(line).append("\n");
      line = buf.readLine();
    }
    
    // remove header and footer of private key 
    String privateKeyHeader = "-----BEGIN PRIVATE KEY-----\n";
    String privateKeyFooter = "\n-----END PRIVATE KEY-----\n";

    String privateKey = sb.toString();
    privateKey = privateKey.replace(privateKeyHeader, "");
    privateKey = privateKey.replace(privateKeyFooter, "");

    // decode base64 data 
    byte [] encoded = Base64.decode(privateKey);

    //get private  key object 
    PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(encoded);
    KeyFactory kf = KeyFactory.getInstance("RSA");
    PrivateKey privKey = kf.generatePrivate(keySpec);

    return privKey;
  }

  /**
   * Helper function to  load certificate from file 
   * 
   * @param certificateFileName
   * @return certificateObject
   * @throws CertificateException
   * @throws FileNotFoundException
   * @throws IOException
   */
  public static X509Certificate loadCertificateFromFile(String certificatePath) throws CertificateException, FileNotFoundException, IOException {
    
    // get template of certifcate which will be loaded 
    CertificateFactory factory  = CertificateFactory.getInstance("X.509");
    
    // read certificate from file 
    FileInputStream inputStream = new FileInputStream (certificatePath);

    // lget certifcate object by reading input from file 
    X509Certificate cer = (X509Certificate) factory.generateCertificate(inputStream);

    inputStream.close();

    return cer;
  }


  public static X509Certificate getCertificateFromString(String cert_string)
                  throws IOException, CertificateException {

    byte []cert_bytes = DatatypeConverter.parseBase64Binary(cert_string);

    CertificateFactory factory  = CertificateFactory.getInstance("X.509");

    InputStream inputStream = new ByteArrayInputStream(cert_bytes);

    // get certifcate object by reading input from file 
    X509Certificate cer = (X509Certificate) factory.generateCertificate(inputStream);

    inputStream.close();

    return cer;

  }

  public static String getPublicKeyString(PublicKey publicKey) {
    byte[] encodedPublicKey = publicKey.getEncoded();
    String b64PublicKey = Base64.encodeToString(encodedPublicKey,true);
    return b64PublicKey;
  }
}
