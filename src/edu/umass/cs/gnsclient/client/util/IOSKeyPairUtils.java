
package edu.umass.cs.gnsclient.client.util;

import edu.umass.cs.gnsclient.client.util.keystorage.AbstractKeyStorage;
import edu.umass.cs.gnsclient.client.util.keystorage.IOSKeyStorage;
import edu.umass.cs.gnsclient.client.util.keystorage.JavaPreferencesKeyStore;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.exceptions.client.EncryptionException;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnscommon.utils.ByteUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SignatureException;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.RSAPrivateKeySpec;
import java.security.spec.RSAPublicKeySpec;
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
    if (!guid.isEmpty()) {
      try {
          KeyPair kp = generateKeyPair();
        return new GuidEntry(username, guid, kp.getPublic(), kp.getPrivate());
      } catch (EncryptionException e) {
        System.out.println(e.toString());
        return null;
      }
    } else {
      return null;
    }
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
            keyStorageObj = new IOSKeyStorage();
        }
      }
    }
    assert (keyStorageObj != null);
  }

  public static KeyPair generateKeyPair() {
    try {
      KeyFactory fact = KeyFactory.getInstance("RSA");
      PublicKey pub = fact.generatePublic(new RSAPublicKeySpec(new BigInteger("126183173082874844637952901960865809228783583450688814141148417648807958333099119644874791736663780530158327353949473953143599351828672745381741163148759593688040153756690753357693891591900736951681645734271187360570232481677612954701021664472857269048194483763294248853395681697580178537778135146755606080851"), new BigInteger("65537")));
      PrivateKey priv = fact.generatePrivate(new RSAPrivateKeySpec(new BigInteger("126183173082874844637952901960865809228783583450688814141148417648807958333099119644874791736663780530158327353949473953143599351828672745381741163148759593688040153756690753357693891591900736951681645734271187360570232481677612954701021664472857269048194483763294248853395681697580178537778135146755606080851"), new BigInteger("15876626108021208918390521836051382622038686988027831017713808259945838601320404360767772901727719215888514386692515101661994297193634671382849956991083989554122755344196765004493909865687803605408109587648252347256902106306082790197547407143436778025637991525989203543883089994924508017995437871770596312209")));
      KeyPair keyPair = new KeyPair(pub, priv);
      return keyPair;
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }


  public static String generateAndSaveKeyPair() {

    return "GUID";
  }

  public static String getBase64OfPublicKeyFromGuid(String guid) {

    byte[] bits = {};
    String result = Base64.encodeToString(bits, false);
    return result;
  }

  public static String signDigestOfMessage(String guid,
                                           String message) throws NoSuchAlgorithmException,
          InvalidKeyException, SignatureException,
          UnsupportedEncodingException {
    return "DIGEST";

  }

  public static void main(String args[]) throws Exception{


  }


}
