/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport;

import static edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.*;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.util.Base64;
import edu.umass.cs.gns.util.ByteUtils;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;

/**
 * Provides signing and ACL checks for commands.
 * 
 * @author westy
 */
public class AccessSupport {
  
  private static KeyFactory keyFactory;
  private static Signature sig;

  static {
    try {
      keyFactory = KeyFactory.getInstance(RSAALGORITHM);
      sig = Signature.getInstance(SIGNATUREALGORITHM);
    } catch (NoSuchAlgorithmException e) {
      GNS.getLogger().severe("Unable to initialize for authentication:" + e);
    }
  }
  
  /**
   * Compares the signature against the message to verify that the messages was signed by the guid.
   * 
   * @param guidInfo
   * @param signature
   * @param message
   * @return
   * @throws InvalidKeySpecException
   * @throws InvalidKeyException
   * @throws java.io.UnsupportedEncodingException
   * @throws SignatureException 
   */
  public static boolean verifySignature(GuidInfo guidInfo, String signature, String message) throws InvalidKeySpecException, 
          InvalidKeyException, SignatureException, UnsupportedEncodingException {
    if (!GNS.enableSignatureAuthentication) {
      return true;
    }
    //GNS.getLogger().info("LocalNS: User " + guidInfo.getName() + " signature:" + signature + " message: " + message);
    byte[] encodedPublicKey = Base64.decode(guidInfo.getPublicKey());
    if (encodedPublicKey == null) { // bogus signature
      return false;
    }
    return verifySignatureInternal(encodedPublicKey, signature, message);
  }
  
  public static synchronized boolean verifySignatureInternal(byte[] publickeyBytes, String signature, String message)
          throws InvalidKeySpecException, InvalidKeyException, SignatureException, UnsupportedEncodingException {

    //KeyFactory keyFactory = KeyFactory.getInstance(RSAALGORITHM);
    X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(publickeyBytes);
    PublicKey publicKey = keyFactory.generatePublic(publicKeySpec);

    //Signature sig = Signature.getInstance(SIGNATUREALGORITHM);
    sig.initVerify(publicKey);
    sig.update(message.getBytes("UTF-8"));
    return sig.verify(ByteUtils.hexStringToByteArray(signature));
  }

  /**
   * Extracts out the message string without the signature part.
   * 
   * @param messageStringWithSignatureParts
   * @param signatureParts
   * @return 
   */
  public static String removeSignature(String messageStringWithSignatureParts, String signatureParts) {
    GNS.getLogger().finer("fullstring = " + messageStringWithSignatureParts + " fullSignatureField = " + signatureParts);
    String result = messageStringWithSignatureParts.substring(0, messageStringWithSignatureParts.lastIndexOf(signatureParts));
    GNS.getLogger().finer("result = " + result);
    return result;
  }
  
}
