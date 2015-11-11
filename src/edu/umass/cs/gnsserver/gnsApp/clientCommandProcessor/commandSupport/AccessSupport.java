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
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport;

import static edu.umass.cs.gnscommon.GnsProtocol.*;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnscommon.utils.ByteUtils;
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
      keyFactory = KeyFactory.getInstance(RSA_ALGORITHM);
      sig = Signature.getInstance(SIGNATURE_ALGORITHM);
    } catch (NoSuchAlgorithmException e) {
      GNS.getLogger().severe("Unable to initialize for authentication:" + e);
    }
  }
  
  /**
   * Compares the signature against the message to verify that the messages was signed by the guid.
   * 
   * @param publicKey
   * @param signature
   * @param message
   * @return true if the verification passes
   * @throws InvalidKeySpecException
   * @throws InvalidKeyException
   * @throws java.io.UnsupportedEncodingException
   * @throws SignatureException 
   */
  public static boolean verifySignature(String publicKey, String signature, String message) throws InvalidKeySpecException, 
          InvalidKeyException, SignatureException, UnsupportedEncodingException {
    if (!GNS.enableSignatureAuthentication) {
      return true;
    }
    //GNS.getLogger().info("LocalNS: User " + guidInfo.getName() + " signature:" + signature + " message: " + message);
    byte[] encodedPublicKey = Base64.decode(publicKey);
    if (encodedPublicKey == null) { // bogus signature
      return false;
    }
    return verifySignatureInternal(encodedPublicKey, signature, message);
  }
  
  private static synchronized boolean verifySignatureInternal(byte[] publickeyBytes, String signature, String message)
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
