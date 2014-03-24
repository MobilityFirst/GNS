/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.clientsupport;

import static edu.umass.cs.gns.clientsupport.Defs.*;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.util.Base64;
import edu.umass.cs.gns.util.ByteUtils;
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
  
  public static boolean verifySignature(GuidInfo guidInfo, String signature, String message) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    if (!GNS.enableSignatureVerification) {
      return true;
    }
    byte[] encodedPublicKey = Base64.decode(guidInfo.getPublicKey());
    if (encodedPublicKey == null) { // bogus signature
      return false;
    }
    KeyFactory keyFactory = KeyFactory.getInstance(RASALGORITHM);
    X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(encodedPublicKey);
    PublicKey publicKey = keyFactory.generatePublic(publicKeySpec);

    Signature sig = Signature.getInstance(SIGNATUREALGORITHM);
    sig.initVerify(publicKey);
    sig.update(message.getBytes());
    boolean result = sig.verify(ByteUtils.hexStringToByteArray(signature));
    GNS.getLogger().fine("User " + guidInfo.getName() + (result ? " verified " : " NOT verified ") + "as author of message " + message);
    return result;
  }

  public static String removeSignature(String fullString, String fullSignatureField) {
    GNS.getLogger().finer("fullstring = " + fullString + " fullSignatureField = " + fullSignatureField);
    String result = fullString.substring(0, fullString.lastIndexOf(fullSignatureField));
    GNS.getLogger().finer("result = " + result);
    return result;
  }
  
}
