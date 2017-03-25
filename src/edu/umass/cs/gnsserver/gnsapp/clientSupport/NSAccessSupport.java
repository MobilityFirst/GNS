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
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsserver.gnsapp.clientSupport;

import com.google.common.collect.Sets;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.FieldNotFoundException;
import edu.umass.cs.gnscommon.exceptions.server.RecordNotFoundException;
import edu.umass.cs.gnsserver.main.GNSConfig.GNSC;
import edu.umass.cs.gnscommon.utils.Base64;
import java.nio.ByteBuffer;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;

import edu.umass.cs.gnscommon.SharedGuidUtils;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.MetaDataTypeName;
import edu.umass.cs.gnsserver.gnsapp.GNSApplicationInterface;
import edu.umass.cs.gnsserver.gnsapp.recordmap.BasicRecordMap;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.SessionKeys;
import edu.umass.cs.utils.Util;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;

import edu.umass.cs.gnscommon.GNSProtocol;
import javax.xml.bind.DatatypeConverter;

/**
 * Provides signing and ACL checks for commands.
 *
 * @author westy, arun
 */
public class NSAccessSupport {

  private static KeyFactory keyFactory;
  // arun: at least as many instances as cores for parallelism.
  private static Signature[] signatureInstances = new Signature[2 * Runtime.getRuntime().availableProcessors()];

  static {
    try {
      keyFactory = KeyFactory.getInstance(GNSProtocol.RSA_ALGORITHM.toString());
      for (int i = 0; i < signatureInstances.length; i++) {
        signatureInstances[i] = Signature.getInstance(GNSProtocol.SIGNATURE_ALGORITHM.toString());
      }
    } catch (NoSuchAlgorithmException e) {
      ClientSupportConfig.getLogger().log(Level.SEVERE, "Unable to initialize for authentication:{0}", e);
    }
  }

  /**
   * Verifies that the signature corresponds to the message using the public key.
   *
   * @param accessorPublicKey
   * @param signature
   * @param message
   * @return true if the signature verifies successfully
   * @throws InvalidKeyException
   * @throws SignatureException
   * @throws UnsupportedEncodingException
   * @throws InvalidKeySpecException
   */
  public static boolean verifySignature(String accessorPublicKey, String signature, String message) throws
          InvalidKeyException, SignatureException, UnsupportedEncodingException, InvalidKeySpecException {
    byte[] publickeyBytes = Base64.decode(accessorPublicKey);
    if (publickeyBytes == null) { // bogus public key
      ClientSupportConfig.getLogger().log(Level.FINE, "&&&&Base 64 decoding is bogus!!!");
      return false;
    }
    ClientSupportConfig.getLogger().log(Level.FINER,
            "public_key:{0}, signature:{1}, message:{2}",
            new Object[]{Util.truncate(accessorPublicKey, 16, 16),
              Util.truncate(signature, 16, 16),
              Util.truncate(message, 16, 16)});
    long t = System.nanoTime();
    boolean result = verifySignatureInternal(publickeyBytes, signature, message);
    if (Util.oneIn(100)) {
      DelayProfiler.updateDelayNano("verification", t);
    }

    ClientSupportConfig.getLogger().log(Level.FINE,
            "public_key:{0} {1} as author of message:{2}",
            new Object[]{accessorPublicKey,
              (result ? " verified " : " NOT verified "),
              message});
    return result;
  }

  private static int sigIndex = 0;

  private synchronized static Signature getSignatureInstance() {
    return signatureInstances[sigIndex++ % signatureInstances.length];
  }

  private static synchronized boolean verifySignatureInternal(byte[] publickeyBytes, String signature, String message)
          throws InvalidKeyException, SignatureException, UnsupportedEncodingException, InvalidKeySpecException {

    if (Config.getGlobalBoolean(GNSC.ENABLE_SECRET_KEY)) {
      try {
        return verifySignatureInternalSecretKey(publickeyBytes, signature, message);
      } catch (Exception e) {
        // This provided backward support for clients that don't have ENABLE_SECRET_KEY on by
        // falling through to non-secret method.
        // At the cost of potentially masking other issues that might cause exceptions
        // in the above code.
        ClientSupportConfig.getLogger().log(Level.FINE, "Falling through to non-secret key verification: {0}",
                new Object[]{e});
      }
    }

    // Non-secret method kept for backwards compatbility with older clients.
    X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(publickeyBytes);
    PublicKey publicKey = keyFactory.generatePublic(publicKeySpec);

    Signature sigInstance = getSignatureInstance();
    synchronized (sigInstance) {
      sigInstance.initVerify(publicKey);
      // iOS client uses UTF-8 - should switch to ISO-8859-1 to be consistent with
      // secret key version
      sigInstance.update(message.getBytes("UTF-8"));
      // Non secret uses ISO-8859-1, but the iOS client uses hex so 
      // we need to keep this for now.
      try {
        return sigInstance.verify(DatatypeConverter.parseHexBinary(signature));
        // This will get thrown if the signature is not a hex string.
      } catch (IllegalArgumentException e) {
        return false;
      }
      //return sigInstance.verify(ByteUtils.hexStringToByteArray(signature));
    }
  }

  private static final MessageDigest[] mds = new MessageDigest[Runtime.getRuntime().availableProcessors()];

  static {
    for (int i = 0; i < mds.length; i++) {
      try {
        mds[i] = MessageDigest.getInstance(GNSProtocol.DIGEST_ALGORITHM.toString());
      } catch (NoSuchAlgorithmException e) {
        e.printStackTrace();
        System.exit(1);
      }
    }
  }
  private static int mdIndex = 0;

  private static MessageDigest getMessageDigestInstance() {
    return mds[mdIndex++ % mds.length];
  }

  private static final Cipher[] ciphers = new Cipher[2 * Runtime.getRuntime().availableProcessors()];

  static {
    for (int i = 0; i < ciphers.length; i++) {
      try {
        ciphers[i] = Cipher.getInstance(GNSProtocol.SECRET_KEY_ALGORITHM.toString());
      } catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
        e.printStackTrace();
        System.exit(1);
      }
    }
  }

  private static int cipherIndex = 0;

  private static Cipher getCipherInstance() {
    return ciphers[cipherIndex++ % ciphers.length];
  }

  private static synchronized boolean verifySignatureInternalSecretKey(byte[] publickeyBytes, String signature, String message)
          throws InvalidKeyException, SignatureException, UnsupportedEncodingException, InvalidKeySpecException, NoSuchAlgorithmException, NoSuchPaddingException, IllegalBlockSizeException, BadPaddingException {

    PublicKey publicKey = keyFactory.generatePublic(new X509EncodedKeySpec(publickeyBytes));

    // FIXME: The reason why we use CHARSET should be more throughly documented here.
    byte[] sigBytes = signature.getBytes(GNSProtocol.CHARSET.toString());
    byte[] bytes = message.getBytes(GNSProtocol.CHARSET.toString());

    // Pull the parts out of the signature
    // See CommandUtils.signDigestOfMessage for the other side of this.
    ByteBuffer bbuf = ByteBuffer.wrap(sigBytes);
    byte[] sign = new byte[bbuf.getShort()];
    bbuf.get(sign);
    byte[] skCertEncoded = new byte[bbuf.getShort()];
    bbuf.get(skCertEncoded);
    SecretKey secretKey = SessionKeys.getSecretKeyFromCertificate(skCertEncoded, publicKey);

    MessageDigest md = getMessageDigestInstance();
    byte[] digest;
    synchronized (md) {
      digest = md.digest(bytes);
    }
    Cipher cipher = getCipherInstance();
    synchronized (cipher) {
      cipher.init(Cipher.ENCRYPT_MODE, secretKey);
      return Arrays.equals(sign, cipher.doFinal(digest));
    }
  }

  /**
   * Handles checking of fields with dot notation.
   * Checks deepest field first then backs up.
   *
   * @param accessType
   * @param guid
   * @param activeReplica
   * @param groups
   * @param field
   * @return true if the accessor has access
   * @throws FailedDBOperationException
   */
  public static boolean hierarchicalAccessGroupCheck(MetaDataTypeName accessType, String guid,
          String field, Set<String> groups,
          GNSApplicationInterface<String> activeReplica) throws FailedDBOperationException {
    ClientSupportConfig.getLogger().log(Level.FINE, "###field={0}", field);
    try {
      return checkForGroupAccess(accessType, guid, field, groups, activeReplica);
    } catch (FieldNotFoundException e) {
      ClientSupportConfig.getLogger().log(Level.FINE, "###field NOT FOUND={0}.. GOING UP", new Object[]{field});
    }
    // otherwise go up the hierarchy and check
    if (field.contains(".")) {
      return hierarchicalAccessGroupCheck(accessType, guid, field.substring(0, field.lastIndexOf(".")),
              groups, activeReplica);
    } else if (!GNSProtocol.ENTIRE_RECORD.toString().equals(field)) {
      return hierarchicalAccessGroupCheck(accessType, guid, GNSProtocol.ENTIRE_RECORD.toString(), groups, activeReplica);
    } else {
      // check all the way up and there is no access
      return false;
    }
  }

  /**
   * Check for one of the groups that accessorguid is in being a member of allowed users.
   * Field can be dotted and at any level.
   *
   * @param accessType
   * @param guid
   * @param field
   * @param accessorGuid
   * @param activeReplica
   * @return true if access is allowed
   * @throws FailedDBOperationException
   */
  private static boolean checkForGroupAccess(MetaDataTypeName accessType,
          String guid, String field, Set<String> groups,
          GNSApplicationInterface<String> activeReplica)
          throws FieldNotFoundException, FailedDBOperationException {
    try {
      // FIXME: Tidy this mess up.
      @SuppressWarnings("unchecked")
      Set<String> allowedUsers = (Set<String>) (Set<?>) NSFieldMetaData.lookupLocally(accessType,
              guid, field, activeReplica.getDB());
      ClientSupportConfig.getLogger().log(Level.FINE, "{0} allowed users of {1} : {2}",
              new Object[]{guid, field, allowedUsers});
      return !Sets.intersection(SharedGuidUtils.convertPublicKeysToGuids(allowedUsers), groups).isEmpty();
    } catch (RecordNotFoundException e) {
      ClientSupportConfig.getLogger().log(Level.WARNING,
              "User {0} access problem for {2} field: {3}",
              new Object[]{guid, field, e});
      return false;
    }
  }

  /**
   * Returns true if the field has access setting that allow it to be read globally.
   *
   * @param access
   * @param guid
   * @param field
   * @param activeReplica
   * @return true if the field can be accessed
   * @throws FailedDBOperationException
   */
  public static boolean fieldAccessibleByEveryone(MetaDataTypeName access, String guid, String field,
          GNSApplicationInterface<String> activeReplica) throws FailedDBOperationException {
    // First we check to see if the field has an acl that allows everyone access.
    // Note: If ACL exists and doesn't give all access we return false because this ACL
    // overrides the ENTIRE_RECORD ACL.
    try {
      try {
        return (NSFieldMetaData.lookupLocally(access, guid, field, activeReplica.getDB())
                .contains(GNSProtocol.EVERYONE.toString()));
      } catch (FieldNotFoundException e) {
        // If the field has no ACL then we also want to check to see if the entire record has an
        // ACL that allows access to everyone.
        try {
          return NSFieldMetaData.lookupLocally(access, guid, GNSProtocol.ENTIRE_RECORD.toString(),
                  activeReplica.getDB()).contains(GNSProtocol.EVERYONE.toString());
        } catch (FieldNotFoundException f) {
          return false;
        }
      }
    } catch (RecordNotFoundException e) {
      ClientSupportConfig.getLogger().log(Level.WARNING,
              "User {0} access problem for {1}''s {2} field: {3}", 
              new Object[]{guid, field, access.toString(), e.getMessage()});
      return false;
    }
  }

  /**
   * Looks up the public key for a guid using the acl of a field.
   * Handles fields that uses dot notation. Recursively goes up the tree
   * towards the root (GNSProtocol.ENTIRE_RECORD.toString()) node.
   *
   * @param access
   * @param guid
   * @param field
   * @param database
   * @return a set of public keys
   * @throws FailedDBOperationException
   */
  @SuppressWarnings("unchecked")
  public static Set<String> lookupPublicKeysFromAcl(MetaDataTypeName access, String guid, String field,
          BasicRecordMap database) throws FailedDBOperationException {
    ClientSupportConfig.getLogger().log(Level.FINE, "###field={0}", new Object[]{field});
    try {
      // If the field is found this will return a list of the public keys in the ACL,
      // empty or otherwise. If it is empty we will stop looking.
      return (Set<String>) (Set<?>) NSFieldMetaData.lookupLocally(access, guid, field, database);
    } catch (RecordNotFoundException e) {
      ClientSupportConfig.getLogger().log(Level.WARNING, "User {0} access problem for {1}'s {2} field: {3}",
              new Object[]{guid, field, access.toString(), e});
      return new HashSet<>();
    } catch (FieldNotFoundException e) {
      ClientSupportConfig.getLogger().log(Level.FINE, "###field NOT FOUND={0}.. GOING UP", new Object[]{field});
    }
    // otherwise go up the hierarchy and check
    if (field.contains(".")) {
      return lookupPublicKeysFromAcl(access, guid, field.substring(0, field.lastIndexOf(".")), database);
      // One last check at the root (GNSProtocol.ENTIRE_RECORD.toString()) field.
    } else if (!GNSProtocol.ENTIRE_RECORD.toString().equals(field)) {
      return lookupPublicKeysFromAcl(access, guid, GNSProtocol.ENTIRE_RECORD.toString(), database);
    } else {
      return new HashSet<>();
    }
  }

  /**
   * Extracts out the message string without the signature part.
   *
   * @param messageStringWithSignatureParts
   * @param signatureParts
   * @return the string without the signature
   */
  public static String removeSignature(String messageStringWithSignatureParts, String signatureParts) {
    ClientSupportConfig.getLogger().log(Level.FINER,
            "fullstring = {0} fullSignatureField = {1}", 
            new Object[]{messageStringWithSignatureParts, signatureParts});
    String result = messageStringWithSignatureParts.substring(0,
            messageStringWithSignatureParts.lastIndexOf(signatureParts));
    ClientSupportConfig.getLogger().log(Level.FINER, "result = {0}", result);
    return result;
  }

}
