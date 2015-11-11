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
package edu.umass.cs.gnsserver.gnsApp.clientSupport;

import com.google.common.collect.Sets;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.ClientUtils;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.GroupAccess;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.MetaDataTypeName;
import edu.umass.cs.gnsserver.exceptions.FailedDBOperationException;
import edu.umass.cs.gnsserver.exceptions.FieldNotFoundException;
import edu.umass.cs.gnsserver.exceptions.RecordNotFoundException;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnscommon.utils.ByteUtils;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import static edu.umass.cs.gnscommon.GnsProtocol.*;
import edu.umass.cs.gnsserver.gnsApp.GnsApplicationInterface;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;

/**
 * Provides signing and ACL checks for commands.
 *
 * @author westy
 */
public class NSAccessSupport {

  private static boolean debuggingEnabled = false;

  // try this for now
  private static final Set<String> WORLDREADABLEFIELDS = new HashSet<String>(Arrays.asList(GroupAccess.JOINREQUESTS, GroupAccess.LEAVEREQUESTS));

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
    if (!GNS.enableSignatureAuthentication) {
      return true;
    }
    byte[] publickeyBytes = Base64.decode(accessorPublicKey);
    if (publickeyBytes == null) { // bogus public key
      if (debuggingEnabled) {
        GNS.getLogger().info("&&&&Base 64 decoding is bogus!!!");
      }
      return false;
    }
    if (debuggingEnabled) {
      GNS.getLogger().info("NS: public key:" + accessorPublicKey + " signature:"
              + signature + " message: " + message);
    }
    boolean result = verifySignatureInternal(publickeyBytes, signature, message);
    if (debuggingEnabled) {
      GNS.getLogger().info("Public key " + accessorPublicKey + (result ? " verified " : " NOT verified ") + "as author of message " + message);
    }
    return result;
  }

  private static synchronized boolean verifySignatureInternal(byte[] publickeyBytes, String signature, String message)
          throws InvalidKeyException, SignatureException, UnsupportedEncodingException, InvalidKeySpecException {

    //KeyFactory keyFactory = KeyFactory.getInstance(RSAALGORITHM);
    X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(publickeyBytes);
    PublicKey publicKey = keyFactory.generatePublic(publicKeySpec);

    //Signature sig = Signature.getInstance(SIGNATUREALGORITHM);
    sig.initVerify(publicKey);
    sig.update(message.getBytes("UTF-8"));
    return sig.verify(ByteUtils.hexStringToByteArray(signature));
  }

  /**
   * Checks to see if the reader given in readerInfo can access the field of the user given by guidInfo.
   * Access type is some combo of read, and write, and blacklist or whitelist.
   * Note: Blacklists are currently not activated.
   *
   * @param access
   * @param guid
   * @param field
   * @param accessorGuid
   * @param activeReplica
   * @param lnsAddress
   * @return true if the the reader has access
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException
   */
  public static boolean verifyAccess(MetaDataTypeName access, String guid, String field,
          String accessorGuid, GnsApplicationInterface<String> activeReplica,
          InetSocketAddress lnsAddress) throws FailedDBOperationException {
    //String accessorGuid = ClientUtils.createGuidStringFromPublicKey(accessorPublicKey);
    if (debuggingEnabled) {
      GNS.getLogger().info("User: " + guid + " Reader: " + accessorGuid + " Field: " + field);
    }
    if (guid.equals(accessorGuid)) {
      return true; // can always read your own stuff
    } else if (hierarchicalAccessCheck(access, guid, field, accessorGuid, activeReplica, lnsAddress)) {
      return true; // accessor can see this field
//    } else if (checkForAccess(access, guidInfo, field, accessorInfo, activeReplica)) {
//      return true; // accessor can see this field
    } else if (checkForAccess(access, guid, ALL_FIELDS, accessorGuid, activeReplica, lnsAddress)) {
      return true; // accessor can see all fields
    } else {
      if (debuggingEnabled) {
        GNS.getLogger().info("User " + accessorGuid + " NOT allowed to access user " + guid + "'s " + field + " field");
      }
      return false;
    }
  }

  /**
   * Handles checking of fields with dot notation.
   * Checks deepest field first then backs up.
   *
   * @param access
   * @param guidInfo
   * @param field
   * @param accessorInfo
   * @param accessorPublicKey
   * @return true if the accessor has access
   * @throws FailedDBOperationException
   */
  private static boolean hierarchicalAccessCheck(MetaDataTypeName access, String guid, 
          String field, String accessorGuid,
          GnsApplicationInterface<String> activeReplica, 
          InetSocketAddress lnsAddress) throws FailedDBOperationException {
    if (debuggingEnabled) {
      GNS.getLogger().info("###field=" + field);
    }
    if (checkForAccess(access, guid, field, accessorGuid, activeReplica, lnsAddress)) {
      return true;
    }
    // otherwise go up the hierarchy and check
    if (field.contains(".")) {
      return hierarchicalAccessCheck(access, guid, field.substring(0, field.lastIndexOf(".")),
              accessorGuid, activeReplica, lnsAddress);
    } else {
      return false;
    }
  }

  private static boolean checkForAccess(MetaDataTypeName access, String guid, String field, String accessorGuid,
          GnsApplicationInterface<String> activeReplica, InetSocketAddress lnsAddress) throws FailedDBOperationException {
    // first check the always world readable ones
    if (WORLDREADABLEFIELDS.contains(field)) {
      return true;
    }
    try {
      // FIXME: Tidy this mess up.
      @SuppressWarnings("unchecked")
      Set<String> allowedusers = (Set<String>) (Set<?>) NSFieldMetaData.lookupOnThisNameServer(access,
              guid, field, activeReplica);
      if (debuggingEnabled) {
        GNS.getLogger().info(guid + " allowed users of " + field + " : " + allowedusers);
      }
      if (checkAllowedUsers(accessorGuid, allowedusers, activeReplica, lnsAddress)) {
        if (debuggingEnabled) {
          GNS.getLogger().info("User " + accessorGuid + " allowed to access "
                  + (field != ALL_FIELDS ? ("user " + guid + "'s " + field + " field") : ("all of user " + guid + "'s fields")));
        }
        return true;
      }
      return false;
    } catch (FieldNotFoundException e) {
      // This is actually a normal result.. so no warning here.
      return false;
    } catch (RecordNotFoundException e) {
      GNS.getLogger().warning("User " + accessorGuid + " access problem for " + guid + "'s " + field + " field: " + e);
      return false;
    }
  }

  private static boolean checkAllowedUsers(String accessorGuid,
          Set<String> allowedUsers, GnsApplicationInterface<String> activeReplica,
          InetSocketAddress lnsAddress) throws FailedDBOperationException {
    if (ClientUtils.publicKeyListContainsGuid(accessorGuid, allowedUsers)) {
      //if (allowedUsers.contains(accessorPublicKey)) {
      return true;
    } else if (allowedUsers.contains(EVERYONE)) {
      return true;
    } else {
      // see if allowed users (the public keys for the guids and group guids that is in the ACL) 
      // intersects with the groups that this
      // guid is a member of (which is stored with this guid)
      return !Sets.intersection(ClientUtils.convertPublicKeysToGuids(allowedUsers),
              NSGroupAccess.lookupGroups(accessorGuid, activeReplica, lnsAddress)).isEmpty();
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
          GnsApplicationInterface<String> activeReplica) throws FailedDBOperationException {
    try {
      return NSFieldMetaData.lookupOnThisNameServer(access, guid, field, activeReplica).contains(EVERYONE)
              || NSFieldMetaData.lookupOnThisNameServer(access, guid, ALL_FIELDS, activeReplica).contains(EVERYONE);
    } catch (FieldNotFoundException e) {
      // This is actually a normal result.. so no warning here.
      return false;
    } catch (RecordNotFoundException e) {
      GNS.getLogger().warning("User " + guid + " access problem for " + field + "'s " + access.toString() + " field: " + e);
      return false;
    }
  }

  /**
   * Looks up the public key for a guid using the acl of a field.
   * 
   * @param access
   * @param guid
   * @param field
   * @param activeReplica
   * @return a set of public keys
   * @throws FailedDBOperationException
   */
  @SuppressWarnings("unchecked")
  public static Set<String> lookupPublicKeysFromAcl(MetaDataTypeName access, String guid, String field,
          GnsApplicationInterface<String> activeReplica) throws FailedDBOperationException {
    if (debuggingEnabled) {
      GNS.getLogger().info("###field=" + field);
    }
    try {
      //FIXME: Clean this mess up.
      return (Set<String>) (Set<?>) NSFieldMetaData.lookupOnThisNameServer(access, guid, field, activeReplica);
    } catch (FieldNotFoundException e) {
      // do nothing
    } catch (RecordNotFoundException e) {
      GNS.getLogger().warning("User " + guid + " access problem for " + field + "'s " + access.toString() + " field: " + e);
      return new HashSet<String>();
    }
    // otherwise go up the hierarchy and check
    if (field.contains(".")) {
      return lookupPublicKeysFromAcl(access, guid, field.substring(0, field.lastIndexOf(".")), activeReplica);
    } else {
      return new HashSet<String>();
    }
  }

}
