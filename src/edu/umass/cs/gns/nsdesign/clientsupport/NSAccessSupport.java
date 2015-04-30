/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.nsdesign.clientsupport;

import com.google.common.collect.Sets;
import edu.umass.cs.gns.clientsupport.ClientUtils;
import edu.umass.cs.gns.clientsupport.GroupAccess;
import edu.umass.cs.gns.clientsupport.GuidInfo;
import edu.umass.cs.gns.clientsupport.MetaDataTypeName;
import edu.umass.cs.gns.exceptions.FailedDBOperationException;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.util.Base64;
import edu.umass.cs.gns.util.ByteUtils;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import static edu.umass.cs.gns.clientsupport.Defs.*;
import edu.umass.cs.gns.nsdesign.GnsApplicationInterface;
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

  public static boolean verifySignature(String accessorPublicKey, String signature, String message) throws NoSuchAlgorithmException,
          InvalidKeySpecException, InvalidKeyException, SignatureException, UnsupportedEncodingException {
    if (!GNS.enableSignatureVerification) {
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
    KeyFactory keyFactory = KeyFactory.getInstance(RSAALGORITHM);
    X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(publickeyBytes);
    PublicKey publicKey = keyFactory.generatePublic(publicKeySpec);

    Signature sig = Signature.getInstance(SIGNATUREALGORITHM);
    sig.initVerify(publicKey);
    sig.update(message.getBytes("UTF-8"));
    boolean result = sig.verify(ByteUtils.hexStringToByteArray(signature));
    if (debuggingEnabled) {
      GNS.getLogger().info("Public key " + accessorPublicKey + (result ? " verified " : " NOT verified ") + "as author of message " + message);
    }
    return result;
  }

  /**
   * Checks to see if the reader given in readerInfo can access the field of the user given by guidInfo.
   * Access type is some combo of read, and write, and blacklist or whitelist.
   * Note: Blacklists are currently not activated.
   *
   * @param access
   * @param guidInfo
   * @param field
   * @param activeReplica
   * @param lnsAddress
   * @return
   */
  public static boolean verifyAccess(MetaDataTypeName access, GuidInfo guidInfo, String field,
          String accessorGuid, GnsApplicationInterface activeReplica,
          InetSocketAddress lnsAddress) throws FailedDBOperationException {
    //String accessorGuid = ClientUtils.createGuidStringFromPublicKey(accessorPublicKey);
    if (debuggingEnabled) {
      GNS.getLogger().info("User: " + guidInfo.getName() + " Reader: " + accessorGuid + " Field: " + field);
    }
    if (guidInfo.getGuid().equals(accessorGuid)) {
      return true; // can always read your own stuff
    } else if (hierarchicalAccessCheck(access, guidInfo, field, accessorGuid, activeReplica, lnsAddress)) {
      return true; // accessor can see this field
//    } else if (checkForAccess(access, guidInfo, field, accessorInfo, activeReplica)) {
//      return true; // accessor can see this field
    } else if (checkForAccess(access, guidInfo, ALLFIELDS, accessorGuid, activeReplica, lnsAddress)) {
      return true; // accessor can see all fields
    } else {
      if (debuggingEnabled) {
        GNS.getLogger().info("User " + accessorGuid + " NOT allowed to access user " + guidInfo.getName() + "'s " + field + " field");
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
   * @return
   * @throws FailedDBOperationException
   */
  private static boolean hierarchicalAccessCheck(MetaDataTypeName access, GuidInfo guidInfo, String field, String accessorGuid,
          GnsApplicationInterface activeReplica, InetSocketAddress lnsAddress) throws FailedDBOperationException {
    if (debuggingEnabled) {
      GNS.getLogger().info("###field=" + field);
    }
    if (checkForAccess(access, guidInfo, field, accessorGuid, activeReplica, lnsAddress)) {
      return true;
    }
    // otherwise go up the hierarchy and check
    if (field.contains(".")) {
      return hierarchicalAccessCheck(access, guidInfo, field.substring(0, field.lastIndexOf(".")),
              accessorGuid, activeReplica, lnsAddress);
    } else {
      return false;
    }
  }

  private static boolean checkForAccess(MetaDataTypeName access, GuidInfo guidInfo, String field, String accessorGuid,
          GnsApplicationInterface activeReplica, InetSocketAddress lnsAddress) throws FailedDBOperationException {
    // first check the always world readable ones
    if (WORLDREADABLEFIELDS.contains(field)) {
      return true;
    }
    try {
      Set<String> allowedusers = (Set<String>)(Set<?>)NSFieldMetaData.lookupOnThisNameServer(access, 
              guidInfo, field, activeReplica);
      if (debuggingEnabled) {
        GNS.getLogger().info(guidInfo.getName() + " allowed users of " + field + " : " + allowedusers);
      }
      if (checkAllowedUsers(accessorGuid, allowedusers, activeReplica, lnsAddress)) {
        if (debuggingEnabled) {
          GNS.getLogger().info("User " + accessorGuid + " allowed to access "
                  + (field != ALLFIELDS ? ("user " + guidInfo.getName() + "'s " + field + " field") : ("all of user " + guidInfo.getName() + "'s fields")));
        }
        return true;
      }
      return false;
    } catch (FieldNotFoundException e) {
      // This is actually a normal result.. so no warning here.
      return false;
    } catch (RecordNotFoundException e) {
      GNS.getLogger().warning("User " + accessorGuid + " access problem for " + guidInfo.getName() + "'s " + field + " field: " + e);
      return false;
    }
  }

  private static boolean checkAllowedUsers(String accessorGuid,
          Set<String> allowedUsers, GnsApplicationInterface activeReplica,
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

  public static boolean fieldAccessibleByEveryone(MetaDataTypeName access, String guid, String field,
          GnsApplicationInterface activeReplica) throws FailedDBOperationException {
    try {
      return NSFieldMetaData.lookupOnThisNameServer(access, guid, field, activeReplica).contains(EVERYONE)
              || NSFieldMetaData.lookupOnThisNameServer(access, guid, ALLFIELDS, activeReplica).contains(EVERYONE);
    } catch (FieldNotFoundException e) {
      // This is actually a normal result.. so no warning here.
      return false;
    } catch (RecordNotFoundException e) {
      GNS.getLogger().warning("User " + guid + " access problem for " + field + "'s " + access.toString() + " field: " + e);
      return false;
    }
  }
  
  public static Set<String> lookupPublicKeysFromAcl(MetaDataTypeName access, String guid, String field,
          GnsApplicationInterface activeReplica) throws FailedDBOperationException {
    if (debuggingEnabled) {
      GNS.getLogger().info("###field=" + field);
    }
    try {
      return (Set<String>)(Set<?>)NSFieldMetaData.lookupOnThisNameServer(access, guid, field, activeReplica);
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
