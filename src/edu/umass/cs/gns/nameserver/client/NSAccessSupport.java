/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.nameserver.client;

import static edu.umass.cs.gns.clientprotocol.Defs.*;
import edu.umass.cs.gns.client.GuidInfo;
import edu.umass.cs.gns.client.MetaDataTypeName;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
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
import java.util.Set;

/**
 * Provides signing and ACL checks for commands.
 * 
 * @author westy
 */
public class NSAccessSupport {

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
    GNS.getLogger().info("User " + guidInfo.getName() + (result ? " verified " : " NOT verified ") + "as author of message " + message);
    return result;
  }

  /**
   * Checks to see if the reader given in readerInfo can access all of the fields of the user given by guidInfo.
   *
   * @param access
   * @param contectInfo
   * @param readerInfo
   * @return
   */
  public static boolean verifyAccess(MetaDataTypeName access, GuidInfo contectInfo, GuidInfo readerInfo) {
    return verifyAccess(access, contectInfo, ALLFIELDS, readerInfo);
  }

  /**
   * Checks to see if the reader given in readerInfo can access the field of the user given by guidInfo. Access type is some combo
   * of read, write, blacklist and whitelist. Note: Blacklists are currently not activated.
   *
   * @param access
   * @param guidInfo
   * @param field
   * @param accessorInfo
   * @return
   */
  public static boolean verifyAccess(MetaDataTypeName access, GuidInfo guidInfo, String field, GuidInfo accessorInfo) {
    try {
      GNS.getLogger().finer("User: " + guidInfo.getName() + " Reader: " + accessorInfo.getName() + " Field: " + field);
      if (guidInfo.getGuid().equals(accessorInfo.getGuid())) {
        return true; // can always read your own stuff
      } else {
        Set<String> allowedusers = NSFieldMetaData.lookup(access, guidInfo, field);
        GNS.getLogger().fine(guidInfo.getName() + " allowed users of " + field + " : " + allowedusers);
        if (checkAllowedUsers(accessorInfo.getGuid(), allowedusers)) {
          GNS.getLogger().fine("User " + accessorInfo.getName() + " allowed to access user " + guidInfo.getName() + "'s " + field + " field");
          return true;
        }
        // otherwise find any users that can access all of the fields
        allowedusers = NSFieldMetaData.lookup(access, guidInfo, ALLFIELDS);
        if (checkAllowedUsers(accessorInfo.getGuid(), allowedusers)) {
          GNS.getLogger().fine("User " + accessorInfo.getName() + " allowed to access all of user " + guidInfo.getName() + "'s fields");
          return true;
        }
      }
      GNS.getLogger().fine("User " + accessorInfo.getName() + " NOT allowed to access user " + guidInfo.getName() + "'s " + field + " field");
      return false;
    } catch (FieldNotFoundException e) {
      GNS.getLogger().warning("User " + accessorInfo.getName() + " access problem for" + guidInfo.getName() + "'s " + field + " field: " + e);
      return false;
    } catch (RecordNotFoundException e) {
      GNS.getLogger().warning("User " + accessorInfo.getName() + " access problem for " + guidInfo.getName() + "'s " + field + " field: " + e);
      return false;
    }
  }

  private static boolean checkAllowedUsers(String accesserGuid, Set<String> allowedusers) {
    try {
      if (allowedusers.contains(accesserGuid)) {
        return true;
      } else if (allowedusers.contains(EVERYONE)) {
        return true;
      } else {
        // map over the allowedusers and see if any of them are groups that the user belongs to
        for (String potentialGroupGuid : allowedusers) {
          if (NSGroupAccess.lookup(potentialGroupGuid).contains(accesserGuid)) {
            return true;
          }
        }
        return false;
      }
    } catch (FieldNotFoundException e) {
      GNS.getLogger().warning("Guid " + accesserGuid + " problem retrieving group: " + e);
      return false;
    } catch (RecordNotFoundException e) {
      GNS.getLogger().warning("Guid " + accesserGuid + " problem retrieving group: " + e);
      return false;
    }
  }

  public static String removeSignature(String fullString, String fullSignatureField) {
    GNS.getLogger().finer("fullstring = " + fullString + " fullSignatureField = " + fullSignatureField);
    String result = fullString.substring(0, fullString.lastIndexOf(fullSignatureField));
    GNS.getLogger().finer("result = " + result);
    return result;
  }

  public static boolean fieldReadableByEveryone(String guid, String field) {
    try {
      return NSFieldMetaData.lookup(MetaDataTypeName.READ_WHITELIST, guid, field).contains(EVERYONE)
              || NSFieldMetaData.lookup(MetaDataTypeName.READ_WHITELIST, guid, ALLFIELDS).contains(EVERYONE);
    } catch (FieldNotFoundException e) {
      GNS.getLogger().warning("User " + guid + " access problem for" + field + "'s " + "READ_WHITELIST" + " field: " + e);
      return false;
    } catch (RecordNotFoundException e) {
      GNS.getLogger().warning("User " + guid + " access problem for " + field + "'s " + "READ_WHITELIST" + " field: " + e);
      return false;
    }
  }

  public static boolean fieldWriteableByEveryone(String guid, String field) {
    try {
      return NSFieldMetaData.lookup(MetaDataTypeName.WRITE_WHITELIST, guid, field).contains(EVERYONE)
              || NSFieldMetaData.lookup(MetaDataTypeName.WRITE_WHITELIST, guid, ALLFIELDS).contains(EVERYONE);
    } catch (FieldNotFoundException e) {
      GNS.getLogger().warning("User " + guid + " access problem for" + field + "'s " + "WRITE_WHITELIST" + " field: " + e);
      return false;
    } catch (RecordNotFoundException e) {
      GNS.getLogger().warning("User " + guid + " access problem for " + field + "'s " + "WRITE_WHITELIST" + " field: " + e);
      return false;
    }
  }
}
