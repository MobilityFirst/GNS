/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.nameserver.clientsupport;

import static edu.umass.cs.gns.clientsupport.Defs.*;
import edu.umass.cs.gns.clientsupport.GroupAccess;
import edu.umass.cs.gns.clientsupport.GuidInfo;
import edu.umass.cs.gns.clientsupport.MetaDataTypeName;
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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Provides signing and ACL checks for commands.
 * 
 * @author westy
 */
public class NSAccessSupport {
  
  // try this for now
  private static final Set<String> WORLDREADABLEFIELDS = new HashSet<String>(Arrays.asList(GroupAccess.JOINREQUESTS, GroupAccess.LEAVEREQUESTS));

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
    GNS.getLogger().fine("User: " + guidInfo.getName() + " Reader: " + accessorInfo.getName() + " Field: " + field);
    if (guidInfo.getGuid().equals(accessorInfo.getGuid())) {
      return true; // can always read your own stuff
    } else if (checkForAccess(access, guidInfo, field, accessorInfo)) {
      return true; // accessor can see this field
    } else if (checkForAccess(access, guidInfo, ALLFIELDS, accessorInfo)) {
      return true; // accessor can see all fields
    } else {
      GNS.getLogger().fine("User " + accessorInfo.getName() + " NOT allowed to access user " + guidInfo.getName() + "'s " + field + " field");
      return false;
    }
  }
  
  private static boolean checkForAccess(MetaDataTypeName access, GuidInfo guidInfo, String field, GuidInfo accessorInfo) {
    // first check the always world readable ones
    if (WORLDREADABLEFIELDS.contains(field)) {
      return true;
    }
    try {
      Set<String> allowedusers = NSFieldMetaData.lookup(access, guidInfo, field);
      GNS.getLogger().fine(guidInfo.getName() + " allowed users of " + field + " : " + allowedusers);
      if (checkAllowedUsers(accessorInfo.getGuid(), allowedusers)) {
        GNS.getLogger().fine("User " + accessorInfo.getName() + " allowed to access "
                + (field != ALLFIELDS ? ("user " + guidInfo.getName() + "'s " + field + " field") : ("all of user " + guidInfo.getName() + "'s fields")));
        return true;
      }
      return false;
    } catch (FieldNotFoundException e) {
      // This is actuallty a normal result.. so no warning here.
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
      // This is actuallty a normal result.. so no warning here.
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

  public static boolean fieldAccessibleByEveryone(MetaDataTypeName access, String guid, String field) {
    try {
      return NSFieldMetaData.lookup(access, guid, field).contains(EVERYONE)
              || NSFieldMetaData.lookup(access, guid, ALLFIELDS).contains(EVERYONE);
    } catch (FieldNotFoundException e) {
      // This is actuallty a normal result.. so no warning here.
      return false;
    } catch (RecordNotFoundException e) {
      GNS.getLogger().warning("User " + guid + " access problem for " + field + "'s " + access.toString() + " field: " + e);
      return false;
    }
  }
}
