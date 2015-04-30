/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.nsdesign.clientsupport;

import edu.umass.cs.gns.clientsupport.ClientUtils;
import edu.umass.cs.gns.clientsupport.Defs;
import static edu.umass.cs.gns.clientsupport.Defs.*;
import edu.umass.cs.gns.clientsupport.GuidInfo;
import edu.umass.cs.gns.clientsupport.MetaDataTypeName;
import edu.umass.cs.gns.exceptions.FailedDBOperationException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.GnsApplicationInterface;
import edu.umass.cs.gns.util.NSResponseCode;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.Set;

/**
 *
 * @author westy
 */
public class NSAuthentication {

  public static NSResponseCode signatureAndACLCheck(String guid, String field, String accessorGuid, String signature,
          String message, MetaDataTypeName access, GnsApplicationInterface gnsApp, InetSocketAddress lnsAddress)
          throws InvalidKeyException, InvalidKeySpecException, SignatureException, NoSuchAlgorithmException,
          FailedDBOperationException, UnsupportedEncodingException {
    GuidInfo guidInfo;
    if ((guidInfo = NSAccountAccess.lookupGuidInfo(guid, gnsApp, lnsAddress)) == null) {
      if (Config.debuggingEnabled) {
        GNS.getLogger().info("Name " + guid + " key = " + field + ": BAD_GUID_ERROR");
      }
      return NSResponseCode.BAD_GUID_ERROR;
    }
    String publicKey;
    if (accessorGuid.equals(guid)) {
      publicKey = guidInfo.getPublicKey();
    } else {
      publicKey = lookupPublicKey(guid, field, accessorGuid, access, gnsApp, lnsAddress);
      if (publicKey == null) {
        return NSResponseCode.BAD_ACCESSOR_ERROR;
      }
    }

    if (signature == null) {
      if (!NSAccessSupport.fieldAccessibleByEveryone(access, guidInfo.getGuid(), field, gnsApp)) {
        if (Config.debuggingEnabled) {
          GNS.getLogger().info("Name " + guid + " key = " + field + ": ACCESS_ERROR");
        }
        return NSResponseCode.ACCESS_ERROR;
      }
    } else if (signature != null) {
      if (!NSAccessSupport.verifySignature(publicKey, signature, message)) {
        if (Config.debuggingEnabled) {
          GNS.getLogger().info("Name " + guid + " key = " + field + ": SIGNATURE_ERROR");
        }
        return NSResponseCode.SIGNATURE_ERROR;
      } else if (!NSAccessSupport.verifyAccess(access, guidInfo, field, accessorGuid,
              gnsApp, lnsAddress)) {
        if (Config.debuggingEnabled) {
          GNS.getLogger().info("Name " + guid + " key = " + field + ": ACCESS_ERROR");
        }
        return NSResponseCode.ACCESS_ERROR;
      }
    }
    return NSResponseCode.NO_ERROR;
  }

  /**
   * Attempts to look up the public key for a accessorGuid using the
   * ACL of the guid for the given field.
   * Will resort to a lookup on another server in certain circumstances.
   * Like when an ACL uses the EVERYONE flag.
   * 
   * @param guid
   * @param field
   * @param accessorGuid
   * @param access
   * @param gnsApp
   * @param lnsAddress
   * @return
   * @throws FailedDBOperationException 
   */
  private static String lookupPublicKey(String guid, String field, String accessorGuid,
          MetaDataTypeName access, GnsApplicationInterface gnsApp, InetSocketAddress lnsAddress)
          throws FailedDBOperationException {
    String publicKey;
    Set<String> publicKeys = NSAccessSupport.lookupPublicKeysFromAcl(access, guid, field, gnsApp);
    publicKey = ClientUtils.findPublicKeyForGuid(accessorGuid, publicKeys);
    if (Config.debuggingEnabled) {
      GNS.getLogger().info("================> Lookup for " + field + " returned: " + publicKey + " public keys=" + publicKeys);
    }
    if (publicKey == null) {
      // also catch all the keys that are stored in the +ALL+ record
      publicKeys.addAll(NSAccessSupport.lookupPublicKeysFromAcl(access, guid, ALLFIELDS, gnsApp));
      publicKey = ClientUtils.findPublicKeyForGuid(accessorGuid, publicKeys);
      if (Config.debuggingEnabled) {
        GNS.getLogger().info("================> Lookup with +ALL+ returned: " + publicKey + " public keys=" + publicKeys);
      }
    }
    // See if public keys contains EVERYONE which means we need to go old school and lookup the guid 
    // because it's not going to have an entry in the ACL
    if (publicKey == null && publicKeys.contains(EVERYONE)) {
      GuidInfo accessorGuidInfo;
      if ((accessorGuidInfo = NSAccountAccess.lookupGuidInfo(accessorGuid, true, gnsApp, lnsAddress)) != null) {
        if (Config.debuggingEnabled) {
          GNS.getLogger().info("================> Lookup for EVERYONE returned: " + accessorGuidInfo);
        }
        publicKey = accessorGuidInfo.getPublicKey();
      }
    }
    if (publicKey == null) {
      if (Config.debuggingEnabled) {
        GNS.getLogger().info("================> Public key not found: accessor=" + accessorGuid + " guid= " + guid
                + " field= " + field + " public keys=" + publicKeys);
      }
    }
    return publicKey;
  }

}
