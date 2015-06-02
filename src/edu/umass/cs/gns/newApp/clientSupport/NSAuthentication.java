/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.newApp.clientSupport;

import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.ClientUtils;
import static edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.Defs.*;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.GuidInfo;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.MetaDataTypeName;
import edu.umass.cs.gns.exceptions.FailedDBOperationException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.newApp.AppReconfigurableNode;
import edu.umass.cs.gns.newApp.GnsApplicationInterface;
import edu.umass.cs.gns.util.Format;
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
    final long aclStartTime = System.nanoTime();
    GuidInfo guidInfo;
    if ((guidInfo = NSAccountAccess.lookupGuidInfo(guid, gnsApp, lnsAddress)) == null) {
      if (AppReconfigurableNode.debuggingEnabled) {
        GNS.getLogger().info("Name " + guid + " key = " + field + ": BAD_GUID_ERROR");
      }
      return NSResponseCode.BAD_GUID_ERROR;
    }
    // First we do the ACL check. By doing this now we also look up the public key as
    // side effect which we need for the signing check below.
    String publicKey;
    boolean aclCheckPassed = false;
    if (accessorGuid.equals(guid)) {
      // The simple case where we're accesing our own guid
      publicKey = guidInfo.getPublicKey();
      aclCheckPassed = true;
    } else {
      // Otherwise we attempt to find the public key for the accessorGuid in the ACL of the guid being
      // accesssed.
      publicKey = lookupPublicKey(guid, field, accessorGuid, access, gnsApp, lnsAddress);
      if (publicKey != null) {
        // If we found the public key in the lookupPublicKey call then our access control list
        // check is done.
        aclCheckPassed = true;
        // otherwise handle the other cases (group guid in acl) with a last ditch lookup
      } else {
        GuidInfo accessorGuidInfo;
        if ((accessorGuidInfo = NSAccountAccess.lookupGuidInfo(accessorGuid, true, gnsApp, lnsAddress)) != null) {
          if (AppReconfigurableNode.debuggingEnabled) {
            GNS.getLogger().info("================> Catchall lookup returned: " + accessorGuidInfo);
          }
          publicKey = accessorGuidInfo.getPublicKey();
        }
      }
    }
    if (publicKey == null) {
      // If we haven't found the publicKey of the accessorGuid yet it's not allowed access
      return NSResponseCode.BAD_ACCESSOR_ERROR;
    } else if (!aclCheckPassed) {
      // Otherwise, in case we found the public key by looking on another server
      // but we still need to verify the ACL.
      // Our last attempt to check the ACL - handles all the edge cases like group guid in acl
      // FIXME: This ACL check Probably does more than it needs to.
      aclCheckPassed = NSAccessSupport.verifyAccess(access, guidInfo, field, accessorGuid, gnsApp, lnsAddress);
    }
    double aclDelayInMS = (System.nanoTime() - aclStartTime) / 1000000.0;
    if (AppReconfigurableNode.debuggingEnabled) {
      GNS.getLogger().info("8888888888888888888888888888>>>>:  ACL CHECK TIME AT THE APP " + Format.formatTime(aclDelayInMS) + "ms");
    }
    long authStartTime = System.nanoTime();
    // now check signatures
    if (signature == null) {
      if (!NSAccessSupport.fieldAccessibleByEveryone(access, guidInfo.getGuid(), field, gnsApp)) {
        if (AppReconfigurableNode.debuggingEnabled) {
          GNS.getLogger().info("Name " + guid + " key = " + field + ": ACCESS_ERROR");
        }
        return NSResponseCode.ACCESS_ERROR;
      }
    } else if (signature != null) {
      if (!NSAccessSupport.verifySignature(publicKey, signature, message)) {
        if (AppReconfigurableNode.debuggingEnabled) {
          GNS.getLogger().info("Name " + guid + " key = " + field + ": SIGNATURE_ERROR");
        }
        return NSResponseCode.SIGNATURE_ERROR;
        //} else if (!NSAccessSupport.verifyAccess(access, guidInfo, field, accessorGuid, gnsApp, lnsAddress)) {
      } else if (!aclCheckPassed) {
        if (AppReconfigurableNode.debuggingEnabled) {
          GNS.getLogger().info("Name " + guid + " key = " + field + ": ACCESS_ERROR");
        }
        return NSResponseCode.ACCESS_ERROR;
      }
    }
    double authDelayInMS = (System.nanoTime() - authStartTime) / 1000000.0;
    if (AppReconfigurableNode.debuggingEnabled) {
      GNS.getLogger().info("8888888888888888888888888888>>>>:  SIG CHECK TIME " + Format.formatTime(authDelayInMS) + "ms");
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
    if (AppReconfigurableNode.debuggingEnabled) {
      GNS.getLogger().info("================> " + access.toString() + " Lookup for " + field + " returned: " + publicKey + " public keys=" + publicKeys);
    }
    if (publicKey == null) {
      // also catch all the keys that are stored in the +ALL+ record
      publicKeys.addAll(NSAccessSupport.lookupPublicKeysFromAcl(access, guid, ALLFIELDS, gnsApp));
      publicKey = ClientUtils.findPublicKeyForGuid(accessorGuid, publicKeys);
      if (AppReconfigurableNode.debuggingEnabled) {
        GNS.getLogger().info("================> " + access.toString() + " Lookup with +ALL+ returned: " + publicKey + " public keys=" + publicKeys);
      }
    }
    // See if public keys contains EVERYONE which means we need to go old school and lookup the guid 
    // because it's not going to have an entry in the ACL
    if (publicKey == null && publicKeys.contains(EVERYONE)) {
      GuidInfo accessorGuidInfo;
      if ((accessorGuidInfo = NSAccountAccess.lookupGuidInfo(accessorGuid, true, gnsApp, lnsAddress)) != null) {
        if (AppReconfigurableNode.debuggingEnabled) {
          GNS.getLogger().info("================> " + access.toString() + " Lookup for EVERYONE returned: " + accessorGuidInfo);
        }
        publicKey = accessorGuidInfo.getPublicKey();
      }
    }
    if (publicKey == null) {
      if (AppReconfigurableNode.debuggingEnabled) {
        GNS.getLogger().info("================> Public key not found: accessor=" + accessorGuid + " guid= " + guid
                + " field= " + field + " public keys=" + publicKeys);
      }
    }
    return publicKey;
  }

}
