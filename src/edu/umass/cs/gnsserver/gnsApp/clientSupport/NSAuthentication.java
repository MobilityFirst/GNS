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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.ClientUtils;
import static edu.umass.cs.gnscommon.GnsProtocol.*;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.GuidInfo;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.MetaDataTypeName;
import edu.umass.cs.gnsserver.exceptions.FailedDBOperationException;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.gnsApp.AppReconfigurableNodeOptions;
import edu.umass.cs.gnsserver.gnsApp.GnsApplicationInterface;
import edu.umass.cs.gnsserver.gnsApp.NSResponseCode;
import edu.umass.cs.utils.DelayProfiler;
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

  private static final Cache<String, String> publicKeyCache = CacheBuilder.newBuilder().concurrencyLevel(5).maximumSize(1000).build();

  /**
   * Does access and signature checking for a field in a guid.
   * 
   * @param guid - the guid containing the field being accessed
   * @param field - the field being accessed
   * @param accessorGuid - the guid doing the access
   * @param signature
   * @param message
   * @param access - the type of access
   * @param gnsApp
   * @param lnsAddress - used in case we need to do a query for more records
   * @return an {@link NSResponseCode}
   * @throws InvalidKeyException
   * @throws InvalidKeySpecException
   * @throws SignatureException
   * @throws NoSuchAlgorithmException
   * @throws FailedDBOperationException
   * @throws UnsupportedEncodingException
   */
  public static NSResponseCode signatureAndACLCheck(String guid, String field, String accessorGuid, String signature,
          String message, MetaDataTypeName access, 
          GnsApplicationInterface<String> gnsApp, InetSocketAddress lnsAddress)
          throws InvalidKeyException, InvalidKeySpecException, SignatureException, NoSuchAlgorithmException,
          FailedDBOperationException, UnsupportedEncodingException {
    final long aclStartTime = System.currentTimeMillis();
    // First we do the ACL check. By doing this now we also look up the public key as
    // side effect which we need for the signing check below.
    String publicKey;
    boolean aclCheckPassed = false;
    if (accessorGuid.equals(guid)) {
      // The simple case where we're accesing our own guid
      final long startTime = System.currentTimeMillis();
      publicKey = lookupPublicKeyFromGuid(guid, gnsApp);
      DelayProfiler.updateDelay("authACLLookupPublicKey", startTime);
      if (publicKey == null) {
        return NSResponseCode.BAD_GUID_ERROR;
      }
      aclCheckPassed = true;
    } else {
      // Otherwise we attempt to find the public key for the accessorGuid in the ACL of the guid being
      // accesssed.
      publicKey = lookupPublicKeyInAcl(guid, field, accessorGuid, access, gnsApp, lnsAddress);
      if (publicKey != null) {
        // If we found the public key in the lookupPublicKey call then our access control list
        // check is done.
        aclCheckPassed = true;
        // otherwise handle the other cases (group guid in acl) with a last ditch lookup
      } else {
        GuidInfo accessorGuidInfo;
        if ((accessorGuidInfo = NSAccountAccess.lookupGuidInfo(accessorGuid, true, gnsApp, lnsAddress)) != null) {
          if (AppReconfigurableNodeOptions.debuggingEnabled) {
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
      // Otherwise, in case we found the accessor public key by looking on another server
      // but we still need to verify the ACL.
      // Our last attempt to check the ACL - handles all the edge cases like group guid in acl
      // FIXME: This ACL check Probably does more than it needs to.
      aclCheckPassed = NSAccessSupport.verifyAccess(access, guid, field, accessorGuid, gnsApp, lnsAddress);
    }
    DelayProfiler.updateDelay("authACL", aclStartTime);
    long sigStartTime = System.currentTimeMillis();
    // now check signatures
    if (signature == null) {
      if (!NSAccessSupport.fieldAccessibleByEveryone(access, guid, field, gnsApp)) {
        if (AppReconfigurableNodeOptions.debuggingEnabled) {
          GNS.getLogger().info("Name " + guid + " key = " + field + ": ACCESS_ERROR");
        }
        return NSResponseCode.ACCESS_ERROR;
      }
    } else if (signature != null) {
      if (!NSAccessSupport.verifySignature(publicKey, signature, message)) {
        if (AppReconfigurableNodeOptions.debuggingEnabled) {
          GNS.getLogger().info("Name " + guid + " key = " + field + ": SIGNATURE_ERROR");
        }
        return NSResponseCode.SIGNATURE_ERROR;
        //} else if (!NSAccessSupport.verifyAccess(access, guidInfo, field, accessorGuid, gnsApp, lnsAddress)) {
      } else if (!aclCheckPassed) {
        if (AppReconfigurableNodeOptions.debuggingEnabled) {
          GNS.getLogger().info("Name " + guid + " key = " + field + ": ACCESS_ERROR");
        }
        return NSResponseCode.ACCESS_ERROR;
      }
    }
    DelayProfiler.updateDelay("authSigCheck", sigStartTime);

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
   * @return the public key
   * @throws FailedDBOperationException
   */
  private static String lookupPublicKeyInAcl(String guid, String field, String accessorGuid,
          MetaDataTypeName access, GnsApplicationInterface<String> gnsApp, InetSocketAddress lnsAddress)
          throws FailedDBOperationException {
    String publicKey;
    Set<String> publicKeys = NSAccessSupport.lookupPublicKeysFromAcl(access, guid, field, gnsApp);
    publicKey = ClientUtils.findPublicKeyForGuid(accessorGuid, publicKeys);
    if (AppReconfigurableNodeOptions.debuggingEnabled) {
      GNS.getLogger().info("================> " + access.toString() + " Lookup for " + field + " returned: " + publicKey + " public keys=" + publicKeys);
    }
    if (publicKey == null) {
      // also catch all the keys that are stored in the +ALL+ record
      publicKeys.addAll(NSAccessSupport.lookupPublicKeysFromAcl(access, guid, ALL_FIELDS, gnsApp));
      publicKey = ClientUtils.findPublicKeyForGuid(accessorGuid, publicKeys);
      if (AppReconfigurableNodeOptions.debuggingEnabled) {
        GNS.getLogger().info("================> " + access.toString() + " Lookup with +ALL+ returned: " + publicKey + " public keys=" + publicKeys);
      }
    }
    // See if public keys contains EVERYONE which means we need to go old school and lookup the guid 
    // because it's not going to have an entry in the ACL
    if (publicKey == null && publicKeys.contains(EVERYONE)) {
      GuidInfo accessorGuidInfo;
      if ((accessorGuidInfo = NSAccountAccess.lookupGuidInfo(accessorGuid, true, gnsApp, lnsAddress)) != null) {
        if (AppReconfigurableNodeOptions.debuggingEnabled) {
          GNS.getLogger().info("================> " + access.toString() + " Lookup for EVERYONE returned: " + accessorGuidInfo);
        }
        publicKey = accessorGuidInfo.getPublicKey();
      }
    }
    if (publicKey == null) {
      if (AppReconfigurableNodeOptions.debuggingEnabled) {
        GNS.getLogger().info("================> Public key not found: accessor=" + accessorGuid + " guid= " + guid
                + " field= " + field + " public keys=" + publicKeys);
      }
    }
    return publicKey;
  }

  private static String lookupPublicKeyFromGuid(String guid, GnsApplicationInterface<String> gnsApp)
          throws FailedDBOperationException {
    String result;
    if ((result = publicKeyCache.getIfPresent(guid)) != null) {
      return result;
    }
    GuidInfo guidInfo;
    if ((guidInfo = NSAccountAccess.lookupGuidInfo(guid, gnsApp)) == null) {
      if (AppReconfigurableNodeOptions.debuggingEnabled) {
        GNS.getLogger().info("Name " + guid + " : BAD_GUID_ERROR");
      }
      return null;
    } else {
      result = guidInfo.getPublicKey();
      publicKeyCache.put(guid, result);
      return result;
    }
  }

}
