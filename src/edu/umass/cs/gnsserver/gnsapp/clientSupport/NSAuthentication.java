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
package edu.umass.cs.gnsserver.gnsapp.clientSupport;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import static edu.umass.cs.gnscommon.GnsProtocol.*;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.gnsapp.AppReconfigurableNodeOptions;
import edu.umass.cs.gnsserver.gnsapp.GNSApplicationInterface;
import edu.umass.cs.gnsserver.gnsapp.NSResponseCode;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.ClientUtils;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.GuidInfo;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.MetaDataTypeName;
import edu.umass.cs.gnsserver.gnsapp.recordmap.BasicRecordMap;
import edu.umass.cs.utils.DelayProfiler;

import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.Set;
import java.util.logging.Level;

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
   * @return an {@link NSResponseCode}
   * @throws InvalidKeyException
   * @throws InvalidKeySpecException
   * @throws SignatureException
   * @throws NoSuchAlgorithmException
   * @throws FailedDBOperationException
   * @throws UnsupportedEncodingException
   */
  public static NSResponseCode signatureAndACLCheck(String guid, String field,
          String accessorGuid, String signature,
          String message, MetaDataTypeName access,
          GNSApplicationInterface<String> gnsApp)
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
      DelayProfiler.updateDelay("lookupPublicKeyFromGuid", startTime);
      if (publicKey == null) {
        return NSResponseCode.BAD_GUID_ERROR;
      }
      aclCheckPassed = true;
    } else {
      // Otherwise we attempt to find the public key for the accessorGuid in the ACL of the guid being
      // accesssed.
    	long t= System.currentTimeMillis();
      publicKey = lookupPublicKeyInACL(guid, field, accessorGuid, access, gnsApp);
      DelayProfiler.updateDelay("lookupPublicKeyInACL", t);
      if (publicKey != null) {
        // If we found the public key in the lookupPublicKey call then our access control list
        // check is done.
        aclCheckPassed = true;
        // otherwise handle the case where the accessor is a group guid and we need to lookup the 
        // public key from the guid record which might not be local
      } else {
        GuidInfo accessorGuidInfo;
        if ((accessorGuidInfo = NSAccountAccess.lookupGuidInfo(accessorGuid, true, gnsApp)) != null) {
          if (AppReconfigurableNodeOptions.debuggingEnabled) {
            GNSConfig.getLogger().info("================> Catchall lookup returned: " + accessorGuidInfo);
          }
          publicKey = accessorGuidInfo.getPublicKey();
        }
      }
    }
    if (publicKey == null) {
      // If we haven't found the publicKey of the accessorGuid yet it's not allowed access
      return NSResponseCode.BAD_ACCESSOR_ERROR;
    } else if (!aclCheckPassed) {
      // Otherwise, we need to find out if this accessorGuid is in a group guid that
      // is in the acl of the field
      aclCheckPassed = NSAccessSupport.verifyAccess(access, guid, field, accessorGuid, gnsApp);

    }
    DelayProfiler.updateDelay("ACLCheck", aclStartTime);
    long sigStartTime = System.currentTimeMillis();
    // now check signatures
    if (signature == null) {
      if (!NSAccessSupport.fieldAccessibleByEveryone(access, guid, field, gnsApp)) {
        if (AppReconfigurableNodeOptions.debuggingEnabled) {
            GNSConfig.getLogger().log(Level.INFO, "Name {0} key={1} : ACESS_ERROR", new Object[]{guid, field});
        }
        return NSResponseCode.ACCESS_ERROR;
      }
    } else if (signature != null) {
      if (!NSAccessSupport.verifySignature(publicKey, signature, message)) {
        if (AppReconfigurableNodeOptions.debuggingEnabled) {
          GNSConfig.getLogger().log(Level.INFO, "Name {0} key={1} : SIGNATURE_ERROR", new Object[]{guid, field});
        }
        return NSResponseCode.SIGNATURE_ERROR;
      } else if (!aclCheckPassed) {
        if (AppReconfigurableNodeOptions.debuggingEnabled) {
            GNSConfig.getLogger().log(Level.INFO, "Name {0} key={1} : ACESS_ERROR", new Object[]{guid, field});
        }
        return NSResponseCode.ACCESS_ERROR;
      }
    }
    DelayProfiler.updateDelay("signatureCheck", sigStartTime);
    DelayProfiler.updateDelay("signatureAndACLCheck", aclStartTime);

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
  private static String lookupPublicKeyInACL(String guid, String field, String accessorGuid,
          MetaDataTypeName access, GNSApplicationInterface<String> gnsApp)
          throws FailedDBOperationException {
    String publicKey;
    Set<String> publicKeys = NSAccessSupport.lookupPublicKeysFromAcl(access, guid, field, gnsApp.getDB());
    publicKey = ClientUtils.findPublicKeyForGuid(accessorGuid, publicKeys);
    if (AppReconfigurableNodeOptions.debuggingEnabled) {
			GNSConfig
					.getLogger()
					.log(Level.INFO,
							"================> {0} lookup for {1} returned: {2} public keys={3}",
							new Object[] { access.toString(), field, publicKey,
									publicKeys });
    }
    if (publicKey == null) {
      // also catch all the keys that are stored in the +ALL+ record
      publicKeys.addAll(NSAccessSupport.lookupPublicKeysFromAcl(access, guid, ALL_FIELDS, gnsApp.getDB()));
      publicKey = ClientUtils.findPublicKeyForGuid(accessorGuid, publicKeys);
      if (AppReconfigurableNodeOptions.debuggingEnabled) {
				GNSConfig
						.getLogger()
						.log(Level.INFO,
								"================> {0} lookup with +ALL+ returned: {1} public keys={2}",
								new Object[] { access.toString(), publicKey,
										publicKeys });
      }
    }
    // See if public keys contains EVERYONE which means we need to go old school and lookup the guid 
    // because it's not going to have an entry in the ACL
    if (publicKey == null && publicKeys.contains(EVERYONE)) {
      GuidInfo accessorGuidInfo;
      if ((accessorGuidInfo = NSAccountAccess.lookupGuidInfo(accessorGuid, true, gnsApp)) != null) {
        if (AppReconfigurableNodeOptions.debuggingEnabled) {
					GNSConfig
							.getLogger()
							.log(Level.INFO,
									"================> {0} lookup for EVERYONE returned {1}",
									new Object[] { access.toString(),
											accessorGuidInfo });
        }
        publicKey = accessorGuidInfo.getPublicKey();
      }
    }
    if (publicKey == null) {
      if (AppReconfigurableNodeOptions.debuggingEnabled) {
				GNSConfig
						.getLogger()
						.log(Level.INFO,
								"================> Public key not found: accessor={0} guid={1} field={2} public keys={3}",
								new Object[] { accessorGuid, guid, field,
										publicKeys });
      }
    }
    return publicKey;
  }

  private static String lookupPublicKeyFromGuid(String guid, GNSApplicationInterface<String> gnsApp)
          throws FailedDBOperationException {
    String result;
    if ((result = publicKeyCache.getIfPresent(guid)) != null) {
      return result;
    }
    GuidInfo guidInfo;
    if ((guidInfo = NSAccountAccess.lookupGuidInfo(guid, gnsApp)) == null) {
      if (AppReconfigurableNodeOptions.debuggingEnabled) {
        GNSConfig.getLogger().log(Level.INFO, "Name {0} : BAD_GUID_ERROR", new Object[]{guid});
      }
      return null;
    } else {
      result = guidInfo.getPublicKey();
      publicKeyCache.put(guid, result);
      return result;
    }
  }

}
