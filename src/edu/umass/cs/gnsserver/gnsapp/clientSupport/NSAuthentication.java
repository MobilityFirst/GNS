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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import static edu.umass.cs.gnscommon.GNSCommandProtocol.*;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnscommon.GNSResponseCode;
import edu.umass.cs.gnscommon.SharedGuidUtils;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.GuidInfo;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.MetaDataTypeName;
import edu.umass.cs.gnsserver.gnsapp.deprecated.GNSApplicationInterface;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;

final class AclResult {

  private final String publicKey;
  private final boolean aclCheckPassed;
  private final GNSResponseCode responseCode;

  public AclResult(String publicKey, boolean aclCheckPassed, GNSResponseCode responseCode) {
    this.publicKey = publicKey;
    this.aclCheckPassed = aclCheckPassed;
    this.responseCode = responseCode;
  }

  public String getPublicKey() {
    return publicKey;
  }

  public boolean isAclCheckPassed() {
    return aclCheckPassed;
  }

  public GNSResponseCode getResponseCode() {
    return responseCode;
  }

}

/**
 *
 * @author westy
 */
public class NSAuthentication {

  private static final Cache<String, String> PUBLIC_KEY_CACHE
          = CacheBuilder.newBuilder().concurrencyLevel(5).maximumSize(1000).build();

  private static AclResult aclCheck(String targetGuid, String field,
          String accessorGuid, MetaDataTypeName access,
          GNSApplicationInterface<String> gnsApp) throws FailedDBOperationException {
    ClientSupportConfig.getLogger().log(Level.FINE,
            "ACL Check guid={0} key={1} accessor={2} access={3}", new Object[]{targetGuid, field, accessorGuid, access});

    // This method attempts to look up the public key as well as check for ACL access.
    String publicKey;
    boolean aclCheckPassed = false;
    if (accessorGuid.equals(targetGuid)) {
      // This handles the simple case where we're accessing our own guid. 
      // Access to all of our fields is always allowed to our own guid so we just need to get
      // the public key out of the guid - possibly from the cache.
      publicKey = lookupPublicKeyFromGuidLocallyWithCacheing(targetGuid, gnsApp);
      // Return an error immediately here because if we can't find the public key 
      // the guid must not be local which is a problem.
      if (publicKey == null) {
        return new AclResult("", false, GNSResponseCode.BAD_GUID_ERROR);
      }
      aclCheckPassed = true;
    } else {
      // Otherwise we attempt to find the public key for the accessorGuid in the ACL of the guid being
      // accesssed.
      publicKey = lookupPublicKeyInACL(targetGuid, field, accessorGuid, access, gnsApp);
      if (publicKey != null) {
        // If we found the public key in the lookupPublicKey call then our access control list
        // check is done because the public key of the accessorGuid is in the given acl of targetGuid.
        aclCheckPassed = true;
      } else {
        // One final case we need to handles is when the the accessorGuid is actually in a group guid
        // and the group guid is in the acl in which case we need to explicitly lookup the 
        // publickey in possibly another server. 
        GuidInfo accessorGuidInfo;
        if ((accessorGuidInfo = NSAccountAccess.lookupGuidInfoAnywhere(accessorGuid, gnsApp)) != null) {
          ClientSupportConfig.getLogger().log(Level.FINE,
                  "================> Catchall lookup returned: {0}", accessorGuidInfo);
          publicKey = accessorGuidInfo.getPublicKey();
        }
      }
    }
    if (publicKey == null) {
      // If we haven't found the publicKey of the accessorGuid yet it's not allowed access.
      return new AclResult("", false, GNSResponseCode.BAD_ACCESSOR_ERROR);
    } else if (!aclCheckPassed) {
      // Otherwise, we need to find out if this accessorGuid is in a group guid that
      // is in the acl of the field.
      // Note that this a full-blown acl check that checks all the cases, but currenly it
      // is only used for checking the case where the accessorGuid is in a group guid
      // that is in the acl.
      aclCheckPassed = NSAccessSupport.verifyAccess(access, targetGuid, field, accessorGuid, gnsApp);
    }
    return new AclResult(publicKey, aclCheckPassed, GNSResponseCode.NO_ERROR);
  }

  /**
   * Does access and signature checking for a field in a guid.
   *
   * @param guid - the guid containing the field being accessed
   * @param field - the field being accessed (one of this or fields should be non-null)
   * @param fields - or the fields beig accessed (one of this or field should be non-null)
   * @param accessorGuid - the guid doing the access
   * @param signature
   * @param message
   * @param access - the type of access
   * @param gnsApp
   * @return an {@link GNSResponseCode}
   * @throws InvalidKeyException
   * @throws InvalidKeySpecException
   * @throws SignatureException
   * @throws NoSuchAlgorithmException
   * @throws FailedDBOperationException
   * @throws UnsupportedEncodingException
   */
  public static GNSResponseCode signatureAndACLCheck(String guid,
          String field, List<String> fields,
          String accessorGuid, String signature,
          String message, MetaDataTypeName access,
          GNSApplicationInterface<String> gnsApp)
          throws InvalidKeyException, InvalidKeySpecException, SignatureException, NoSuchAlgorithmException,
          FailedDBOperationException, UnsupportedEncodingException {
    // First we do the ACL check. By doing this now we also look up the public key as
    // side effect which we need for the signing check below.
    AclResult aclResult = null;
    if (field != null) {
      aclResult = aclCheck(guid, field, accessorGuid, access, gnsApp);
      if (aclResult.getResponseCode().isExceptionOrError()) {
        return aclResult.getResponseCode();
      }
    } else if (fields != null) {
      // Check each field individually.
      for (String aField : fields) {
        aclResult = aclCheck(guid, aField, accessorGuid, access, gnsApp);
        if (aclResult.getResponseCode().isExceptionOrError()) {
          return aclResult.getResponseCode();
        }
      }
    }
    if (aclResult == null) {
      assert (false) : "Should never come here";
      // Something went wrong above, but we shouldn't really get here.
      ClientSupportConfig.getLogger().log(Level.FINE,
              "Name {0} key={1} : ACCESS_ERROR", new Object[]{guid, field});
      return GNSResponseCode.ACCESS_ERROR;
    }

    String publicKey = aclResult.getPublicKey();
    boolean aclCheckPassed = aclResult.isAclCheckPassed();
    // now check signatures
    if (signature == null) {
      if (!NSAccessSupport.fieldAccessibleByEveryone(access, guid, field, gnsApp)) {
        ClientSupportConfig.getLogger().log(Level.FINE,
                "Name {0} key={1} : ACCESS_ERROR", new Object[]{guid, field});
        return GNSResponseCode.ACCESS_ERROR;
      }
    } else if (!NSAccessSupport.verifySignature(publicKey, signature, message)) {
      ClientSupportConfig.getLogger().log(Level.FINE,
              "Name {0} key={1} : SIGNATURE_ERROR", new Object[]{guid, field});
      return GNSResponseCode.SIGNATURE_ERROR;
    } else if (!aclCheckPassed) {
      ClientSupportConfig.getLogger().log(Level.FINE,
              "Name {0} key={1} : ACCESS_ERROR", new Object[]{guid, field});
      return GNSResponseCode.ACCESS_ERROR;
    }

    return GNSResponseCode.NO_ERROR;
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
    publicKey = SharedGuidUtils.findPublicKeyForGuid(accessorGuid, publicKeys);
    ClientSupportConfig.getLogger().log(Level.FINE,
            "================> {0} lookup for {1} returned: {2} public keys={3}",
            new Object[]{access.toString(), field, publicKey,
              publicKeys});
    if (publicKey == null) {
      // also catch all the keys that are stored in the +ALL+ record
      publicKeys.addAll(NSAccessSupport.lookupPublicKeysFromAcl(access, guid, ALL_FIELDS, gnsApp.getDB()));
      publicKey = SharedGuidUtils.findPublicKeyForGuid(accessorGuid, publicKeys);
      GNSConfig.getLogger().log(Level.FINE,
              "================> {0} lookup with +ALL+ returned: {1} public keys={2}",
              new Object[]{access.toString(), publicKey, publicKeys});
    }
    // See if public keys contains EVERYONE which means we need to go old school and lookup the guid 
    // because it's not going to have an entry in the ACL
    if (publicKey == null && publicKeys.contains(EVERYONE)) {
      GuidInfo accessorGuidInfo;
      if ((accessorGuidInfo = NSAccountAccess.lookupGuidInfoAnywhere(accessorGuid, gnsApp)) != null) {
        GNSConfig.getLogger().log(Level.FINE,
                "================> {0} lookup for EVERYONE returned {1}",
                new Object[]{access.toString(), accessorGuidInfo});
        publicKey = accessorGuidInfo.getPublicKey();
      }
    }
    if (publicKey == null) {
      GNSConfig.getLogger().log(Level.FINE,
              "================> Public key not found: accessor={0} guid={1} field={2} public keys={3}",
              new Object[]{accessorGuid, guid, field, publicKeys});
    }
    return publicKey;
  }

  private static String lookupPublicKeyFromGuidLocallyWithCacheing(String guid, GNSApplicationInterface<String> gnsApp)
          throws FailedDBOperationException {
    String result;
    if ((result = PUBLIC_KEY_CACHE.getIfPresent(guid)) != null) {
      return result;
    }
    GuidInfo guidInfo;
    if ((guidInfo = NSAccountAccess.lookupGuidInfoLocally(guid, gnsApp)) == null) {
      ClientSupportConfig.getLogger().log(Level.FINE, "Name {0} : BAD_GUID_ERROR", new Object[]{guid});
      return null;
    } else {
      result = guidInfo.getPublicKey();
      PUBLIC_KEY_CACHE.put(guid, result);
      return result;
    }
  }

}
