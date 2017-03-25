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

import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.SharedGuidUtils;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.GuidInfo;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.MetaDataTypeName;
import edu.umass.cs.gnsserver.gnsapp.GNSApplicationInterface;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import edu.umass.cs.gnscommon.GNSProtocol;

/**
 *
 * @author westy
 */
public class NSAuthentication {

  private static final Cache<String, String> PUBLIC_KEY_CACHE
          = CacheBuilder.newBuilder().concurrencyLevel(5).maximumSize(1000).build();

  /**
   * Does access and signature checking for a field OR fields in a guid.
   * For explicit multi-field access all fields must be accessible or
   * ACL check fails.
   *
   * @param guid - the guid containing the field being accessed
   * @param field - the field being accessed (one of this or fields should be non-null)
   * @param fields - or the fields being accessed (one of this or field should be non-null)
   * @param accessorGuid - the guid doing the access
   * @param signature
   * @param message
   * @param access - the type of access
   * @param gnsApp
   * @return an {@link ResponseCode}
   * @throws InvalidKeyException
   * @throws InvalidKeySpecException
   * @throws SignatureException
   * @throws NoSuchAlgorithmException
   * @throws FailedDBOperationException
   * @throws UnsupportedEncodingException
   */
  public static ResponseCode signatureAndACLCheck(InternalRequestHeader header, String guid,
          String field, List<String> fields,
          String accessorGuid, String signature,
          String message, MetaDataTypeName access,
          GNSApplicationInterface<String> gnsApp)
          throws InvalidKeyException, InvalidKeySpecException, SignatureException, NoSuchAlgorithmException,
          FailedDBOperationException, UnsupportedEncodingException {
    return signatureAndACLCheck(header, guid, field, fields, accessorGuid, signature,
            message, access, gnsApp, false);
  }

  /**
   * Does access and signature checking for a field OR fields in a guid.
   * For explicit multi-field access all fields must be accessible or
   * ACL check fails.
   *
   * @param guid - the guid containing the field being accessed
   * @param field - the field being accessed (one of this or fields should be non-null)
   * @param fields - or the fields being accessed (one of this or field should be non-null)
   * @param accessorGuid - the guid doing the access
   * @param signature
   * @param message
   * @param access - the type of access
   * @param gnsApp
   * @param skipSigCheck
   * @return an {@link ResponseCode}
   * @throws InvalidKeyException
   * @throws InvalidKeySpecException
   * @throws SignatureException
   * @throws NoSuchAlgorithmException
   * @throws FailedDBOperationException
   * @throws UnsupportedEncodingException
   */
  public static ResponseCode signatureAndACLCheck(InternalRequestHeader header, String guid,
          String field, List<String> fields,
          String accessorGuid, String signature,
          String message, MetaDataTypeName access,
          GNSApplicationInterface<String> gnsApp, boolean skipSigCheck)
          throws InvalidKeyException, InvalidKeySpecException, SignatureException, NoSuchAlgorithmException,
          FailedDBOperationException, UnsupportedEncodingException {
    // Do a check for unsigned reads if there is no signature
    if ((!skipSigCheck && signature == null) || accessorGuid == null) {
      if (NSAccessSupport.fieldAccessibleByEveryone(access, guid, field, gnsApp)) {
        return ResponseCode.NO_ERROR;
      } else {
        ClientSupportConfig.getLogger().log(Level.FINE, "Name {0} key={1} : ACCESS_ERROR",
                new Object[]{guid, field});
        return ResponseCode.ACCESS_ERROR;
      }
    }
    // If the signature is being checking and isn't null a null accessorGuid is also an access failure because
    // only unsigned reads (handled above) can have a null accessorGuid
    if (!skipSigCheck && accessorGuid == null) {
      ClientSupportConfig.getLogger().log(Level.WARNING, "Name {0} key={1} : NULL accessorGuid",
              new Object[]{guid, field});
      return ResponseCode.ACCESS_ERROR;
    }

    // Now we do the ACL check. By doing this now we also look up the public key as
    // side effect which we need for the signing check below.
    AclCheckResult aclResult = null;
    if (field != null) {
      // Remember that field can also be GNSProtocol.ENTIRE_RECORD.toString()
      aclResult = aclCheck(header, guid, field, accessorGuid, access, gnsApp);
      if (aclResult.getResponseCode().isExceptionOrError()) {
        return aclResult.getResponseCode();
      }
    } else if (fields != null) {
      // Check each field individually; if any field doesn't pass the entire access fails.
      for (String aField : fields) {
        aclResult = aclCheck(header, guid, aField, accessorGuid, access, gnsApp);
        if (aclResult.getResponseCode().isExceptionOrError()) {
          return aclResult.getResponseCode();
        }
      }
      // If one field or fields is not null then this is also an access error.
    } else {
      ClientSupportConfig.getLogger().log(Level.SEVERE, "Name {0} key={1} : Field and Fields are both NULL",
              new Object[]{guid, field});
      return ResponseCode.ACCESS_ERROR;
    }

    if (skipSigCheck) {
      return ResponseCode.NO_ERROR;
    } else // Now check signatures
    if (NSAccessSupport.verifySignature(aclResult.getPublicKey(), signature, message)) {
      return ResponseCode.NO_ERROR;
    } else {
      ClientSupportConfig.getLogger().log(Level.FINE,
              "Name {0} key={1} : SIGNATURE_ERROR", new Object[]{guid, field});
      return ResponseCode.SIGNATURE_ERROR;
    }
  }

  /**
   * Check the acl to insure that {@code accessorGuid} can access {@code targetGuid}'s {@code field}.
   *
   * @param header
   * @param targetGuid
   * @param field
   * @param accessorGuid
   * @param access
   * @param gnsApp
   * @return
   * @throws FailedDBOperationException
   */
  public static AclCheckResult aclCheck(InternalRequestHeader header, String targetGuid, String field,
          String accessorGuid, MetaDataTypeName access,
          GNSApplicationInterface<String> gnsApp) throws FailedDBOperationException {
    ClientSupportConfig.getLogger().log(Level.FINE,
            "@@@@@@@@@@@@@@@@ACL Check guid={0} key={1} accessor={2} access={3}",
            new Object[]{targetGuid, field, accessorGuid, access});

    // This method attempts to look up the public key as well as check for ACL access.
    String publicKey;
    if (accessorGuid.equals(targetGuid)) {
      // This handles the base case where we're accessing our own guid. 
      // Access to all of our fields is always allowed to our own guid so we just need to get
      // the public key out of the guid - possibly from the cache.
      publicKey = lookupPublicKeyLocallyWithCacheing(targetGuid, gnsApp);
      // Return an error immediately here because if we can't find the public key 
      // the guid must not be local which is a problem.
      if (publicKey == null) {
        return new AclCheckResult("", ResponseCode.BAD_GUID_ERROR);
      }
    } else {
      // Otherwise we attempt to find the public key for the accessorGuid in the ACL of the guid being
      // accesssed.
      // Note that field can be GNSProtocol.ENTIRE_RECORD.toString() here
      publicKey = lookupPublicKeyInACL(header, targetGuid, field, accessorGuid, access, gnsApp);
    }
    // Handle the one final case: the accessorGuid is a member of a group guid and
    // that group guid is in the ACL
    if (publicKey == null) {
      // First thing to do is to lookup the accessorGuid... possibly remotely.
      GuidInfo accessorGuidInfo;
      //TODO: Add a cache here
      if ((accessorGuidInfo = NSAccountAccess.lookupGuidInfoAnywhere(header, accessorGuid, gnsApp)) != null) {
        ClientSupportConfig.getLogger().log(Level.FINE,
                "================> Catchall lookup returned: {0}",
                accessorGuidInfo);
        // Check all the ACLs in the tree for this field to see if there is a group guid that
        // in there somewhere that has accessorGuid as a member
        Set<String> groups;
        if (!(groups = NSGroupAccess.lookupGroups(header, accessorGuid, gnsApp.getRequestHandler())).isEmpty()) {
          if (NSAccessSupport.hierarchicalAccessGroupCheck(access, targetGuid, field, groups, gnsApp)) {
            publicKey = accessorGuidInfo.getPublicKey();
          }
        }
      }
    }
    // If we didn't find the public key return an ACCESS_ERROR
    if (publicKey == null) {
      return new AclCheckResult("", ResponseCode.ACCESS_ERROR);
    } else {
      return new AclCheckResult(publicKey, ResponseCode.NO_ERROR);
    }
  }

  /**
   * Attempts to look up the public key for a accessorGuid using the
   * ACL of the guid for the given field.
   * Will resort to a lookup on another server in certain circumstances.
   * Like when an ACL uses the GNSProtocol.EVERYONE.toString() flag.
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
  private static String lookupPublicKeyInACL(InternalRequestHeader header, String guid, String field, String accessorGuid,
          MetaDataTypeName access, GNSApplicationInterface<String> gnsApp)
          throws FailedDBOperationException {
    String publicKey;
    // Field could also be GNSProtocol.ENTIRE_RECORD.toString() here 
    Set<String> publicKeys = NSAccessSupport.lookupPublicKeysFromAcl(access, guid, field, gnsApp.getDB());
    publicKey = SharedGuidUtils.findPublicKeyForGuid(accessorGuid, publicKeys);
    ClientSupportConfig.getLogger().log(Level.FINE,
            "================> {0} lookup for {1} returned: {2} public keys={3}",
            new Object[]{access.toString(), field, publicKey,
              publicKeys});
    // See if public keys contains GNSProtocol.EVERYONE.toString() which means we need to go old school and lookup the guid 
    // explicitly because it's not going to have an entry in the ACL
    if (publicKey == null && publicKeys.contains(GNSProtocol.EVERYONE.toString())) {
      GuidInfo accessorGuidInfo;
      if ((accessorGuidInfo = NSAccountAccess.lookupGuidInfoAnywhere(header, accessorGuid, gnsApp)) != null) {
        ClientSupportConfig.getLogger().log(Level.FINE,
                "================> {0} lookup for EVERYONE returned {1}",
                new Object[]{access.toString(), accessorGuidInfo});
        publicKey = accessorGuidInfo.getPublicKey();
      }
    }
    if (publicKey == null) {
      ClientSupportConfig.getLogger().log(Level.FINE,
              "================> Public key not found: accessor={0} guid={1} field={2} public keys={3}",
              new Object[]{accessorGuid, guid, field, publicKeys});
    }
    return publicKey;
  }

  /**
   * Look up a public key for the {@code guid} using a cache.
   *
   * @param guid
   * @param gnsApp
   * @return
   * @throws FailedDBOperationException
   */
  public static String lookupPublicKeyLocallyWithCacheing(String guid, GNSApplicationInterface<String> gnsApp)
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
