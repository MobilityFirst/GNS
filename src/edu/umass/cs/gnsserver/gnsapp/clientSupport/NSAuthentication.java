
package edu.umass.cs.gnsserver.gnsapp.clientSupport;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.SharedGuidUtils;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.GuidInfo;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.MetaDataTypeName;
import edu.umass.cs.gnsserver.gnsapp.deprecated.GNSApplicationInterface;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;

import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.utils.Config;
import edu.umass.cs.gnscommon.GNSProtocol;


public class NSAuthentication {

  private static final Cache<String, String> PUBLIC_KEY_CACHE
          = CacheBuilder.newBuilder().concurrencyLevel(5).maximumSize(1000).build();


  public static ResponseCode signatureAndACLCheck(InternalRequestHeader header, String guid,
          String field, List<String> fields,
          String accessorGuid, String signature,
          String message, MetaDataTypeName access,
          GNSApplicationInterface<String> gnsApp)
          throws InvalidKeyException, InvalidKeySpecException, SignatureException, NoSuchAlgorithmException,
          FailedDBOperationException, UnsupportedEncodingException {
    // Do a check for unsigned reads if there is no signature
    if (signature == null) {
      if (NSAccessSupport.fieldAccessibleByEveryone(access, guid, field, gnsApp)) {
        return ResponseCode.NO_ERROR;
      } else {
        ClientSupportConfig.getLogger().log(Level.FINE, "Name {0} key={1} : ACCESS_ERROR",
                new Object[]{guid, field});
        return ResponseCode.ACCESS_ERROR;
      }
    }
    // If the signature isn't null a null accessorGuid is also an access failure because
    // only unsigned reads (handled above) can have a null accessorGuid
    if (accessorGuid == null) {
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

    // Now check signatures
    if (NSAccessSupport.verifySignature(aclResult.getPublicKey(), signature, message)) {
      return ResponseCode.NO_ERROR;
    } else {
      ClientSupportConfig.getLogger().log(Level.FINE,
              "Name {0} key={1} : SIGNATURE_ERROR", new Object[]{guid, field});
      return ResponseCode.SIGNATURE_ERROR;
    }
  }

  public static AclCheckResult aclCheck(InternalRequestHeader header, String targetGuid, String field,
          String accessorGuid, MetaDataTypeName access,
          GNSApplicationInterface<String> gnsApp) throws FailedDBOperationException {
    if (Config.getGlobalBoolean(GNSConfig.GNSC.USE_OLD_ACL_MODEL)) {
      return oldAclCheck(header, targetGuid, field, accessorGuid, access, gnsApp);
    } else {
      return newAclCheck(header, targetGuid, field, accessorGuid, access, gnsApp);
    }
  }

  private static AclCheckResult newAclCheck(InternalRequestHeader header, String targetGuid, String field,
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
      publicKey = lookupPublicKeyFromGuidLocallyWithCacheing(targetGuid, gnsApp);
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

  @Deprecated
  private static AclCheckResult oldAclCheck(InternalRequestHeader header, String targetGuid, String field,
          String accessorGuid, MetaDataTypeName access,
          GNSApplicationInterface<String> gnsApp) throws FailedDBOperationException {
    ClientSupportConfig.getLogger().log(Level.FINE,
            "@@@@@@@@@@@@@@@@ACL Check guid={0} key={1} accessor={2} access={3}",
            new Object[]{targetGuid, field, accessorGuid, access});

    // This method attempts to look up the public key as well as check for ACL access.
    String publicKey;
    boolean aclCheckPassed = false;
    if (accessorGuid.equals(targetGuid)) {
      // This handles the base case where we're accessing our own guid. 
      // Access to all of our fields is always allowed to our own guid so we just need to get
      // the public key out of the guid - possibly from the cache.
      publicKey = lookupPublicKeyFromGuidLocallyWithCacheing(targetGuid, gnsApp);
      // Return an error immediately here because if we can't find the public key 
      // the guid must not be local which is a problem.
      if (publicKey == null) {
        return new AclCheckResult("", ResponseCode.BAD_GUID_ERROR);
      }
      aclCheckPassed = true;
    } else {
      // Otherwise we attempt to find the public key for the accessorGuid in the ACL of the guid being
      // accesssed.
      // field can be GNSProtocol.ENTIRE_RECORD.toString() here
      publicKey = lookupPublicKeyInACL(header, targetGuid, field, accessorGuid, access, gnsApp);
      if (publicKey != null) {
        // If we found the public key in the lookupPublicKey call then our access control list
        // check is done because the public key of the accessorGuid is in the given acl of targetGuid.
        aclCheckPassed = true;

      } else {
        // One final case we need to handle is when the the accessorGuid is actually in a group guid
        // and the group guid is in the acl in which case we need to explicitly lookup the 
        // publickey in possibly another server. 
        GuidInfo accessorGuidInfo;
        if ((accessorGuidInfo = NSAccountAccess.lookupGuidInfoAnywhere(header, accessorGuid, gnsApp)) != null) {
          ClientSupportConfig.getLogger().log(Level.FINE, "================> Catchall lookup returned: {0}",
                  accessorGuidInfo);
          publicKey = accessorGuidInfo.getPublicKey();
        }
      }
    }
    if (publicKey == null) {
      // If we haven't found the publicKey of the accessorGuid yet it's not allowed access.
      return new AclCheckResult("", ResponseCode.BAD_ACCESSOR_ERROR);
    } else if (!aclCheckPassed) {
      // Otherwise, we need to find out if this accessorGuid is in a group guid that
      // is in the acl of the field.
      // Note that this a full-blown acl check that checks all the cases, but currenly it
      // is only used for checking the case where the accessorGuid is in a group guid
      // that is in the acl.
      aclCheckPassed = NSAccessSupport.verifyAccess(header, access, targetGuid, field, accessorGuid, gnsApp);
    }
    return new AclCheckResult(publicKey, ResponseCode.NO_ERROR);
  }


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
    // In the new ACL model this is done differently in the above lookupPublicKeysFromAcl call.
    if (Config.getGlobalBoolean(GNSConfig.GNSC.USE_OLD_ACL_MODEL) && publicKey == null) {
      // NOT DONE IN THE NEW ACL MODEL.
      // Also catch all the keys that are stored in the +ALL+ record.
      // This handles the case where the guid attempting access isn't stored in a single field ACL
      // but is stored in the GNSProtocol.ENTIRE_RECORD.toString() (+ALL+) ACL
      publicKeys.addAll(NSAccessSupport.lookupPublicKeysFromAcl(access, guid, GNSProtocol.ENTIRE_RECORD.toString(), gnsApp.getDB()));
      publicKey = SharedGuidUtils.findPublicKeyForGuid(accessorGuid, publicKeys);
      ClientSupportConfig.getLogger().log(Level.FINE,
              "================> {0} lookup with +ALL+ returned: {1} public keys={2}",
              new Object[]{access.toString(), publicKey, publicKeys});
    }
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
