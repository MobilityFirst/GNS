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

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.xml.bind.DatatypeConverter;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.common.collect.Sets;

import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.SharedGuidUtils;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.FieldNotFoundException;
import edu.umass.cs.gnscommon.exceptions.server.RecordNotFoundException;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnsserver.gnsapp.GNSApplicationInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.MetaDataTypeName;
import edu.umass.cs.gnsserver.gnsapp.recordmap.BasicRecordMap;
import edu.umass.cs.gnsserver.main.GNSConfig.GNSC;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.SessionKeys;
import edu.umass.cs.utils.Util;

/**
 * Provides signing and ACL checks for commands.
 *
 * @author westy, arun
 */
public class NSAccessSupport {

  private static KeyFactory keyFactory;
  // arun: at least as many instances as cores for parallelism.
  private static Signature[] signatureInstances = new Signature[2 * Runtime.getRuntime().availableProcessors()];

  static {
    try {
      keyFactory = KeyFactory.getInstance(GNSProtocol.RSA_ALGORITHM.toString());
      for (int i = 0; i < signatureInstances.length; i++) {
        signatureInstances[i] = Signature.getInstance(GNSProtocol.SIGNATURE_ALGORITHM.toString());
      }
    } catch (NoSuchAlgorithmException e) {
      ClientSupportConfig.getLogger().log(Level.SEVERE, "Unable to initialize for authentication:{0}", e);
    }
  }

  /**
   * Verifies that the signature corresponds to the message using the public key.
   *
   * @param accessorPublicKey
   * @param signature
   * @param message
   * @return true if the signature verifies successfully
   * @throws InvalidKeyException
   * @throws SignatureException
   * @throws UnsupportedEncodingException
   * @throws InvalidKeySpecException
   */
  public static boolean verifySignature(String accessorPublicKey, String signature, String message) throws
          InvalidKeyException, SignatureException, UnsupportedEncodingException, InvalidKeySpecException {
    byte[] publickeyBytes = Base64.decode(accessorPublicKey);
    if (publickeyBytes == null) { // bogus public key
      ClientSupportConfig.getLogger().log(Level.FINE, "&&&&Base 64 decoding is bogus!!!");
      return false;
    }
    ClientSupportConfig.getLogger().log(Level.FINER,
            "public_key:{0}, signature:{1}, message:{2}",
            new Object[]{Util.truncate(accessorPublicKey, 16, 16),
              Util.truncate(signature, 16, 16),
              Util.truncate(message, 16, 16)});
    long t = System.nanoTime();
    boolean result = verifySignatureInternal(publickeyBytes, signature, message);
    if (Util.oneIn(100)) {
      DelayProfiler.updateDelayNano("verification", t);
    }

    ClientSupportConfig.getLogger().log(Level.FINE,
            "public_key:{0} {1} as author of message:{2}",
            new Object[]{accessorPublicKey,
              (result ? " verified " : " NOT verified "),
              message});
    return result;
  }

  private static int sigIndex = 0;

  private synchronized static Signature getSignatureInstance() {
    return signatureInstances[sigIndex++ % signatureInstances.length];
  }

  private static synchronized boolean verifySignatureInternal(byte[] publickeyBytes, String signature, String message)
          throws InvalidKeyException, SignatureException, UnsupportedEncodingException, InvalidKeySpecException {

    if (Config.getGlobalBoolean(GNSC.ENABLE_SECRET_KEY)) {
      try {
        return verifySignatureInternalSecretKey(publickeyBytes, signature, message);
      } catch (Exception e) {
        // This provided backward support for clients that don't have ENABLE_SECRET_KEY on by
        // falling through to non-secret method.
        // At the cost of potentially masking other issues that might cause exceptions
        // in the above code.
        ClientSupportConfig.getLogger().log(Level.FINE, "Falling through to non-secret key verification: {0}",
                new Object[]{e});
      }
    }

    // Non-secret method kept for backwards compatbility with older clients.
    X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(publickeyBytes);
    PublicKey publicKey = keyFactory.generatePublic(publicKeySpec);

    Signature sigInstance = getSignatureInstance();
    synchronized (sigInstance) {
      sigInstance.initVerify(publicKey);
      // iOS client uses UTF-8 - should switch to ISO-8859-1 to be consistent with
      // secret key version
      sigInstance.update(message.getBytes("UTF-8"));
      // Non secret uses ISO-8859-1, but the iOS client uses hex so 
      // we need to keep this for now.
      try {
        return sigInstance.verify(DatatypeConverter.parseHexBinary(signature));
        // This will get thrown if the signature is not a hex string.
      } catch (IllegalArgumentException e) {
        return false;
      }
      //return sigInstance.verify(ByteUtils.hexStringToByteArray(signature));
    }
  }

  private static final MessageDigest[] mds = new MessageDigest[Runtime.getRuntime().availableProcessors()];

  static {
    for (int i = 0; i < mds.length; i++) {
      try {
        mds[i] = MessageDigest.getInstance(GNSProtocol.DIGEST_ALGORITHM.toString());
      } catch (NoSuchAlgorithmException e) {
        e.printStackTrace();
        System.exit(1);
      }
    }
  }
  private static int mdIndex = 0;

  private static MessageDigest getMessageDigestInstance() {
    return mds[mdIndex++ % mds.length];
  }

  private static final Cipher[] ciphers = new Cipher[2 * Runtime.getRuntime().availableProcessors()];

  static {
    for (int i = 0; i < ciphers.length; i++) {
      try {
        ciphers[i] = Cipher.getInstance(GNSProtocol.SECRET_KEY_ALGORITHM.toString());
      } catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
        e.printStackTrace();
        System.exit(1);
      }
    }
  }

  private static int cipherIndex = 0;

  private static Cipher getCipherInstance() {
    return ciphers[cipherIndex++ % ciphers.length];
  }

  private static synchronized boolean verifySignatureInternalSecretKey(byte[] publickeyBytes, String signature, String message)
          throws InvalidKeyException, SignatureException, UnsupportedEncodingException, InvalidKeySpecException, NoSuchAlgorithmException, NoSuchPaddingException, IllegalBlockSizeException, BadPaddingException {

    PublicKey publicKey = keyFactory.generatePublic(new X509EncodedKeySpec(publickeyBytes));

    // FIXME: The reason why we use CHARSET should be more throughly documented here.
    byte[] sigBytes = signature.getBytes(GNSProtocol.CHARSET.toString());
    byte[] bytes = message.getBytes(GNSProtocol.CHARSET.toString());

    // Pull the parts out of the signature
    // See CommandUtils.signDigestOfMessage for the other side of this.
    ByteBuffer bbuf = ByteBuffer.wrap(sigBytes);
    byte[] sign = new byte[bbuf.getShort()];
    bbuf.get(sign);
    byte[] skCertEncoded = new byte[bbuf.getShort()];
    bbuf.get(skCertEncoded);
    SecretKey secretKey = SessionKeys.getSecretKeyFromCertificate(skCertEncoded, publicKey);

    MessageDigest md = getMessageDigestInstance();
    byte[] digest;
    synchronized (md) {
      digest = md.digest(bytes);
    }
    Cipher cipher = getCipherInstance();
    synchronized (cipher) {
      cipher.init(Cipher.ENCRYPT_MODE, secretKey);
      return Arrays.equals(sign, cipher.doFinal(digest));
    }
  }

  /**
   * Handles checking of fields with dot notation.
   * Checks deepest field first then backs up.
   *
   * @param accessType
   * @param guid
   * @param activeReplica
   * @param groups
   * @param field
   * @return true if the accessor has access
   * @throws FailedDBOperationException
   */
  public static boolean hierarchicalAccessGroupCheck(MetaDataTypeName accessType, String guid,
          String field, Set<String> groups,
          GNSApplicationInterface<String> activeReplica) throws FailedDBOperationException {
    ClientSupportConfig.getLogger().log(Level.FINE, "###field={0}", field);
    try {
      return checkForGroupAccess(accessType, guid, field, groups, activeReplica);
    } catch (FieldNotFoundException e) {
      ClientSupportConfig.getLogger().log(Level.FINE, "###field NOT FOUND={0}.. GOING UP", new Object[]{field});
    }
    // otherwise go up the hierarchy and check
    if (field.contains(".")) {
      return hierarchicalAccessGroupCheck(accessType, guid, field.substring(0, field.lastIndexOf(".")),
              groups, activeReplica);
    } else if (!GNSProtocol.ENTIRE_RECORD.toString().equals(field)) {
      return hierarchicalAccessGroupCheck(accessType, guid, GNSProtocol.ENTIRE_RECORD.toString(), groups, activeReplica);
    } else {
      // check all the way up and there is no access
      return false;
    }
  }

  /**
   * Check for one of the groups that accessorGuid is in being a member of allowed users.
   * Field can be dotted and at any level.
   *
   * @param accessType
   * @param guid
   * @param field
   * @param accessorGuid
   * @param activeReplica
   * @return true if access is allowed
   * @throws FailedDBOperationException
   */
  private static boolean checkForGroupAccess(MetaDataTypeName accessType,
          String guid, String field, Set<String> groups,
          GNSApplicationInterface<String> activeReplica)
          throws FieldNotFoundException, FailedDBOperationException {
    try {
      // FIXME: Tidy this mess up.
      @SuppressWarnings("unchecked")
      Set<String> allowedUsers = (Set<String>) (Set<?>) NSFieldMetaData.lookupLocally(accessType,
              guid, field, activeReplica.getDB());
      ClientSupportConfig.getLogger().log(Level.FINE, "{0} allowed users of {1} : {2}",
              new Object[]{guid, field, allowedUsers});
      return !Sets.intersection(SharedGuidUtils.convertPublicKeysToGuids(allowedUsers), groups).isEmpty();
    } catch (RecordNotFoundException e) {
      ClientSupportConfig.getLogger().log(Level.WARNING,
              "User {0} access problem for {2} field: {3}",
              new Object[]{guid, field, e});
      return false;
    }
  }

   /**
	 * return the first index of item in arr, -1 means not found the item in arr
	 * 
	 * @param arr
	 * @param item
	 * @return index of item in arr
	 */
	protected static int indexOfItemInJSONArray(JSONArray arr, String item){
		int index = -1;
		if(arr == null)
			return index;
		for (int i=0; i<arr.length(); i++){
			try {
				if(arr.getString(i).equals(item)){
					return i;
				}
			} catch (JSONException e) {
				// It doesn't matter if it is not a string
			}
		}
		return index;
	}
  
  /**
   * Returns true if the field has access setting that allow it to be read globally.
   *
   * @param access
   * @param guid
   * @param field
   * @param activeReplica
   * @return true if the field can be accessed
   * @throws FailedDBOperationException
   */
  public static boolean fieldAccessibleByEveryone(MetaDataTypeName access, String guid, String field,
          GNSApplicationInterface<String> activeReplica) throws FailedDBOperationException {
	  /**
	   * retrieve the metadata for ACL check
	   */
	 JSONObject metaData = getMataDataForACLCheck(guid, activeReplica.getDB());
	 if(metaData == null){
		 ClientSupportConfig.getLogger().log(Level.WARNING,
	              "User {0} access problem for {1}'s {2} field: no meta data exists", 
	              new Object[]{guid, field, access.toString()});
		 return false;
	 }
	 // we need another field called MD
	 final String md = "MD";
	 try {
			JSONArray aclOfField = metaData.getJSONObject(access.getPrefix())
					.getJSONObject(access.name()).getJSONObject(field).getJSONArray(md);
			if(indexOfItemInJSONArray(aclOfField, GNSProtocol.EVERYONE.toString()) >= 0){				
				return true;
			}
	 } catch (JSONException e) {
			try {
				JSONArray aclOfEntireRecord = metaData.getJSONObject(access.getPrefix())
						.getJSONObject(access.name()).getJSONObject(GNSProtocol.ENTIRE_RECORD.toString()).getJSONArray(md);
				if(indexOfItemInJSONArray(aclOfEntireRecord, GNSProtocol.EVERYONE.toString()) >= 0){				
					return true;
				}
			} catch (JSONException e1) {
				// We can not find GNSProtocol.EVERYONE, and return false
				return false;
			}
	  }
	 return false;
  }

  /**
   * @param guid
   * @param basicRecordMap
   * @return meta data
   */
  protected static JSONObject getMataDataForACLCheck(String guid, BasicRecordMap basicRecordMap) {
	  /**
	   *  For the rest of ACL and signature check, let's first retrieve
	   *  the entire record, then check ACL.
	   */	  
	  JSONObject json = null;
	  /**
	   * 1. Fetch the entire record of guid
	   */
	  try {
		  long startTime = System.nanoTime();
		  /**
		   * An entire record retrieved from DB is a JSON
		   */
		  json = basicRecordMap.lookupEntireRecord(guid);
		  DelayProfiler.updateDelayNano("lookupEntireRecordForACLCheck", startTime);
	  } catch (FailedDBOperationException | RecordNotFoundException e) {
		  /**
		   * If the entire record can not be retrieved, then there is no way for us to check
		   * ACL and signature
		   */
		  return null;
	  }
		
	  if(json == null){
		  // The entire record is null, so we did not find the record
		  return null;
	  }
	  
	  JSONObject metaData = null; 
	  try {
		  metaData = json.getJSONObject(GNSProtocol.META_DATA_FIELD.toString());
	  } catch (JSONException e) {
		  return null;
	  }
	  return metaData;
  }
  
  /**
   * Looks up the public key for a guid using the acl of a field.
   * Handles fields that uses dot notation. Recursively goes up the tree
   * towards the root (GNSProtocol.ENTIRE_RECORD.toString()) node.
   *
   * @param access
   * @param guid
   * @param fields a list of fields from the root to the field that needs to be checked
   * @param metaData 
   * @return a set of public keys
   * @throws FailedDBOperationException
   */
  public static JSONArray lookupPublicKeysFromAcl(MetaDataTypeName access, String guid, List<String> fields,
          JSONObject metaData) throws FailedDBOperationException {
    ClientSupportConfig.getLogger().log(Level.FINE, "###fields={0}", new Object[]{fields});
    try {
    	JSONObject fieldACL = metaData.getJSONObject(access.getPrefix())
				.getJSONObject(access.name());
    	for(String field:fields){
    		fieldACL = fieldACL.getJSONObject(field);
    	}
    	return fieldACL.getJSONArray(GNSProtocol.MD.toString()); 
	} catch (JSONException e) {
		ClientSupportConfig.getLogger().log(Level.FINE, "###field NOT FOUND={0}.. GOING UP", new Object[]{fields});
	}
	
    // otherwise go up the hierarchy and check
    if (fields.size() > 0) {
      return lookupPublicKeysFromAcl(access, guid, fields.subList(0, fields.size()-1), metaData);      
    } else if (fields.size() == 0) {
    // One last check at the root (GNSProtocol.ENTIRE_RECORD.toString()) field.
	    try {
			return metaData.getJSONObject(access.getPrefix())
					.getJSONObject(access.name()).getJSONObject(GNSProtocol.ENTIRE_RECORD.toString())
					.getJSONArray(GNSProtocol.MD.toString());
		} catch (JSONException e) {
			// No ACL exists for root (GNSProtocol.ENTIRE_RECORD.toString()) field
			return null;
		}
    } else {
      return null;
    }
  }

  /**
   * Extracts out the message string without the signature part.
   *
   * @param messageStringWithSignatureParts
   * @param signatureParts
   * @return the string without the signature
   */
  public static String removeSignature(String messageStringWithSignatureParts, String signatureParts) {
    ClientSupportConfig.getLogger().log(Level.FINER,
            "fullstring = {0} fullSignatureField = {1}", 
            new Object[]{messageStringWithSignatureParts, signatureParts});
    String result = messageStringWithSignatureParts.substring(0,
            messageStringWithSignatureParts.lastIndexOf(signatureParts));
    ClientSupportConfig.getLogger().log(Level.FINER, "result = {0}", result);
    return result;
  }

}
