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
 *  Initial developer(s): Westy, Emmanuel Cecchet
 *
 */
package edu.umass.cs.gnsclient.client.http;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientInterface;
import edu.umass.cs.gnscommon.GnsProtocol;
import edu.umass.cs.gnsclient.client.GuidEntry;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import edu.umass.cs.gnsclient.client.http.android.DownloadTask;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnscommon.utils.ByteUtils;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.client.util.Password;
import edu.umass.cs.gnscommon.utils.URIEncoderDecoder;
import edu.umass.cs.gnsclient.exceptions.EncryptionException;
import edu.umass.cs.gnsclient.exceptions.GnsACLException;
import edu.umass.cs.gnsclient.exceptions.GnsDuplicateNameException;
import edu.umass.cs.gnsclient.exceptions.GnsException;
import edu.umass.cs.gnsclient.exceptions.GnsInvalidFieldException;
import edu.umass.cs.gnsclient.exceptions.GnsInvalidGroupException;
import edu.umass.cs.gnsclient.exceptions.GnsInvalidGuidException;
import edu.umass.cs.gnsclient.exceptions.GnsInvalidUserException;
import edu.umass.cs.gnsclient.exceptions.GnsVerificationException;

/**
 * This class defines a UniversalHttpClient to communicate with a GNS instance
 * over HTTP. This class works on both Android and Desktop platforms.
 * This class contains a subset of all available server operations.
 * For a more complete set see UniversalGnsClientExtended.
 *
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class UniversalHttpClient implements GNSClientInterface {

  /**
   * Check whether we are on an Android platform or not
   */
  public static final boolean isAndroid = System.getProperty("java.vm.name").equalsIgnoreCase("Dalvik");

  private final static String QUERYPREFIX = "?";
  private final static String VALSEP = "=";
  private final static String KEYSEP = "&";
  /**
   * The host address used when attempting to connect to the HTTP service.
   * Initialized in the default constructor.
   */
  protected String host;
  /**
   * The port number used when attempting to connect to the HTTP service.
   * Initialized in the default constructor.
   */
  protected int port;
  /**
   * The timeout used when attempting to connect to the HTTP service.
   */
  protected int readTimeout = 10000;
  /**
   * The number of retries on timeout attempted when connecting to the HTTP
   * service.
   */
  protected int readRetries = 1;
  /**
   * Static reference to a GNS object (use setGnrs() to define)
   */
  public static UniversalHttpClient gns;

  /**
   * Creates a new <code>AbstractGnrsClient</code> object
   *
   * @param host Hostname of the GNS instance
   * @param port Port number of the GNS instance
   */
  public UniversalHttpClient(String host, int port) {
    this.host = host;
    this.port = port;
  }

  /**
   * Returns the gns value.
   *
   * @return Returns the gns.
   */
  public static UniversalHttpClient getGns() {
    return gns;
  }

  /**
   * Sets the gns value.
   *
   * @param gns The gns to set.
   */
  public static void setGns(UniversalHttpClient gns) {
    UniversalHttpClient.gns = gns;
  }

  /**
   * Returns the host value.
   *
   * @return Returns the host.
   */
  public String getGnsRemoteHost() {
    return host;
  }

  /**
   * Returns the port value.
   *
   * @return Returns the port.
   */
  public int getGnsRemotePort() {
    return port;
  }

  /**
   * Returns the timeout value (milliseconds) used when sending commands to the
   * server.
   *
   * @return value in milliseconds
   */
  public int getReadTimeout() {
    return readTimeout;
  }

  /**
   * Sets the timeout value (milliseconds) used when sending commands to the
   * server.
   *
   * @param readTimeout in milliseconds
   */
  public void setReadTimeout(int readTimeout) {
    this.readTimeout = readTimeout;
  }

  /**
   * Returns the number of potential retries used when sending commands to the
   * server.
   *
   * @return the number of retries
   */
  public int getReadRetries() {
    return readRetries;
  }

  /**
   * Sets the number of potential retries used when sending commands to the
   * server.
   *
   * @param readRetries
   */
  public void setReadRetries(int readRetries) {
    this.readRetries = readRetries;
  }

  /**
   * Return the help message of the GNS. Can be used to check connectivity
   *
   * @return
   * @throws IOException
   */
  public String getHelp() throws IOException {
    return sendGetCommand("help");
  }

  /**
   * Obtains the guid of the alias from the GNS server.
   *
   * @param alias
   * @return guid
   * @throws IOException
   * @throws UnsupportedEncodingException
   * @throws GnsException
   * @throws Exception
   */
  public String lookupGuid(String alias) throws UnsupportedEncodingException, IOException, GnsException {
    String command = createQuery(GnsProtocol.LOOKUP_GUID, GnsProtocol.NAME, URIEncoderDecoder.quoteIllegal(alias, ""));
    String response = sendGetCommand(command);

    return checkResponse(command, response);
  }

  /**
   * If this is a sub guid returns the account guid it was created under.
   *
   * @param guid
   * @return
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws GnsException
   */
  public String lookupPrimaryGuid(String guid) throws UnsupportedEncodingException, IOException, GnsException {
    String command = createQuery(GnsProtocol.LOOKUP_PRIMARY_GUID, GnsProtocol.GUID, guid);
    String response = sendGetCommand(command);

    return checkResponse(command, response);
  }

  /**
   * Returns a JSON object containing all of the guid information.
   *
   * @param guid
   * @return
   * @throws IOException
   * @throws GnsException
   */
  public JSONObject lookupGuidRecord(String guid) throws IOException, GnsException {
    String command = createQuery(GnsProtocol.LOOKUP_GUID_RECORD, GnsProtocol.GUID, guid);
    String response = sendGetCommand(command);
    checkResponse(command, response);
    try {
      return new JSONObject(response);
    } catch (JSONException e) {
      throw new GnsException("Failed to parse LOOKUP_GUID_RECORD response", e);
    }
  }

  /**
   * Returns a JSON object containing all of the account information for an
   * account guid.
   *
   * @param gaccountGuid
   * @return
   * @throws IOException
   * @throws GnsException
   */
  public JSONObject lookupAccountRecord(String gaccountGuid) throws IOException, GnsException {
    String command = createQuery(GnsProtocol.LOOKUP_ACCOUNT_RECORD, GnsProtocol.GUID, gaccountGuid);
    String response = sendGetCommand(command);
    checkResponse(command, response);
    try {
      return new JSONObject(response);
    } catch (JSONException e) {
      throw new GnsException("Failed to parse LOOKUP_ACCOUNT_RECORD response", e);
    }
  }

  /**
   * Get the public key for a given alias
   *
   * @param alias
   * @return the public key registered for the alias
   * @throws GnsInvalidGuidException
   * @throws GnsException
   * @throws IOException
   */
  public PublicKey publicKeyLookupFromAlias(String alias) throws GnsInvalidGuidException, GnsException, IOException {

    String guid = lookupGuid(alias);
    return publicKeyLookupFromGuid(guid);
  }

  /**
   * Get the public key for a given GUID
   *
   * @param guid
   * @return
   * @throws GnsInvalidGuidException
   * @throws GnsException
   * @throws IOException
   */
  public PublicKey publicKeyLookupFromGuid(String guid) throws GnsInvalidGuidException, GnsException, IOException {
    JSONObject guidInfo = lookupGuidRecord(guid);
    try {
      String key = guidInfo.getString(GnsProtocol.GUID_RECORD_PUBLICKEY);
      byte[] encodedPublicKey = Base64.decode(key);
      KeyFactory keyFactory = KeyFactory.getInstance(GnsProtocol.RSA_ALGORITHM);
      X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(encodedPublicKey);
      return keyFactory.generatePublic(publicKeySpec);
    } catch (JSONException e) {
      throw new GnsException("Failed to parse LOOKUP_USER response", e);
    } catch (NoSuchAlgorithmException e) {
      throw new EncryptionException("Public key encryption failed", e);
    } catch (InvalidKeySpecException e) {
      throw new EncryptionException("Public key encryption failed", e);
    }

  }

  /**
   * Register a new account guid with the corresponding alias on the GNS server.
   * This generates a new guid and a public / private key pair. Returns a
   * GuidEntry for the new account which contains all of this information.
   *
   * @param alias - a human readable alias to the guid - usually an email
   * address
   * @return
   * @throws Exception
   */
  public GuidEntry accountGuidCreate(String alias, String password) throws Exception {

    KeyPair keyPair = KeyPairGenerator.getInstance(GnsProtocol.RSA_ALGORITHM).generateKeyPair();
    String guid = accountGuidCreate(alias, keyPair.getPublic(), password);

    KeyPairUtils.saveKeyPair(host + ":" + port, alias, guid, keyPair);

    GuidEntry entry = new GuidEntry(alias, guid, keyPair.getPublic(), keyPair.getPrivate());

    return entry;
  }

  /**
   * Verify an account by sending the verification code back to the server.
   *
   * @param guid the account GUID to verify
   * @param code the verification code
   * @return ?
   * @throws Exception
   */
  public String accountGuidVerify(GuidEntry guid, String code) throws Exception {
    String command = createAndSignQuery(guid, GnsProtocol.VERIFY_ACCOUNT, GnsProtocol.GUID, guid.getGuid(),
            GnsProtocol.CODE, code);
    String response = sendGetCommand(command);
    return checkResponse(command, response);
  }

  /**
   * Deletes the account given by name
   *
   * @param guid GuidEntry
   * @throws Exception
   */
  public void accountGuidRemove(GuidEntry guid) throws Exception {
    String command = createAndSignQuery(guid, GnsProtocol.REMOVE_ACCOUNT, GnsProtocol.GUID, guid.getGuid(),
            GnsProtocol.NAME, guid.getEntityName());
    String response = sendGetCommand(command);
    checkResponse(command, response);
  }

  /**
   * Creates an new GUID associated with an account on the GNS server.
   *
   * @param accountGuid
   * @param alias the alias
   * @return the newly created GUID entry
   * @throws Exception
   */
  public GuidEntry guidCreate(GuidEntry accountGuid, String alias) throws Exception {

    KeyPair keyPair = KeyPairGenerator.getInstance(GnsProtocol.RSA_ALGORITHM).generateKeyPair();
    String newGuid = guidCreate(accountGuid, alias, keyPair.getPublic());

    KeyPairUtils.saveKeyPair(host + ":" + port, alias, newGuid, keyPair);

    GuidEntry entry = new GuidEntry(alias, newGuid, keyPair.getPublic(), keyPair.getPrivate());

    return entry;
  }

  /**
   * Removes a guid (not for account Guids - use removeAccountGuid for them).
   *
   * @param guid the guid to remove
   * @throws Exception
   */
  public void guidRemove(GuidEntry guid) throws Exception {
    String command = createAndSignQuery(guid, GnsProtocol.REMOVE_GUID, GnsProtocol.GUID, guid.getGuid());
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Removes a guid given the guid and the associated account guid.
   *
   * @param accountGuid
   * @param guidToRemove
   * @throws Exception
   */
  public void guidRemove(GuidEntry accountGuid, String guidToRemove) throws Exception {
    String command = createAndSignQuery(accountGuid, GnsProtocol.REMOVE_GUID,
            GnsProtocol.GUID, guidToRemove, GnsProtocol.ACCOUNT_GUID, accountGuid.getGuid());
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Return a list of the groups that the guid is a member of. Signs the query
   * using the private key of the user associated with the guid.
   *
   * @param groupGuid the guid of the group to lookup
   * @param reader the guid of the entity doing the lookup
   * @return the list of groups as a JSONArray
   * @throws IOException if a communication error occurs
   * @throws GnsException if a protocol error occurs or the list cannot be
   * parsed
   * @throws GnsInvalidGuidException if the group guid is invalid
   */
  public JSONArray guidGetGroups(String groupGuid, GuidEntry reader) throws IOException, GnsException,
          GnsInvalidGuidException {
    String command = createAndSignQuery(reader, GnsProtocol.GET_GROUPS, GnsProtocol.GUID, groupGuid,
            GnsProtocol.READER, reader.getGuid());
    String response = sendGetCommand(command);

    try {
      return new JSONArray(checkResponse(command, response));
    } catch (JSONException e) {
      throw new GnsException("Invalid member list", e);
    }
  }

  /**
   * Add a guid to a group guid. Any guid can be a group guid. Signs the query
   * using the private key of the user associated with the writer.
   *
   * @param groupGuid guid of the group
   * @param guidToAdd guid to add to the group
   * @param writer the guid doing the add
   * @throws IOException
   * @throws GnsInvalidGuidException if the group guid does not exist
   * @throws GnsException
   */
  public void groupAddGuid(String groupGuid, String guidToAdd, GuidEntry writer) throws IOException,
          GnsInvalidGuidException, GnsException {
    String command = createAndSignQuery(writer, GnsProtocol.ADD_TO_GROUP, GnsProtocol.GUID, groupGuid,
            GnsProtocol.MEMBER, guidToAdd, GnsProtocol.WRITER, writer.getGuid());
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Add multiple members to a group
   *
   * @param groupGuid guid of the group
   * @param members guids of members to add to the group
   * @param writer the guid doing the add
   * @throws IOException
   * @throws GnsInvalidGuidException
   * @throws GnsException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   */
  public void groupAddGuids(String groupGuid, JSONArray members, GuidEntry writer) throws IOException,
          GnsInvalidGuidException, GnsException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    String command = createAndSignQuery(writer, GnsProtocol.ADD_TO_GROUP, GnsProtocol.GUID, groupGuid,
            GnsProtocol.MEMBERS, members.toString(), GnsProtocol.WRITER, writer.getGuid());
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Removes a guid from a group guid. Any guid can be a group guid. Signs the
   * query using the private key of the user associated with the writer.
   *
   * @param guid guid of the group
   * @param guidToRemove guid to remove from the group
   * @param writer the guid of the entity doing the remove
   * @throws IOException
   * @throws GnsInvalidGuidException if the group guid does not exist
   * @throws GnsException
   */
  public void groupRemoveGuid(String guid, String guidToRemove, GuidEntry writer) throws IOException,
          GnsInvalidGuidException, GnsException {
    String command = createAndSignQuery(writer, GnsProtocol.REMOVE_FROM_GROUP, GnsProtocol.GUID, guid,
            GnsProtocol.MEMBER, guidToRemove, GnsProtocol.WRITER, writer.getGuid());

    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Remove a list of members from a group
   *
   * @param guid guid of the group
   * @param members guids to remove from the group
   * @param writer the guid of the entity doing the remove
   * @throws IOException
   * @throws GnsInvalidGuidException
   * @throws GnsException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   */
  public void groupRemoveGuids(String guid, JSONArray members, GuidEntry writer) throws IOException,
          GnsInvalidGuidException, GnsException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    String command = createAndSignQuery(writer, GnsProtocol.REMOVE_FROM_GROUP, GnsProtocol.GUID, guid,
            GnsProtocol.MEMBERS, members.toString(), GnsProtocol.WRITER, writer.getGuid());

    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Return the list of guids that are member of the group. Signs the query
   * using the private key of the user associated with the guid.
   *
   * @param groupGuid the guid of the group to lookup
   * @param reader the guid of the entity doing the lookup
   * @return the list of guids as a JSONArray
   * @throws IOException if a communication error occurs
   * @throws GnsException if a protocol error occurs or the list cannot be
   * parsed
   * @throws GnsInvalidGuidException if the group guid is invalid
   */
  public JSONArray groupGetMembers(String groupGuid, GuidEntry reader) throws IOException, GnsException,
          GnsInvalidGuidException {
    String command = createAndSignQuery(reader, GnsProtocol.GET_GROUP_MEMBERS, GnsProtocol.GUID, groupGuid,
            GnsProtocol.READER, reader.getGuid());
    String response = sendGetCommand(command);

    try {
      return new JSONArray(checkResponse(command, response));
    } catch (JSONException e) {
      throw new GnsException("Invalid member list", e);
    }
  }

  /**
   * Authorize guidToAuthorize to add/remove members from the group groupGuid.
   * If guidToAuthorize is null, everyone is authorized to add/remove members to
   * the group. Note that this method can only be called by the group owner
   * (private key required) Signs the query using the private key of the group
   * owner.
   *
   * @param groupGuid the group GUID entry
   * @param guidToAuthorize the guid to authorize to manipulate group membership
   * or null for anyone
   * @throws Exception
   */
  public void groupAddMembershipUpdatePermission(GuidEntry groupGuid, String guidToAuthorize) throws Exception {
    aclAdd(GnsProtocol.AccessType.WRITE_WHITELIST, groupGuid, GnsProtocol.GROUP_ACL, guidToAuthorize);
  }

  /**
   * Unauthorize guidToUnauthorize to add/remove members from the group
   * groupGuid. If guidToUnauthorize is null, everyone is forbidden to
   * add/remove members to the group. Note that this method can only be called
   * by the group owner (private key required). Signs the query using the
   * private key of the group owner.
   *
   * @param groupGuid the group GUID entry
   * @param guidToUnauthorize the guid to authorize to manipulate group
   * membership or null for anyone
   * @throws Exception
   */
  public void groupRemoveMembershipUpdatePermission(GuidEntry groupGuid, String guidToUnauthorize) throws Exception {
    aclRemove(GnsProtocol.AccessType.WRITE_WHITELIST, groupGuid, GnsProtocol.GROUP_ACL, guidToUnauthorize);
  }

  /**
   * Authorize guidToAuthorize to get the membership list from the group
   * groupGuid. If guidToAuthorize is null, everyone is authorized to list
   * members of the group. Note that this method can only be called by the group
   * owner (private key required). Signs the query using the private key of the
   * group owner.
   *
   * @param groupGuid the group GUID entry
   * @param guidToAuthorize the guid to authorize to manipulate group membership
   * or null for anyone
   * @throws Exception
   */
  public void groupAddMembershipReadPermission(GuidEntry groupGuid, String guidToAuthorize) throws Exception {
    aclAdd(GnsProtocol.AccessType.READ_WHITELIST, groupGuid, GnsProtocol.GROUP_ACL, guidToAuthorize);
  }

  /**
   * Unauthorize guidToUnauthorize to get the membership list from the group
   * groupGuid. If guidToUnauthorize is null, everyone is forbidden from
   * querying the group membership. Note that this method can only be called by
   * the group owner (private key required). Signs the query using the private
   * key of the group owner.
   *
   * @param groupGuid the group GUID entry
   * @param guidToUnauthorize the guid to authorize to manipulate group
   * membership or null for anyone
   * @throws Exception
   */
  public void groupRemoveMembershipReadPermission(GuidEntry groupGuid, String guidToUnauthorize) throws Exception {
    aclRemove(GnsProtocol.AccessType.READ_WHITELIST, groupGuid, GnsProtocol.GROUP_ACL, guidToUnauthorize);
  }

  /**
   * Adds to an access control list of the given field. The accesser can be a
   * guid of a user or a group guid or null which means anyone can access the
   * field. The field can be also be +ALL+ which means all fields can be read by
   * the reader. Signs the query using the private key of the user associated
   * with the guid.
   *
   * @param accessType a value from GnrsProtocol.AccessType
   * @param targetGuid guid of the field to be modified
   * @param field field name
   * @param accesserGuid guid to add to the ACL
   * @throws Exception
   * @throws GnsException if the query is not accepted by the server.
   */
  public void aclAdd(GnsProtocol.AccessType accessType, GuidEntry targetGuid, String field, String accesserGuid)
          throws Exception {
    aclAdd(accessType.name(), targetGuid, field, accesserGuid);
  }

  /**
   * Removes a GUID from an access control list of the given user's field on the
   * GNS server to include the guid specified in the accesser param. The
   * accesser can be a guid of a user or a group guid or null which means anyone
   * can access the field. The field can be also be +ALL+ which means all fields
   * can be read by the reader. Signs the query using the private key of the
   * user associated with the guid.
   *
   * @param accessType
   * @param guid
   * @param field
   * @param accesserGuid
   * @throws Exception
   * @throws GnsException if the query is not accepted by the server.
   */
  public void aclRemove(GnsProtocol.AccessType accessType, GuidEntry guid, String field, String accesserGuid)
          throws Exception {
    aclRemove(accessType.name(), guid, field, accesserGuid);
  }

  /**
   * Get an access control list of the given user's field on the GNS server to
   * include the guid specified in the accesser param. The accesser can be a
   * guid of a user or a group guid or null which means anyone can access the
   * field. The field can be also be +ALL+ which means all fields can be read by
   * the reader. Signs the query using the private key of the user associated
   * with the guid.
   *
   * @param accessType
   * @param guid
   * @param field
   * @param accesserGuid
   * @return list of GUIDs for that ACL
   * @throws Exception
   * @throws GnsException if the query is not accepted by the server.
   */
  public JSONArray aclGet(GnsProtocol.AccessType accessType, GuidEntry guid, String field, String accesserGuid)
          throws Exception {
    return aclGet(accessType.name(), guid, field, accesserGuid);
  }

  /**
   * Creates a new field with value being the list. Allows a a different guid as
   * the writer. If the writer is different use addToACL first to allow other
   * the guid to write this field.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws IOException
   * @throws GnsException
   */
  public void fieldCreate(String targetGuid, String field, JSONArray value, GuidEntry writer) throws IOException,
          GnsException {
    String command = createAndSignQuery(writer, GnsProtocol.CREATE_LIST, GnsProtocol.GUID, targetGuid,
            GnsProtocol.FIELD, field, GnsProtocol.VALUE, value.toString(), GnsProtocol.WRITER, writer.getGuid());
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Removes a field. Allows a a different guid as the writer.
   *
   * @param targetGuid
   * @param field
   * @param writer
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   * @throws GnsException
   */
  public void fieldRemove(String targetGuid, String field, GuidEntry writer) throws IOException, InvalidKeyException,
          NoSuchAlgorithmException, SignatureException, GnsException {
    String command = createAndSignQuery(writer, GnsProtocol.REMOVE_FIELD, GnsProtocol.GUID, targetGuid,
            GnsProtocol.FIELD, field, GnsProtocol.WRITER, writer.getGuid());
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Appends the values of the field onto list of values or creates a new field
   * with values in the list if it does not exist.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws IOException
   * @throws GnsException
   */
  public void fieldAppendOrCreate(String targetGuid, String field, JSONArray value, GuidEntry writer)
          throws IOException, GnsException {
    String command = createAndSignQuery(writer, GnsProtocol.APPEND_OR_CREATE_LIST, GnsProtocol.GUID, targetGuid,
            GnsProtocol.FIELD, field, GnsProtocol.VALUE, value.toString(), GnsProtocol.WRITER, writer.getGuid());
    String response = sendGetCommand(command);
    checkResponse(command, response);
  }

  /**
   * Replaces the values of the field with the list of values or creates a new
   * field with values in the list if it does not exist.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws IOException
   * @throws GnsException
   */
  public void fieldReplaceOrCreate(String targetGuid, String field, JSONArray value, GuidEntry writer)
          throws IOException, GnsException {
    String command = createAndSignQuery(writer, GnsProtocol.REPLACE_OR_CREATE_LIST, GnsProtocol.GUID, targetGuid,
            GnsProtocol.FIELD, field, GnsProtocol.VALUE, value.toString(), GnsProtocol.WRITER, writer.getGuid());
    String response = sendGetCommand(command);
    checkResponse(command, response);
  }

  /**
   * Appends a list of values onto a field.
   *
   * @param targetGuid GUID where the field is stored
   * @param field field name
   * @param value list of values
   * @param writer GUID entry of the writer
   * @throws IOException
   * @throws GnsException
   */
  public void fieldAppend(String targetGuid, String field, JSONArray value, GuidEntry writer) throws IOException,
          GnsException {
    String command = createAndSignQuery(writer, GnsProtocol.APPEND_LIST_WITH_DUPLICATION, GnsProtocol.GUID, targetGuid,
            GnsProtocol.FIELD, field, GnsProtocol.VALUE, value.toString(), GnsProtocol.WRITER, writer.getGuid());
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Replaces all the values of field with the list of values.
   *
   * @param targetGuid GUID where the field is stored
   * @param field field name
   * @param value list of values
   * @param writer GUID entry of the writer
   * @throws IOException
   * @throws GnsException
   */
  public void fieldReplace(String targetGuid, String field, JSONArray value, GuidEntry writer) throws IOException,
          GnsException {
    String command = createAndSignQuery(writer, GnsProtocol.REPLACE_LIST, GnsProtocol.GUID, targetGuid,
            GnsProtocol.FIELD, field, GnsProtocol.VALUE, value.toString(), GnsProtocol.WRITER, writer.getGuid());
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Removes all the values in the list from the field.
   *
   * @param targetGuid GUID where the field is stored
   * @param field field name
   * @param value list of values
   * @param writer GUID entry of the writer
   * @throws IOException
   * @throws GnsException
   */
  public void fieldClear(String targetGuid, String field, JSONArray value, GuidEntry writer) throws IOException,
          GnsException {
    String command = createAndSignQuery(writer, GnsProtocol.REMOVE_LIST, GnsProtocol.GUID, targetGuid,
            GnsProtocol.FIELD, field, GnsProtocol.VALUE, value.toString(), GnsProtocol.WRITER, writer.getGuid());
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Removes all values from the field.
   *
   * @param targetGuid GUID where the field is stored
   * @param field field name
   * @param writer GUID entry of the writer
   * @throws IOException
   * @throws GnsException
   */
  public void fieldClear(String targetGuid, String field, GuidEntry writer) throws IOException, GnsException {
    String command = createAndSignQuery(writer, GnsProtocol.CLEAR, GnsProtocol.GUID, targetGuid, GnsProtocol.FIELD,
            field, GnsProtocol.WRITER, writer.getGuid());
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Reads all the values value for a key from the GNS server for the given
   * guid. The guid of the user attempting access is also needed. Signs the
   * query using the private key of the user associated with the reader guid
   * (unsigned if reader is null).
   *
   * @param guid
   * @param field
   * @param reader if null the field must be readable for all
   * @return a JSONArray containing the values in the field
   * @throws Exception
   */
  public JSONArray fieldRead(String guid, String field, GuidEntry reader) throws Exception {
    String command;
    if (reader == null) {
      command = createQuery(GnsProtocol.READ_ARRAY, GnsProtocol.GUID, guid, GnsProtocol.FIELD, field);
    } else {
      command = createAndSignQuery(reader, GnsProtocol.READ_ARRAY, GnsProtocol.GUID, guid, GnsProtocol.FIELD, field,
              GnsProtocol.READER, reader.getGuid());
    }

    String response = sendGetCommand(command);

    return new JSONArray(checkResponse(command, response));
  }

  /**
   * Sets the nth value (zero-based) indicated by index in the list contained in
   * field to newValue. Index must be less than the current size of the list.
   *
   * @param targetGuid
   * @param field
   * @param newValue
   * @param index
   * @param writer
   * @throws IOException
   * @throws GnsException
   */
  public void fieldSetElement(String targetGuid, String field, String newValue, int index, GuidEntry writer)
          throws IOException, GnsException {
    String command = createAndSignQuery(writer, GnsProtocol.SET, GnsProtocol.GUID, targetGuid, GnsProtocol.FIELD,
            field, GnsProtocol.VALUE, newValue, GnsProtocol.N, Integer.toString(index), GnsProtocol.WRITER,
            writer.getGuid());
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Sets a field to be null. That is when read field is called a null will be returned.
   *
   * @param targetGuid
   * @param field
   * @param writer
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   * @throws GnsException
   */
  public void fieldSetNull(String targetGuid, String field, GuidEntry writer) throws IOException,
          InvalidKeyException, NoSuchAlgorithmException, SignatureException,
          GnsException {
    String command = createAndSignQuery(writer, GnsProtocol.SET_FIELD_NULL,
            GnsProtocol.GUID, targetGuid, GnsProtocol.FIELD, field,
            GnsProtocol.WRITER, writer.getGuid());
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  //
  // SELECT
  //
  /**
   * Returns all GUIDs that have a field that contains the given value as a
   * JSONArray containing guids. Also note that the GNS currently does not enforce any
   * ACL checking for this operation - everything is accessible and no
   * signatures are required. This might change.
   *
   * @param field
   * @param value
   * @return a JSONArray containing all the matched records as JSONObjects
   * @throws Exception
   */
  public JSONArray select(String field, String value) throws Exception {
    String command = createQuery(GnsProtocol.SELECT, GnsProtocol.FIELD, field, GnsProtocol.VALUE, value);
    String response = sendGetCommand(command);

    return new JSONArray(checkResponse(command, response));
  }

  /**
   * If field is a GeoSpatial field returns all guids that have fields that are within value
   * which is a bounding box specified as a nested
   * JSONArrays of paired tuples: [[LONG_UL, LAT_UL],[LONG_BR, LAT_BR]]
   *
   * @param field
   * @param value - [[LONG_UL, LAT_UL],[LONG_BR, LAT_BR]]
   * @return a JSONArray containing the guids of all the matched records
   * @throws Exception
   */
  public JSONArray selectWithin(String field, JSONArray value) throws Exception {
    String command = createQuery(GnsProtocol.SELECT, GnsProtocol.FIELD, field, GnsProtocol.WITHIN, value.toString());
    String response = sendGetCommand(command);

    return new JSONArray(checkResponse(command, response));
  }

  /**
   * If field is a GeoSpatial field returns all guids that have fields that are near value
   * which is a point specified as a two element
   * JSONArray: [LONG, LAT]. Max Distance is in meters.
   *
   * @param field
   * @param value - [LONG, LAT]
   * @param maxDistance - distance in meters
   * @return a JSONArray containing the guids of all the matched records
   * @throws Exception
   */
  public JSONArray selectNear(String field, JSONArray value, Double maxDistance) throws Exception {
    String command = createQuery(GnsProtocol.SELECT, GnsProtocol.FIELD, field, GnsProtocol.NEAR, value.toString(),
            GnsProtocol.MAX_DISTANCE, Double.toString(maxDistance));
    // System.out.println(command);
    String response = sendGetCommand(command);

    return new JSONArray(checkResponse(command, response));
  }

  /**
   * Selects all records that match query.
   *
   * @param query
   * @return
   * @throws Exception
   */
  public JSONArray selectQuery(String query) throws Exception {
    String command = createQuery(GnsProtocol.SELECT, GnsProtocol.QUERY, query);
    // System.out.println(command);
    String response = sendGetCommand(command);

    return new JSONArray(checkResponse(command, response));
  }

  /**
   * Set up a context aware group guid using a query.
   *
   * @param guid
   * @param query
   * @return
   * @throws Exception
   */
  public JSONArray selectSetupGroupQuery(String guid, String query) throws Exception {
    String command = createQuery(GnsProtocol.SELECT_GROUP, GnsProtocol.GUID, guid,
            GnsProtocol.QUERY, query);
    // System.out.println(command);
    String response = sendGetCommand(command);

    return new JSONArray(checkResponse(command, response));
  }

  /**
   * Look up the value of a context aware group guid using a query.
   *
   * @param guid
   * @return
   * @throws Exception
   */
  public JSONArray selectLookupGroupQuery(String guid) throws Exception {
    String command = createQuery(GnsProtocol.SELECT_GROUP, GnsProtocol.GUID, guid);
    // System.out.println(command);
    String response = sendGetCommand(command);

    return new JSONArray(checkResponse(command, response));
  }

  /**
   * Update the location field for the given GUID
   *
   * @param longitude the GUID longitude
   * @param latitude the GUID latitude
   * @param guid the GUID to update
   * @throws Exception if a GNS error occurs
   */
  public void setLocation(double longitude, double latitude, GuidEntry guid) throws Exception {
    JSONArray array = new JSONArray(Arrays.asList(longitude, latitude));
    fieldReplaceOrCreate(guid.getGuid(), GnsProtocol.LOCATION_FIELD_NAME, array, guid);
  }

  /**
   * Get the location of the target GUID as a JSONArray: [LONG, LAT]
   *
   * @param readerGuid the GUID issuing the request
   * @param targetGuid the GUID that we want to know the location
   * @return a JSONArray: [LONGITUDE, LATITUDE]
   * @throws Exception if a GNS error occurs
   */
  public JSONArray getLocation(GuidEntry readerGuid, String targetGuid) throws Exception {
    return fieldRead(targetGuid, GnsProtocol.LOCATION_FIELD_NAME, readerGuid);
  }

  /**
   * Creates an alias entity name for the given guid. The alias can be used just
   * like the original entity name.
   *
   * @param guid
   * @param name - the alias
   * @throws Exception
   */
  public void addAlias(GuidEntry guid, String name) throws Exception {
    String command = createAndSignQuery(guid, GnsProtocol.ADD_ALIAS,
            GnsProtocol.GUID, guid.getGuid(), GnsProtocol.NAME, name);
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Removes the alias for the given guid.
   *
   * @param guid
   * @param name - the alias
   * @throws Exception
   */
  public void removeAlias(GuidEntry guid, String name) throws Exception {
    String command = createAndSignQuery(guid, GnsProtocol.REMOVE_ALIAS,
            GnsProtocol.GUID, guid.getGuid(), GnsProtocol.NAME, name);
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Retrieve the aliases associated with the given guid.
   *
   * @param guid
   * @return - a JSONArray containing the aliases
   * @throws Exception
   */
  public JSONArray getAliases(GuidEntry guid) throws Exception {
    String command = createAndSignQuery(guid, GnsProtocol.RETRIEVE_ALIASES,
            GnsProtocol.GUID, guid.getGuid());

    String response = sendGetCommand(command);
    // System.out.println("ALIASES: |" + response + "|");
    try {
      return new JSONArray(checkResponse(command, response));
    } catch (JSONException e) {
      throw new GnsException("Invalid alias list", e);
    }
  }

  // /////////////////////////////////////////
  // // PLATFORM DEPENDENT METHODS BELOW /////
  // /////////////////////////////////////////
  /**
   * Check that the connectivity with the host:port can be established
   *
   * @throws IOException throws exception if a communication error occurs
   */
  public void checkConnectivity() throws IOException {
    if (isAndroid) {
      String urlString = "http://" + host + ":" + port + "/";
      final AndroidHttpGet httpGet = new AndroidHttpGet();
      httpGet.execute(urlString);
      try {
        Object httpGetResponse = httpGet.get();
        if (httpGetResponse instanceof IOException) {
          throw (IOException) httpGetResponse;
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
    } else // Desktop version
    {
      sendGetCommand(null);
    }
  }

  /**
   * Creates a new GUID associated with an account.
   *
   * @param accountGuid
   * @param name
   * @param publicKey
   * @return
   * @throws Exception
   */
  private String guidCreate(GuidEntry accountGuid, String name, PublicKey publicKey) throws Exception {
    byte[] publicKeyBytes = publicKey.getEncoded();
    String publicKeyString = Base64.encodeToString(publicKeyBytes, false);
    String command = createAndSignQuery(accountGuid, GnsProtocol.ADD_GUID,
            GnsProtocol.ACCOUNT_GUID, accountGuid.getGuid(),
            GnsProtocol.NAME, URIEncoderDecoder.quoteIllegal(name, ""),
            GnsProtocol.PUBLIC_KEY, publicKeyString);
    String response = sendGetCommand(command);
    return checkResponse(command, response);
  }

  /**
   * Register a new account guid with the corresponding alias and the given
   * public key on the GNS server. Returns a new guid.
   *
   * @param alias the alias to register (usually an email address)
   * @param publicKey the public key associate with the account
   * @return guid the GUID generated by the GNS
   * @throws IOException
   * @throws UnsupportedEncodingException
   * @throws GnsException
   * @throws GnsInvalidGuidException if the user already exists
   */
  private String accountGuidCreate(String alias, PublicKey publicKey, String password) throws UnsupportedEncodingException, IOException,
          GnsException, GnsInvalidGuidException, NoSuchAlgorithmException {
    byte[] publicKeyBytes = publicKey.getEncoded();
    String publicKeyString = Base64.encodeToString(publicKeyBytes, false);
    String command;
    if (password != null) {
      command = createQuery(GnsProtocol.REGISTER_ACCOUNT, GnsProtocol.NAME,
              URIEncoderDecoder.quoteIllegal(alias, ""), GnsProtocol.PUBLIC_KEY, publicKeyString,
              GnsProtocol.PASSWORD, Base64.encodeToString(Password.encryptPassword(password, alias), false));
    } else {
      command = createQuery(GnsProtocol.REGISTER_ACCOUNT, GnsProtocol.NAME,
              URIEncoderDecoder.quoteIllegal(alias, ""), GnsProtocol.PUBLIC_KEY, publicKeyString);
    }
    return checkResponse(command, sendGetCommand(command));

  }

  // ///////////////////////////////
  // // PRIVATE METHODS BELOW /////
  // /////////////////////////////
  protected void aclAdd(String accessType, GuidEntry guid, String field, String accesserGuid) throws Exception {
    String command = createAndSignQuery(guid, GnsProtocol.ACL_ADD, GnsProtocol.ACL_TYPE, accessType, GnsProtocol.GUID,
            guid.getGuid(), GnsProtocol.FIELD, field, GnsProtocol.ACCESSER, accesserGuid == null
                    ? GnsProtocol.ALL_USERS
                    : accesserGuid);
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  protected void aclRemove(String accessType, GuidEntry guid, String field, String accesserGuid) throws Exception {
    String command = createAndSignQuery(guid, GnsProtocol.ACL_REMOVE, GnsProtocol.ACL_TYPE, accessType,
            GnsProtocol.GUID, guid.getGuid(), GnsProtocol.FIELD, field, GnsProtocol.ACCESSER, accesserGuid == null
                    ? GnsProtocol.ALL_USERS
                    : accesserGuid);
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  protected JSONArray aclGet(String accessType, GuidEntry guid, String field, String accesserGuid) throws Exception {
    String command = createAndSignQuery(guid, GnsProtocol.ACL_RETRIEVE, GnsProtocol.ACL_TYPE, accessType,
            GnsProtocol.GUID, guid.getGuid(), GnsProtocol.FIELD, field, GnsProtocol.ACCESSER, accesserGuid == null
                    ? GnsProtocol.ALL_USERS
                    : accesserGuid);
    String response = sendGetCommand(command);
    try {
      return new JSONArray(checkResponse(command, response));
    } catch (JSONException e) {
      throw new GnsException("Invalid ACL list", e);
    }
  }

  protected String checkResponse(String command, String response) throws GnsException {
    // System.out.println("response:" + response);
    if (response.startsWith(GnsProtocol.BAD_RESPONSE)) {
      String results[] = response.split(" ");
      // System.out.println("results length:" + results.length);
      if (results.length < 2) {
        throw new GnsException("Invalid bad response indicator: " + response + " Command: " + command);
      } else if (results.length >= 2) {
        // System.out.println("results[0]:" + results[0]);
        // System.out.println("results[1]:" + results[1]);
        String error = results[1];
        // deal with the rest
        StringBuilder parts = new StringBuilder();
        for (int i = 2; i < results.length; i++) {
          parts.append(" ");
          parts.append(results[i]);
        }
        String rest = parts.toString();

        if (error.startsWith(GnsProtocol.BAD_SIGNATURE)) {
          throw new EncryptionException();
        }
        if (error.startsWith(GnsProtocol.BAD_GUID) || error.startsWith(GnsProtocol.BAD_ACCESSOR_GUID)
                || error.startsWith(GnsProtocol.DUPLICATE_GUID) || error.startsWith(GnsProtocol.BAD_ACCOUNT)) {
          throw new GnsInvalidGuidException(error + rest);
        }
        if (error.startsWith(GnsProtocol.BAD_FIELD) || error.startsWith(GnsProtocol.DUPLICATE_FIELD)) {
          throw new GnsInvalidFieldException(error + rest);
        }
        if (error.startsWith(GnsProtocol.BAD_USER) || error.startsWith(GnsProtocol.DUPLICATE_USER)) {
          throw new GnsInvalidUserException(error + rest);
        }
        if (error.startsWith(GnsProtocol.BAD_GROUP) || error.startsWith(GnsProtocol.DUPLICATE_GROUP)) {
          throw new GnsInvalidGroupException(error + rest);
        }

        if (error.startsWith(GnsProtocol.ACCESS_DENIED)) {
          throw new GnsACLException(error + rest);
        }

        if (error.startsWith(GnsProtocol.DUPLICATE_NAME)) {
          throw new GnsDuplicateNameException(error + rest);
        }

        if (error.startsWith(GnsProtocol.VERIFICATION_ERROR)) {
          throw new GnsVerificationException(error + rest);
        }
        throw new GnsException("General command failure: " + error + rest);
      }
    }
    if (response.startsWith(GnsProtocol.NULL_RESPONSE)) {
      return null;
    } else {
      return response;
    }
  }

  /**
   * Creates a http query string from the given action string and a variable
   * number of key and value pairs.
   *
   * @param action
   * @param keysAndValues
   * @return the query string
   * @throws IOException
   */
  protected String createQuery(String action, String... keysAndValues) throws IOException {
    String key;
    String value;
    StringBuilder result = new StringBuilder(action + QUERYPREFIX);

    for (int i = 0; i < keysAndValues.length; i = i + 2) {
      key = keysAndValues[i];
      value = keysAndValues[i + 1];
      result.append(URIEncoderDecoder.quoteIllegal(key, "") + VALSEP + URIEncoderDecoder.quoteIllegal(value, "")
              + (i + 2 < keysAndValues.length ? KEYSEP : ""));
    }
    return result.toString();
  }

  /**
   * Creates a http query string from the given action string and a variable
   * number of key and value pairs with a signature parameter. The signature is
   * generated from the query signed by the given guid.
   *
   * @param guid
   * @param action
   * @param keysAndValues
   * @return the query string
   * @throws GnsException
   */
  protected String createAndSignQuery(GuidEntry guid, String action, String... keysAndValues) throws GnsException {
    String key;
    String value;
    StringBuilder encodedString = new StringBuilder(action + QUERYPREFIX);
    StringBuilder unencodedString = new StringBuilder(action + QUERYPREFIX);

    try {
      // map over the leys and values to produce the query
      for (int i = 0; i < keysAndValues.length; i = i + 2) {
        key = keysAndValues[i];
        value = keysAndValues[i + 1];
        encodedString.append(URIEncoderDecoder.quoteIllegal(key, "") + VALSEP
                + URIEncoderDecoder.quoteIllegal(value, "") + (i + 2 < keysAndValues.length ? KEYSEP : ""));
        unencodedString.append(key + VALSEP + value + (i + 2 < keysAndValues.length ? KEYSEP : ""));
      }

      // generate the signature from the unencoded query
      String signature = signDigestOfMessage(guid, unencodedString.toString());
      // return the encoded query with the signature appended
      return encodedString.toString() + KEYSEP + GnsProtocol.SIGNATURE + VALSEP + signature;
    } catch (Exception e) {
      throw new GnsException("Error encoding message", e);
    }
  }

  /**
   * Signs a digest of a message using private key of the given guid.
   *
   * @param guid
   * @param message
   * @return a signed digest of the message string
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   */
  private String signDigestOfMessage(GuidEntry guid, String message) throws NoSuchAlgorithmException,
          InvalidKeyException, SignatureException {

    KeyPair keypair;
    keypair = new KeyPair(guid.getPublicKey(), guid.getPrivateKey());

    PrivateKey privateKey = keypair.getPrivate();
    Signature instance = Signature.getInstance(GnsProtocol.SIGNATURE_ALGORITHM);

    instance.initSign(privateKey);
    // instance.update(messageDigest);
    instance.update(message.getBytes());
    byte[] signature = instance.sign();

    return ByteUtils.toHex(signature);
  }

  // /////////////////////////////////////////
  // // PLATFORM DEPENDENT METHODS BELOW /////
  // /////////////////////////////////////////
  /**
   * Sends a HTTP get with given queryString to the host specified by the
   * {@link host} field.
   *
   * @param queryString
   * @return result of get as a string
   * @throws IOException if an error occurs
   */
  protected String sendGetCommand(String queryString) throws IOException {
    if (isAndroid) {
      return androidSendGetCommand(queryString);
    } else {
      return desktopSendGetCommmand(queryString);
    }
  }

  /**
   * Sends a HTTP get with given queryString to the host specified by the
   * {@link host} field.
   *
   * @param queryString
   * @return result of get as a string
   * @throws IOException if an error occurs
   */
  private String desktopSendGetCommmand(String queryString) throws IOException {
    // long t0 = System.currentTimeMillis();
    HttpURLConnection connection = null;
    try {

      String urlString = "http://" + host + ":" + port;
      if (queryString != null) {
        urlString += "/GNS/" + queryString;
      }
      GNSClient.getLogger().fine("Sending: " + urlString);
      URL serverURL = new URL(urlString);
      // set up out communications stuff
      connection = null;

      // Set up the initial connection
      connection = (HttpURLConnection) serverURL.openConnection();
      connection.setRequestMethod("GET");
      connection.setDoOutput(true);
      connection.setReadTimeout(readTimeout);

      connection.connect();

      // read the result from the server
      BufferedReader inputStream = new BufferedReader(new InputStreamReader(connection.getInputStream()));

      String response = null;
      int cnt = readRetries;
      do {
        try {
          response = inputStream.readLine(); // we only expect one line to be
          // sent
          break;
        } catch (java.net.SocketTimeoutException e) {
          GNSClient.getLogger().info("Get Response timed out. Trying " + cnt + " more times. Query is " + queryString);
        }
      } while (cnt-- > 0);
      try {
        // in theory this close should allow the keepalive mechanism to operate
        // correctly
        // http://docs.oracle.com/javase/6/docs/technotes/guides/net/http-keepalive.html
        inputStream.close();
      } catch (IOException e) {
        GNSClient.getLogger().warning("Problem closing the HttpURLConnection's stream.");
      }
      GNSClient.getLogger().fine("Received: " + response);
      if (response != null) {
        return response;
      } else {
        throw new IOException("No response to command: " + queryString);
      }
    } finally {
      // close the connection, set all objects to null
      connection.disconnect();
      connection = null;
      // long t1 = System.currentTimeMillis();
      // System.out.println("QueryLength = " + " Latency = " + (t1 - t0) +
      // " ms");
    }
  }

  private String androidSendGetCommand(String queryString) throws IOException {
    String urlString = "http://" + host + ":" + port + "/GNS/" + queryString;
    final AndroidHttpGet httpGet = new AndroidHttpGet();
    httpGet.execute(urlString);
    try {
      Object httpGetResponse = httpGet.get();
      if (httpGetResponse instanceof IOException) {
        throw (IOException) httpGetResponse;
      } else {
        return (String) httpGetResponse;
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void stop() {
    // nothing to stop
  }

  /**
   * Creates a tag to the tags of the guid.
   *
   * @param guid
   * @param tag
   * @throws Exception
   */
  @Override
  public void addTag(GuidEntry guid, String tag) throws Exception {
    String command = createAndSignQuery(guid, GnsProtocol.ADD_TAG,
            GnsProtocol.GUID, guid.getGuid(), GnsProtocol.NAME, tag);
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  private class AndroidHttpGet extends DownloadTask {

    /**
     * Creates a new <code>httpGet</code> object
     */
    public AndroidHttpGet() {
      super();
    }

    // onPostExecute displays the results of the AsyncTask.
    @Override
    protected void onPostExecute(Object result) {
    }

  }

}
