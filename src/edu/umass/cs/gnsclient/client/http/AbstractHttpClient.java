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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
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

import edu.umass.cs.gnscommon.GnsProtocol;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnscommon.utils.ByteUtils;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
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
 * This class defines a AbstractGnrsClient to communicate with a GNS instance
 * over HTTP.
 *
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
@Deprecated
public abstract class AbstractHttpClient {

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
  public static AbstractHttpClient gns;

  /**
   * Creates a new <code>AbstractGnrsClient</code> object
   *
   * @param host Hostname of the GNS instance
   * @param port Port number of the GNS instance
   */
  public AbstractHttpClient(String host, int port) {
    this.host = host;
    this.port = port;
  }

  /**
   * Returns the gns value.
   *
   * @return Returns the gns.
   */
  public static AbstractHttpClient getGns() {
    return gns;
  }

  /**
   * Sets the gns value.
   *
   * @param gns The gns to set.
   */
  public static void setGns(AbstractHttpClient gns) {
    AbstractHttpClient.gns = gns;
  }

  /**
   * Returns the host value.
   *
   * @return Returns the host.
   */
  public String getGnsHost() {
    return host;
  }

  /**
   * Returns the port value.
   *
   * @return Returns the port.
   */
  public int getGnsPort() {
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
   * Get the public key for a given alias
   *
   * @param alias
   * @return the public key registered for the alias
   * @throws GnsInvalidGuidException
   * @throws GnsException
   * @throws IOException
   */
  public PublicKey lookupPublicKeyFromAlias(String alias) throws GnsInvalidGuidException, GnsException, IOException {

    String guid = lookupGuid(alias);
    return lookupPublicKey(guid);
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
  public PublicKey lookupPublicKey(String guid) throws GnsInvalidGuidException, GnsException, IOException {
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
   * Obtains the guid of the alias from the GNS server. Query format:
   *
   * @param alias
   * @return guid
   * @throws IOException
   * @throws UnsupportedEncodingException
   * @throws GnsException
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
   * Returns a JSON object containing all of the guid information
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
   * Returns a JSON object containing all of the account information for a guid.
   *
   * @param guid
   * @return
   * @throws IOException
   * @throws GnsException
   */
  public JSONObject lookupAccountRecord(String guid) throws IOException, GnsException {
    String command = createQuery(GnsProtocol.LOOKUP_ACCOUNT_RECORD, GnsProtocol.GUID, guid);
    String response = sendGetCommand(command);
    checkResponse(command, response);
    try {
      return new JSONObject(response);
    } catch (JSONException e) {
      throw new GnsException("Failed to parse LOOKUP_ACCOUNT_RECORD response", e);
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
  public GuidEntry registerNewAccountGuid(String alias) throws Exception {

    KeyPair keyPair = KeyPairGenerator.getInstance(GnsProtocol.RSA_ALGORITHM).generateKeyPair();
    String guid = registerNewAccountGuid(alias, keyPair.getPublic());

    KeyPairUtils.saveKeyPair(host + ":" + port, alias, guid, keyPair);

    GuidEntry entry = new GuidEntry(alias, guid, keyPair.getPublic(), keyPair.getPrivate());

    return entry;
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
  public String registerNewAccountGuid(String alias, PublicKey publicKey) throws UnsupportedEncodingException,
          IOException, GnsException, GnsInvalidGuidException {
    byte[] publicKeyBytes = publicKey.getEncoded();
    String publicKeyString = Base64.encodeToString(publicKeyBytes, false);
    String command = createQuery(GnsProtocol.REGISTER_ACCOUNT, GnsProtocol.NAME,
            URIEncoderDecoder.quoteIllegal(alias, ""), GnsProtocol.PUBLIC_KEY, publicKeyString);
    return checkResponse(command, sendGetCommand(command));

  }

  /**
   * Verify an account by sending the verification code back to the server.
   *
   * @param guid the account GUID to verify
   * @param code the verification code
   * @return ?
   * @throws Exception
   */
  public String verifyAccountGuid(GuidEntry guid, String code) throws Exception {
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
  public void removeAccountGuid(GuidEntry guid) throws Exception {
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
  public GuidEntry addGuid(GuidEntry accountGuid, String alias) throws Exception {

    KeyPair keyPair = KeyPairGenerator.getInstance(GnsProtocol.RSA_ALGORITHM).generateKeyPair();
    String newGuid = addGuid(accountGuid, alias, keyPair.getPublic());

    KeyPairUtils.saveKeyPair(host + ":" + port, alias, newGuid, keyPair);

    GuidEntry entry = new GuidEntry(alias, newGuid, keyPair.getPublic(), keyPair.getPrivate());

    return entry;
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
  public String addGuid(GuidEntry accountGuid, String name, PublicKey publicKey) throws Exception {
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
   * Removes a guid (not for account Guids - use removeAccountGuid for them).
   *
   * @param guid the guid to remove
   * @throws Exception
   */
  public void removeGuid(GuidEntry guid) throws Exception {
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
  public void removeGuid(GuidEntry accountGuid, String guidToRemove) throws Exception {
    String command = createAndSignQuery(accountGuid, GnsProtocol.REMOVE_GUID, GnsProtocol.GUID, guidToRemove,
            GnsProtocol.ACCOUNT_GUID, accountGuid.getGuid());
    String response = sendGetCommand(command);

    checkResponse(command, response);
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
   * @throws java.security.InvalidKeyException
   * @throws java.security.NoSuchAlgorithmException
   * @throws java.security.SignatureException
   */
  public void addToGroup(String groupGuid, String guidToAdd, GuidEntry writer) throws IOException,
          GnsInvalidGuidException, GnsException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
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
  public void addToGroup(String groupGuid, JSONArray members, GuidEntry writer) throws IOException,
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
   * @throws java.security.InvalidKeyException
   * @throws java.security.NoSuchAlgorithmException
   * @throws java.security.SignatureException
   */
  public void removeFromGroup(String guid, String guidToRemove, GuidEntry writer) throws IOException,
          GnsInvalidGuidException, GnsException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
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
  public void removeFromGroup(String guid, JSONArray members, GuidEntry writer) throws IOException,
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
   * @param groupGuid
   * @param reader the guid of the entity doing the lookup
   * @return the list of guids as a JSONArray
   * @throws IOException if a communication error occurs
   * @throws GnsException if a protocol error occurs or the list cannot be
   * parsed
   * @throws GnsInvalidGuidException if the group guid is invalid
   * @throws java.security.InvalidKeyException
   * @throws java.security.NoSuchAlgorithmException
   * @throws java.security.SignatureException
   */
  public JSONArray getGroupMembers(String groupGuid, GuidEntry reader) throws IOException, GnsException,
          GnsInvalidGuidException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
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
   * Return a list of the groups that the guid is a member of. Signs the query
   * using the private key of the user associated with the guid.
   *
   * @param groupGuid
   * @param reader the guid of the entity doing the lookup
   * @return the list of groups as a JSONArray
   * @throws IOException if a communication error occurs
   * @throws GnsException if a protocol error occurs or the list cannot be
   * parsed
   * @throws GnsInvalidGuidException if the group guid is invalid
   * @throws java.security.InvalidKeyException
   * @throws java.security.NoSuchAlgorithmException
   * @throws java.security.SignatureException
   */
  public JSONArray getGroups(String groupGuid, GuidEntry reader) throws IOException, GnsException,
          GnsInvalidGuidException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
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
  public void addGroupMembershipUpdatePermission(GuidEntry groupGuid, String guidToAuthorize) throws Exception {
    addToACL(GnsProtocol.AccessType.WRITE_WHITELIST, groupGuid, GnsProtocol.GROUP_ACL, guidToAuthorize);
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
  public void removeGroupMembershipUpdatePermission(GuidEntry groupGuid, String guidToUnauthorize) throws Exception {
    removeFromACL(GnsProtocol.AccessType.WRITE_WHITELIST, groupGuid, GnsProtocol.GROUP_ACL, guidToUnauthorize);
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
  public void addGroupMembershipReadPermission(GuidEntry groupGuid, String guidToAuthorize) throws Exception {
    addToACL(GnsProtocol.AccessType.READ_WHITELIST, groupGuid, GnsProtocol.GROUP_ACL, guidToAuthorize);
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
  public void removeGroupMembershipReadPermission(GuidEntry groupGuid, String guidToUnauthorize) throws Exception {
    removeFromACL(GnsProtocol.AccessType.READ_WHITELIST, groupGuid, GnsProtocol.GROUP_ACL, guidToUnauthorize);
  }

  /**
   * Called by the member to request admission to the group.
   *
   * @param groupGuid
   * @param member
   * @throws IOException
   * @throws GnsException
   * @throws GnsInvalidGuidException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   */
  public void requestJoinGroup(String groupGuid, GuidEntry member) throws IOException, GnsException,
          GnsInvalidGuidException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    String command = createAndSignQuery(member, GnsProtocol.REQUEST_JOIN_GROUP, GnsProtocol.GUID, groupGuid,
            GnsProtocol.MEMBER, member.getGuid());
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Called by the group see who wants admission to the group.
   *
   * @param groupGuid
   * @return
   * @throws IOException
   * @throws GnsException
   * @throws GnsInvalidGuidException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   */
  public JSONArray getJoinGroupRequests(GuidEntry groupGuid) throws IOException, GnsException, GnsInvalidGuidException,
          InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    String command = createAndSignQuery(groupGuid, GnsProtocol.RETRIEVE_GROUP_JOIN_REQUESTS, GnsProtocol.GUID,
            groupGuid.getGuid());
    String response = sendGetCommand(command);

    try {
      return new JSONArray(checkResponse(command, response));
    } catch (JSONException e) {
      throw new GnsException("Invalid member list", e);
    }
  }

  /**
   * Called by the group admit a member that previously requested admission. The
   * results are undefined if you call this with members that didn't request
   * permission to be added.
   *
   * @param groupGuid
   * @param member
   * @throws IOException
   * @throws GnsException
   * @throws GnsInvalidGuidException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   */
  public void grantMembership(GuidEntry groupGuid, String member) throws IOException, GnsException,
          GnsInvalidGuidException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    String command = createAndSignQuery(groupGuid, GnsProtocol.GRANT_MEMBERSHIP, GnsProtocol.GUID, groupGuid.getGuid(),
            GnsProtocol.MEMBER, member);
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Called by the group admit a multiple members that previously requested
   * admission. The results are undefined if you call this with members that
   * didn't request permission to be added.
   *
   * @param groupGuid
   * @param members - a JSONArray
   * @throws IOException
   * @throws GnsException
   * @throws GnsInvalidGuidException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   */
  public void grantMemberships(GuidEntry groupGuid, JSONArray members) throws IOException, GnsException,
          GnsInvalidGuidException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    String command = createAndSignQuery(groupGuid, GnsProtocol.GRANT_MEMBERSHIP, GnsProtocol.GUID, groupGuid.getGuid(),
            GnsProtocol.MEMBERS, members.toString());
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Called by the member to request to leave the group.
   *
   * @param groupGuid
   * @param member
   * @throws IOException
   * @throws GnsException
   * @throws GnsInvalidGuidException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   */
  public void requestLeaveGroup(String groupGuid, GuidEntry member) throws IOException, GnsException,
          GnsInvalidGuidException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    String command = createAndSignQuery(member, GnsProtocol.REQUEST_LEAVE_GROUP, GnsProtocol.GUID, groupGuid,
            GnsProtocol.MEMBER, member.getGuid());
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Called by the group see who wants to leave the group.
   *
   * @param groupGuid
   * @return
   * @throws IOException
   * @throws GnsException
   * @throws GnsInvalidGuidException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   */
  public JSONArray getLeaveGroupRequests(GuidEntry groupGuid) throws IOException, GnsException,
          GnsInvalidGuidException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    String command = createAndSignQuery(groupGuid, GnsProtocol.RETRIEVE_GROUP_LEAVE_REQUESTS, GnsProtocol.GUID,
            groupGuid.getGuid());
    String response = sendGetCommand(command);

    try {
      return new JSONArray(checkResponse(command, response));
    } catch (JSONException e) {
      throw new GnsException("Invalid member list", e);
    }
  }

  /**
   * Called by the group revoke membership for a member that previously request
   * to leave. The results are undefined if you call this with members that
   * didn't request permission to leave.
   *
   * @param groupGuid
   * @param member
   * @throws IOException
   * @throws GnsException
   * @throws GnsInvalidGuidException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   */
  public void revokeMembership(GuidEntry groupGuid, String member) throws IOException, GnsException,
          GnsInvalidGuidException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    String command = createAndSignQuery(groupGuid, GnsProtocol.REVOKE_MEMBERSHIP, GnsProtocol.GUID,
            groupGuid.getGuid(), GnsProtocol.MEMBER, member);
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Called by the group revoke membership for multiple members that previously
   * requested to leave. The results are undefined if you call this with members
   * that didn't request permission to leave.
   *
   * @param groupGuid
   * @param members - a JSONArray
   * @throws IOException
   * @throws GnsException
   * @throws GnsInvalidGuidException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   */
  public void revokeMemberships(GuidEntry groupGuid, JSONArray members) throws IOException, GnsException,
          GnsInvalidGuidException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    String command = createAndSignQuery(groupGuid, GnsProtocol.REVOKE_MEMBERSHIP, GnsProtocol.GUID,
            groupGuid.getGuid(), GnsProtocol.MEMBERS, members.toString());
    String response = sendGetCommand(command);

    checkResponse(command, response);
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
  public void addToACL(GnsProtocol.AccessType accessType, GuidEntry targetGuid, String field, String accesserGuid)
          throws Exception {
    addToACL(accessType.name(), targetGuid, field, accesserGuid);
  }

  private void addToACL(String accessType, GuidEntry guid, String field, String accesserGuid) throws Exception {
    String command = createAndSignQuery(guid, GnsProtocol.ACL_ADD, GnsProtocol.ACL_TYPE, accessType, GnsProtocol.GUID,
            guid.getGuid(), GnsProtocol.FIELD, field, GnsProtocol.ACCESSER, accesserGuid);
    String response = sendGetCommand(command);

    checkResponse(command, response);
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
  public void removeFromACL(GnsProtocol.AccessType accessType, GuidEntry guid, String field, String accesserGuid)
          throws Exception {
    removeFromACL(accessType.name(), guid, field, accesserGuid);
  }

  private void removeFromACL(String accessType, GuidEntry guid, String field, String accesserGuid) throws Exception {
    String command = createAndSignQuery(guid, GnsProtocol.ACL_REMOVE, GnsProtocol.ACL_TYPE, accessType,
            GnsProtocol.GUID, guid.getGuid(), GnsProtocol.FIELD, field, GnsProtocol.ACCESSER, accesserGuid == null
                    ? GnsProtocol.ALL_USERS
                    : accesserGuid);
    String response = sendGetCommand(command);

    checkResponse(command, response);
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
  public JSONArray getACL(GnsProtocol.AccessType accessType, GuidEntry guid, String field, String accesserGuid)
          throws Exception {
    return getACL(accessType.name(), guid, field, accesserGuid);
  }

  private JSONArray getACL(String accessType, GuidEntry guid, String field, String accesserGuid) throws Exception {
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

  /**
   * Creates a new field with value. Only the GUID can create a new field. Use
   * addToACL to allow other guids to write to update this field
   *
   * @param targetGuid
   * @param field
   * @param value
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   * @throws GnsException
   */
  public void createField(GuidEntry targetGuid, String field, String value) throws IOException, InvalidKeyException,
          NoSuchAlgorithmException, SignatureException, GnsException {
    createField(targetGuid.getGuid(), field, value, targetGuid);
  }

  /**
   * Creates a new field with value. Allows a a different guid as the writer. If
   * the writer is different use addToACL first to allow other the guid to write
   * this field.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   * @throws GnsException
   */
  public void createField(String targetGuid, String field, String value, GuidEntry writer) throws IOException,
          InvalidKeyException, NoSuchAlgorithmException, SignatureException, GnsException {
    String command = createAndSignQuery(writer, GnsProtocol.CREATE, GnsProtocol.GUID, targetGuid, GnsProtocol.FIELD,
            field, GnsProtocol.VALUE, value, GnsProtocol.WRITER, writer.getGuid());
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Creates a new field with value being the list. Only the GUID can create a
   * new field. Use addToACL to allow other guids to write to update this field
   *
   * @param targetGuid
   * @param field
   * @param value
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   * @throws GnsException
   */
  public void createFieldUsingList(GuidEntry targetGuid, String field, JSONArray value) throws IOException,
          InvalidKeyException, NoSuchAlgorithmException, SignatureException, GnsException {
    createFieldUsingList(targetGuid.getGuid(), field, value, targetGuid);
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
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   * @throws GnsException
   */
  public void createFieldUsingList(String targetGuid, String field, JSONArray value, GuidEntry writer)
          throws IOException, InvalidKeyException, NoSuchAlgorithmException, SignatureException, GnsException {
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
  public void removeField(String targetGuid, String field, GuidEntry writer) throws IOException, InvalidKeyException,
          NoSuchAlgorithmException, SignatureException, GnsException {
    String command = createAndSignQuery(writer, GnsProtocol.REMOVE_FIELD, GnsProtocol.GUID, targetGuid,
            GnsProtocol.FIELD, field, GnsProtocol.WRITER, writer.getGuid());
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Removes a field.
   *
   * @param targetGuid
   * @param field
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   * @throws GnsException
   */
  public void removeField(GuidEntry targetGuid, String field) throws IOException, InvalidKeyException,
          NoSuchAlgorithmException, SignatureException, GnsException {
    removeField(targetGuid.getGuid(), field, targetGuid);
  }

  /**
   * Appends the value onto a field or creates a new field with value if it does
   * not exist.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   * @throws GnsException
   */
  public void appendOrCreate(String targetGuid, String field, String value, GuidEntry writer) throws IOException,
          InvalidKeyException, NoSuchAlgorithmException, SignatureException, GnsException {
    String command = createAndSignQuery(writer, GnsProtocol.APPEND_OR_CREATE, GnsProtocol.GUID, targetGuid,
            GnsProtocol.FIELD, field, GnsProtocol.VALUE, value, GnsProtocol.WRITER, writer.getGuid());
    String response = sendGetCommand(command);
    checkResponse(command, response);
  }

  /**
   * Appends the value onto a field or creates a new field with value if it does
   * not exist.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   * @throws GnsException
   */
  public void appendOrCreate(GuidEntry targetGuid, String field, String value) throws IOException, InvalidKeyException,
          NoSuchAlgorithmException, SignatureException, GnsException {
    appendOrCreate(targetGuid.getGuid(), field, value, targetGuid);
  }

  /**
   * Replaces the value of the field or creates a new field with value if it
   * does not exist.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   * @throws GnsException
   */
  public void replaceOrCreate(String targetGuid, String field, String value, GuidEntry writer) throws IOException,
          InvalidKeyException, NoSuchAlgorithmException, SignatureException, GnsException {
    String command = createAndSignQuery(writer, GnsProtocol.REPLACE_OR_CREATE, GnsProtocol.GUID, targetGuid,
            GnsProtocol.FIELD, field, GnsProtocol.VALUE, value, GnsProtocol.WRITER, writer.getGuid());
    String response = sendGetCommand(command);
    checkResponse(command, response);
  }

  /**
   * Replaces the value of the field or creates a new field with value if it
   * does not exist.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   * @throws GnsException
   */
  public void replaceOrCreate(GuidEntry targetGuid, String field, String value) throws IOException,
          InvalidKeyException, NoSuchAlgorithmException, SignatureException, GnsException {
    replaceOrCreate(targetGuid.getGuid(), field, value, targetGuid);
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
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   * @throws GnsException
   */
  public void appendOrCreateUsingList(String targetGuid, String field, JSONArray value, GuidEntry writer)
          throws IOException, InvalidKeyException, NoSuchAlgorithmException, SignatureException, GnsException {
    String command = createAndSignQuery(writer, GnsProtocol.APPEND_OR_CREATE_LIST, GnsProtocol.GUID, targetGuid,
            GnsProtocol.FIELD, field, GnsProtocol.VALUE, value.toString(), GnsProtocol.WRITER, writer.getGuid());
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
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   * @throws GnsException
   */
  public void appendOrCreateUsingList(GuidEntry targetGuid, String field, JSONArray value) throws IOException,
          InvalidKeyException, NoSuchAlgorithmException, SignatureException, GnsException {
    appendOrCreateUsingList(targetGuid.getGuid(), field, value, targetGuid);
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
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   * @throws GnsException
   */
  public void replaceOrCreateUsingList(String targetGuid, String field, JSONArray value, GuidEntry writer)
          throws IOException, InvalidKeyException, NoSuchAlgorithmException, SignatureException, GnsException {
    String command = createAndSignQuery(writer, GnsProtocol.REPLACE_OR_CREATE_LIST, GnsProtocol.GUID, targetGuid,
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
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   * @throws GnsException
   */
  public void replaceOrCreateUsingList(GuidEntry targetGuid, String field, JSONArray value) throws IOException,
          InvalidKeyException, NoSuchAlgorithmException, SignatureException, GnsException {
    replaceOrCreateUsingList(targetGuid.getGuid(), field, value, targetGuid);
  }

  /**
   * Appends a value onto a field. The list is treated like a set meaning
   * duplicates are removed and the order is undetermined.
   *
   * @param targetGuid GUID where the field is stored
   * @param field field name
   * @param value field value
   * @param writer GUID entry of the writer
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   * @throws GnsException
   */
  public void appendValue(String targetGuid, String field, String value, GuidEntry writer) throws IOException,
          InvalidKeyException, NoSuchAlgorithmException, SignatureException, GnsException {
    String command = createAndSignQuery(writer, GnsProtocol.APPEND, GnsProtocol.GUID, targetGuid, GnsProtocol.FIELD,
            field, GnsProtocol.VALUE, value, GnsProtocol.WRITER, writer.getGuid());
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Appends a value onto a field. The list is treated like a set meaning
   * duplicates are removed and the order is undetermined.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   * @throws GnsException
   */
  public void appendValue(GuidEntry targetGuid, String field, String value) throws IOException, InvalidKeyException,
          NoSuchAlgorithmException, SignatureException, GnsException {
    appendValue(targetGuid.getGuid(), field, value, targetGuid);
  }

  /**
   * Appends a value onto a field. The list is treated like a list meaning
   * duplicates are allowed and the order is maintained.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   * @throws GnsException
   */
  public void appendValueWithDuplicates(String targetGuid, String field, String value, GuidEntry writer)
          throws IOException, InvalidKeyException, NoSuchAlgorithmException, SignatureException, GnsException {
    String command = createAndSignQuery(writer, GnsProtocol.APPEND_WITH_DUPLICATION, GnsProtocol.GUID, targetGuid,
            GnsProtocol.FIELD, field, GnsProtocol.VALUE, value, GnsProtocol.WRITER, writer.getGuid());
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Appends a value onto a field. The list is treated like a list meaning
   * duplicates are allowed and the order is maintained.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   * @throws GnsException
   */
  public void appendValueWithDuplicates(GuidEntry targetGuid, String field, String value) throws IOException,
          InvalidKeyException, NoSuchAlgorithmException, SignatureException, GnsException {
    appendValueWithDuplicates(targetGuid.getGuid(), field, value, targetGuid);
  }

  /**
   * Appends a list of values onto a field. The list is treated like a set
   * meaning duplicates are removed and the order is undetermined.
   *
   * @param targetGuid GUID where the field is stored
   * @param field field name
   * @param value list of values
   * @param writer GUID entry of the writer
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   * @throws GnsException
   */
  public void appendValuesUsingList(String targetGuid, String field, JSONArray value, GuidEntry writer)
          throws IOException, InvalidKeyException, NoSuchAlgorithmException, SignatureException, GnsException {
    String command = createAndSignQuery(writer, GnsProtocol.APPEND_LIST, GnsProtocol.GUID, targetGuid,
            GnsProtocol.FIELD, field, GnsProtocol.VALUE, value.toString(), GnsProtocol.WRITER, writer.getGuid());
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Replaces all the values of field with the value.
   *
   * @param targetGuid GUID where the field is stored
   * @param field field name
   * @param value field value
   * @param writer GUID entry of the writer
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   * @throws GnsException
   */
  public void replaceValue(String targetGuid, String field, String value, GuidEntry writer) throws IOException,
          InvalidKeyException, NoSuchAlgorithmException, SignatureException, GnsException {
    String command = createAndSignQuery(writer, GnsProtocol.REPLACE, GnsProtocol.GUID, targetGuid, GnsProtocol.FIELD,
            field, GnsProtocol.VALUE, value, GnsProtocol.WRITER, writer.getGuid());
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
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   * @throws GnsException
   */
  public void replaceValuesUsingList(String targetGuid, String field, JSONArray value, GuidEntry writer)
          throws IOException, InvalidKeyException, NoSuchAlgorithmException, SignatureException, GnsException {
    String command = createAndSignQuery(writer, GnsProtocol.REPLACE_LIST, GnsProtocol.GUID, targetGuid,
            GnsProtocol.FIELD, field, GnsProtocol.VALUE, value.toString(), GnsProtocol.WRITER, writer.getGuid());
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Removes the value from the field.
   *
   * @param targetGuid GUID where the field is stored
   * @param field field name
   * @param value field value
   * @param writer GUID entry of the writer
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   * @throws GnsException
   */
  public void removeValue(String targetGuid, String field, String value, GuidEntry writer) throws IOException,
          InvalidKeyException, NoSuchAlgorithmException, SignatureException, GnsException {
    String command = createAndSignQuery(writer, GnsProtocol.REMOVE, GnsProtocol.GUID, targetGuid, GnsProtocol.FIELD,
            field, GnsProtocol.VALUE, value, GnsProtocol.WRITER, writer.getGuid());
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
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   * @throws GnsException
   */
  public void removeValuesUsingList(String targetGuid, String field, JSONArray value, GuidEntry writer)
          throws IOException, InvalidKeyException, NoSuchAlgorithmException, SignatureException, GnsException {
    String command = createAndSignQuery(writer, GnsProtocol.REMOVE_LIST, GnsProtocol.GUID, targetGuid,
            GnsProtocol.FIELD, field, GnsProtocol.VALUE, value.toString(), GnsProtocol.WRITER, writer.getGuid());
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Substitutes the value for oldValue in the field.
   *
   * @param targetGuid GUID where the field is stored
   * @param field field name
   * @param newValue field value
   * @param oldValue field value to replace
   * @param writer GUID entry of the writer
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   * @throws GnsException
   */
  public void substituteValue(String targetGuid, String field, String newValue, String oldValue, GuidEntry writer)
          throws IOException, InvalidKeyException, NoSuchAlgorithmException, SignatureException, GnsException {
    String command = createAndSignQuery(writer, GnsProtocol.SUBSTITUTE, GnsProtocol.GUID, targetGuid,
            GnsProtocol.FIELD, field, GnsProtocol.VALUE, newValue, GnsProtocol.OLD_VALUE, oldValue, GnsProtocol.WRITER,
            writer.getGuid());
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Pairwise substitutes all the values for the oldValues in the field. In
   * other words item one in newValue replaces item one of oldValue in the
   * field... and so on.
   *
   * @param targetGuid GUID where the field is stored
   * @param field field name
   * @param newValue list of values to sub in
   * @param oldValue field values to replace
   * @param writer GUID entry of the writer
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   * @throws GnsException
   */
  public void substituteValuesUsingList(String targetGuid, String field, JSONArray newValue, JSONArray oldValue,
          GuidEntry writer) throws IOException, InvalidKeyException, NoSuchAlgorithmException, SignatureException,
          GnsException {
    String command = createAndSignQuery(writer, GnsProtocol.SUBSTITUTE_LIST, GnsProtocol.GUID, targetGuid,
            GnsProtocol.FIELD, field, GnsProtocol.VALUE, newValue.toString(), GnsProtocol.OLD_VALUE, oldValue.toString(),
            GnsProtocol.WRITER, writer.getGuid());
    String response = sendGetCommand(command);

    checkResponse(command, response);
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
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   * @throws GnsException
   */
  public void setValue(String targetGuid, String field, String newValue, int index, GuidEntry writer)
          throws IOException, InvalidKeyException, NoSuchAlgorithmException, SignatureException, GnsException {
    String command = createAndSignQuery(writer, GnsProtocol.SET, GnsProtocol.GUID, targetGuid, GnsProtocol.FIELD,
            field, GnsProtocol.VALUE, newValue, GnsProtocol.N, Integer.toString(index), GnsProtocol.WRITER,
            writer.getGuid());
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Sets a field to be null. That is when read field is called a null will be
   * returned.
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
  public void setFieldNull(String targetGuid, String field, GuidEntry writer) throws IOException, InvalidKeyException,
          NoSuchAlgorithmException, SignatureException, GnsException {
    String command = createAndSignQuery(writer, GnsProtocol.SET_FIELD_NULL, GnsProtocol.GUID, targetGuid,
            GnsProtocol.FIELD, field, GnsProtocol.WRITER, writer.getGuid());
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
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   * @throws GnsException
   */
  public void clearField(String targetGuid, String field, GuidEntry writer) throws IOException, InvalidKeyException,
          NoSuchAlgorithmException, SignatureException, GnsException {
    String command = createAndSignQuery(writer, GnsProtocol.CLEAR, GnsProtocol.GUID, targetGuid, GnsProtocol.FIELD,
            field, GnsProtocol.WRITER, writer.getGuid());
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  //
  // ALL ACCESS VERSIONS OF THE ABOVE
  //
  /**
   * Removes a field that is write accessible to everyone. This is the
   * non-signing version.
   *
   * @param targetGuid
   * @param field
   * @throws IOException
   * @throws GnsException
   */
  public void removeField(String targetGuid, String field) throws IOException, GnsException {
    String command = createQuery(GnsProtocol.REMOVE_FIELD, GnsProtocol.GUID, targetGuid, GnsProtocol.FIELD, field);
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Appends the value onto a field or creates a new field with value if it does
   * not exist and the field must be write accessible to everyone. Does this
   * make sense for create part? This is the non-signing version.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @throws IOException
   * @throws GnsException
   */
  public void appendOrCreate(String targetGuid, String field, String value) throws IOException, GnsException {
    String command = createQuery(GnsProtocol.APPEND_OR_CREATE, GnsProtocol.GUID, targetGuid, GnsProtocol.FIELD, field,
            GnsProtocol.VALUE, value);
    String response = sendGetCommand(command);
    checkResponse(command, response);
  }

  /**
   * Replaces the value of the field or creates a new field with value if it
   * does not exist and the field must be write accessible to everyone. Does
   * this make sense for create part? This is the non-signing version.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @throws IOException
   * @throws GnsException
   */
  public void replaceOrCreate(String targetGuid, String field, String value) throws IOException, GnsException {
    String command = createQuery(GnsProtocol.REPLACE_OR_CREATE, GnsProtocol.GUID, targetGuid, GnsProtocol.FIELD, field,
            GnsProtocol.VALUE, value);
    String response = sendGetCommand(command);
    checkResponse(command, response);
  }

  /**
   * Appends the values of the field onto list of values or creates a new field
   * with values in the list if it does not exist and the field must be write
   * accessible to everyone. Does this make sense for create part? This is the
   * non-signing version.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @throws IOException
   * @throws GnsException
   */
  public void appendOrCreateUsingList(String targetGuid, String field, JSONArray value) throws IOException,
          GnsException {
    String command = createQuery(GnsProtocol.APPEND_OR_CREATE_LIST, GnsProtocol.GUID, targetGuid, GnsProtocol.FIELD,
            field, GnsProtocol.VALUE, value.toString());
    String response = sendGetCommand(command);
    checkResponse(command, response);
  }

  /**
   * Replaces the values of the field with the list of values or creates a new
   * field with values in the list if it does not exist and the field must be
   * write accessible to everyone. Does this make sense for create part? This is
   * the non-signing version.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @throws IOException
   * @throws GnsException
   */
  public void replaceOrCreateUsingList(String targetGuid, String field, JSONArray value) throws IOException,
          GnsException {
    String command = createQuery(GnsProtocol.REPLACE_OR_CREATE_LIST, GnsProtocol.GUID, targetGuid, GnsProtocol.FIELD,
            field, GnsProtocol.VALUE, value.toString());
    String response = sendGetCommand(command);
    checkResponse(command, response);
  }

  /**
   * Appends a value onto a field that is write accessible to everyone. This is
   * the non-signing version.
   *
   * @param targetGuid GUID where the field is stored
   * @param field field name
   * @param value field value
   * @throws IOException
   * @throws GnsException
   */
  public void appendValue(String targetGuid, String field, String value) throws IOException, GnsException {
    String command = createQuery(GnsProtocol.APPEND, GnsProtocol.GUID, targetGuid, GnsProtocol.FIELD, field,
            GnsProtocol.VALUE, value);
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Appends a list of values onto a field that is write accessible to everyone.
   * This is the non-signing version.
   *
   * @param targetGuid GUID where the field is stored
   * @param field field name
   * @param value list of values
   * @throws IOException
   * @throws SignatureException
   * @throws GnsException
   */
  public void appendValuesUsingList(String targetGuid, String field, JSONArray value) throws IOException, GnsException {
    String command = createQuery(GnsProtocol.APPEND_LIST, GnsProtocol.GUID, targetGuid, GnsProtocol.FIELD, field,
            GnsProtocol.VALUE, value.toString());
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Replaces all the values of field with the value and field must be write
   * accessible to everyone. This is the non-signing version.
   *
   * @param targetGuid GUID where the field is stored
   * @param field field name
   * @param value field value
   * @throws IOException
   * @throws GnsException
   */
  public void replaceValue(String targetGuid, String field, String value) throws IOException, GnsException {
    String command = createQuery(GnsProtocol.REPLACE, GnsProtocol.GUID, targetGuid, GnsProtocol.FIELD, field,
            GnsProtocol.VALUE, value);
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Replaces all the values of field with the list of values and field must be
   * write accessible to everyone. This is the non-signing version.
   *
   * @param targetGuid GUID where the field is stored
   * @param field field name
   * @param value list of values
   * @throws IOException
   * @throws GnsException
   */
  public void replaceValuesUsingList(String targetGuid, String field, JSONArray value) throws IOException, GnsException {
    String command = createQuery(GnsProtocol.REPLACE_LIST, GnsProtocol.GUID, targetGuid, GnsProtocol.FIELD, field,
            GnsProtocol.VALUE, value.toString());
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Removes the value from the field that is write accessible to everyone. This
   * is the non-signing version.
   *
   * @param targetGuid GUID where the field is stored
   * @param field field name
   * @param value field value
   * @throws IOException
   * @throws GnsException
   */
  public void removeValue(String targetGuid, String field, String value) throws IOException, GnsException {
    String command = createQuery(GnsProtocol.REMOVE, GnsProtocol.GUID, targetGuid, GnsProtocol.FIELD, field,
            GnsProtocol.VALUE, value);
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Removes all the values in the list from the field that is write accessible
   * to everyone. This is the non-signing version.
   *
   * @param targetGuid GUID where the field is stored
   * @param field field name
   * @param value list of values
   * @throws IOException
   * @throws GnsException
   */
  public void removeValuesUsingList(String targetGuid, String field, JSONArray value) throws IOException, GnsException {
    String command = createQuery(GnsProtocol.REMOVE_LIST, GnsProtocol.GUID, targetGuid, GnsProtocol.FIELD, field,
            GnsProtocol.VALUE, value.toString());
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Substitutes the value for oldValue in the field that is write accessible to
   * everyone. This is the non-signing version.
   *
   * @param targetGuid GUID where the field is stored
   * @param field field name
   * @param newValue field value
   * @param oldValue field value to replace
   * @throws IOException
   * @throws GnsException
   */
  public void substituteValue(String targetGuid, String field, String newValue, String oldValue) throws IOException,
          GnsException {
    String command = createQuery(GnsProtocol.SUBSTITUTE, GnsProtocol.GUID, targetGuid, GnsProtocol.FIELD, field,
            GnsProtocol.VALUE, newValue, GnsProtocol.OLD_VALUE, oldValue);
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Pairwise substitutes all the values for the oldValues in the field that is
   * write accessible to everyone. In other words item one in newValue replaces
   * item one of oldValue in the field... and so on. This is the non-signing
   * version.
   *
   * @param targetGuid GUID where the field is stored
   * @param field field name
   * @param newValue list of values to sub in
   * @param oldValue field values to replace
   * @throws IOException
   * @throws GnsException
   */
  public void substituteValuesUsingList(String targetGuid, String field, JSONArray newValue, JSONArray oldValue)
          throws IOException, GnsException {
    String command = createQuery(GnsProtocol.SUBSTITUTE_LIST, GnsProtocol.GUID, targetGuid, GnsProtocol.FIELD, field,
            GnsProtocol.VALUE, newValue.toString(), GnsProtocol.OLD_VALUE, oldValue.toString());
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Removes all values from the field that is write accessible to everyone.
   *
   * @param targetGuid GUID where the field is stored
   * @param field field name
   * @throws IOException
   * @throws GnsException
   */
  public void clearField(String targetGuid, String field) throws IOException, GnsException {
    String command = createQuery(GnsProtocol.CLEAR, GnsProtocol.GUID, targetGuid, GnsProtocol.FIELD, field);
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  //
  // READING
  //
  /**
   * Reads a single value for a key for the given guid. The guid of the user
   * attempting access is also needed. Signs the query using the private key of
   * the user associated with the guid.
   *
   * @param guid
   * @param field
   * @param reader
   * @return
   * @throws Exception
   */
  public String readField(String guid, String field, GuidEntry reader) throws Exception {
    String command = createAndSignQuery(reader, GnsProtocol.READ_ARRAY_ONE, GnsProtocol.GUID, guid, GnsProtocol.FIELD, field,
            GnsProtocol.READER, reader.getGuid());
    String response = sendGetCommand(command);

    return checkResponse(command, response);
  }

  /**
   * Reads all the values value for a key from the GNS server for the given
   * guid. The guid of the user attempting access is also needed. Signs the
   * query using the private key of the user associated with the guid.
   *
   * @param guid
   * @param field
   * @param reader
   * @return a JSONArray containing the values in the field
   * @throws Exception
   */
  public JSONArray readFieldList(String guid, String field, GuidEntry reader) throws Exception {
    String command = createAndSignQuery(reader, GnsProtocol.READ_ARRAY, GnsProtocol.GUID, guid, GnsProtocol.FIELD, field,
            GnsProtocol.READER, reader.getGuid());
    String response = sendGetCommand(command);

    return new JSONArray(checkResponse(command, response));
  }

  /**
   * Reads a single value form a field that must be readable by everyone. Does
   * not require a signature.
   *
   * @param guid
   * @param field
   * @return
   * @throws Exception
   */
  public String readField(String guid, String field) throws Exception {
    String command = createQuery(GnsProtocol.READ_ARRAY_ONE, GnsProtocol.GUID, guid, GnsProtocol.FIELD, field);
    String response = sendGetCommand(command);

    return checkResponse(command, response);
  }

  /**
   * Reads all the values value for a key from the GNS server for the given
   * guid. Does not require signature, but the field must be readable by
 everyone (ie., have ALL_USERS as the read ACL.
   *
   * @param guid
   * @param field
   * @return a JSONArray containing the values in the field
   * @throws Exception
   */
  public JSONArray readFieldList(String guid, String field) throws Exception {
    String command = createQuery(GnsProtocol.READ_ARRAY, GnsProtocol.GUID, guid, GnsProtocol.FIELD, field);
    String response = sendGetCommand(command);

    return new JSONArray(checkResponse(command, response));
  }

  //
  // SELECT
  //
  /**
   * Returns all GUIDs that have a field that contains the given value as a
   * JSONArray containing JSONObjects. The JSONObjects have a "GUID" field in
   * addition to all the other fields. Keep in mind that all values are lists so
   * yes it is "contains". Also note that the GNS currently does not enforce any
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
   * If field is a GeoSpatial field queries the GNS server return all fields
   * that are within value which is a bounding box specified as a nested
   * JSONArrays of paired tuples: [[LONG_UL, LAT_UL],[LONG_BR, LAT_BR]]
   *
   * @param field
   * @param value - [[LONG_UL, LAT_UL],[LONG_BR, LAT_BR]]
   * @return a JSONArray containing all the matched records as JSONObjects
   * @throws Exception
   */
  public JSONArray selectWithin(String field, JSONArray value) throws Exception {
    String command = createQuery(GnsProtocol.SELECT, GnsProtocol.FIELD, field, GnsProtocol.WITHIN, value.toString());
    String response = sendGetCommand(command);

    return new JSONArray(checkResponse(command, response));
  }

  /**
   * If field is a GeoSpatial field queries the GNS server and returns all
   * fields that are near value which is a point specified as a two element
   * JSONArray: [LONG, LAT]. Max Distance is in meters.
   *
   * @param field
   * @param value - [LONG, LAT]
   * @param maxDistance - distance in meters
   * @return a JSONArray containing all the matched records as JSONObjects
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

  public JSONArray selectSetupGroupQuery(String guid, String query) throws Exception {
    String command = createQuery(GnsProtocol.SELECT_GROUP, GnsProtocol.GUID, guid, GnsProtocol.QUERY, query);
    // System.out.println(command);
    String response = sendGetCommand(command);

    return new JSONArray(checkResponse(command, response));
  }

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
    replaceOrCreateUsingList(guid, GnsProtocol.LOCATION_FIELD_NAME, array);
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
    return readFieldList(targetGuid, GnsProtocol.LOCATION_FIELD_NAME, readerGuid);
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
    String command = createAndSignQuery(guid, GnsProtocol.ADD_ALIAS, GnsProtocol.GUID, guid.getGuid(),
            GnsProtocol.NAME, name);
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
    String command = createAndSignQuery(guid, GnsProtocol.REMOVE_ALIAS, GnsProtocol.GUID, guid.getGuid(),
            GnsProtocol.NAME, name);
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
    String command = createAndSignQuery(guid, GnsProtocol.RETRIEVE_ALIASES, GnsProtocol.GUID, guid.getGuid());

    String response = sendGetCommand(command);
    // System.out.println("ALIASES: |" + response + "|");
    try {
      return new JSONArray(checkResponse(command, response));
    } catch (JSONException e) {
      throw new GnsException("Invalid alias list", e);
    }
  }

  /**
   * Creates a tag to the tags of the guid.
   *
   * @param guid
   * @param tag
   * @throws Exception
   */
  public void addTag(GuidEntry guid, String tag) throws Exception {
    String command = createAndSignQuery(guid, GnsProtocol.ADD_TAG, GnsProtocol.GUID, guid.getGuid(), GnsProtocol.NAME,
            tag);
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Removes a tag from the tags of the guid.
   *
   * @param guid
   * @param tag
   * @throws Exception
   */
  public void removeTag(GuidEntry guid, String tag) throws Exception {
    String command = createAndSignQuery(guid, GnsProtocol.REMOVE_TAG, GnsProtocol.GUID, guid.getGuid(),
            GnsProtocol.NAME, tag);
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Retrieves GUIDs that have been tagged with tag
   *
   * @param tag
   * @return
   * @throws Exception
   */
  public JSONArray retrieveTagged(String tag) throws Exception {
    String command = createQuery(GnsProtocol.DUMP, GnsProtocol.NAME, tag);
    String response = sendGetCommand(command);

    return new JSONArray(checkResponse(command, response));
  }

  /**
   * Removes all guids that have the corresponding tag. Removes the reverse
   * fields for the entity name and aliases. Note: doesn't remove all the
   * associated fields yet, though, so still a work in progress.
   *
   * @param tag
   * @throws Exception
   */
  public void clearTagged(String tag) throws Exception {
    String command = createQuery(GnsProtocol.CLEAR_TAGGED, GnsProtocol.NAME, tag);
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * TODO: resetDB to be removed for prod version.
   *
   * @throws Exception
   */
  public void resetDB() throws Exception {
    String command = createQuery(GnsProtocol.RESET_DATABASE);
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  private String checkResponse(String command, String response) throws GnsException {
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
                || error.startsWith(GnsProtocol.DUPLICATE_GUID)) {
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
  private String createQuery(String action, String... keysAndValues) throws IOException {
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
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   */
  public String createAndSignQuery(GuidEntry guid, String action, String... keysAndValues) throws IOException,
          InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    String key;
    String value;
    StringBuilder encodedString = new StringBuilder(action + QUERYPREFIX);
    StringBuilder unencodedString = new StringBuilder(action + QUERYPREFIX);

    // map over the leys and values to produce the query
    for (int i = 0; i < keysAndValues.length; i = i + 2) {
      key = keysAndValues[i];
      value = keysAndValues[i + 1];
      encodedString.append(URIEncoderDecoder.quoteIllegal(key, "") + VALSEP + URIEncoderDecoder.quoteIllegal(value, "")
              + (i + 2 < keysAndValues.length ? KEYSEP : ""));
      unencodedString.append(key + VALSEP + value + (i + 2 < keysAndValues.length ? KEYSEP : ""));
    }

    // generate the signature from the unencoded query
    String signature = signDigestOfMessage(guid, unencodedString.toString());
    // return the encoded query with the signature appended
    return encodedString.toString() + KEYSEP + GnsProtocol.SIGNATURE + VALSEP + signature;
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

  /**
   * Check that the connectivity with the host:port can be established
   *
   * @throws IOException throws exception if a communication error occurs
   */
  public abstract void checkConnectivity() throws IOException;

  /**
   * Sends a HTTP get with given queryString to the host specified by the
   * {@link host} field.
   *
   * @param queryString
   * @return result of get as a string
   * @throws IOException if an error occurs
   */
  protected abstract String sendGetCommand(String queryString) throws IOException;
}
