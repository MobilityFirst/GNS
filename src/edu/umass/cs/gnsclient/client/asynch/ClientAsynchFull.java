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
package edu.umass.cs.gnsclient.client.asynch;

import edu.umass.cs.gnsclient.client.GuidEntry;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import org.json.JSONArray;
import java.util.ArrayList;
import org.json.JSONException;
import org.json.JSONObject;
import static edu.umass.cs.gnscommon.GnsProtocol.*;
import edu.umass.cs.gnsclient.exceptions.GnsException;
import edu.umass.cs.gnsclient.exceptions.GnsFieldNotFoundException;
import edu.umass.cs.gnsclient.exceptions.GnsInvalidGuidException;
import java.util.Arrays;
import java.util.HashSet;

/**
 * This class defines a basic client to communicate with a GNS instance over TCP. This
 * class contains a concise set of read and write commands which read and write JSON Objects.
 * It also contains the basic field read and write commands as well a
 * set of commands to use context aware group guids.
 *
 * For a more complete set of commands see also {@link UniversalTcpClient} and {@link UniversalTcpClientExtended}.
 *
 * @author <a href="mailto:westy@cs.umass.edu">Westy</a>
 */
public class ClientAsynchFull extends ClientAsynchBase {

  /**
   * Creates a new <code>NewBasicUniversalTcpClient</code> object
   *
   * @param address
   * @throws java.io.IOException
   */
  public ClientAsynchFull(InetSocketAddress address) throws IOException {
    super(new HashSet<>(Arrays.asList(address)), false);
  }

  /**
   * @throws IOException
   */
  public ClientAsynchFull() throws IOException {
    super();
  }

  // READ AND WRITE COMMANDS
  /**
   * Updates the JSONObject associated with targetGuid using the given JSONObject.
   * Top-level fields not specified in the given JSONObject are not modified.
   * The writer is the guid of the user attempting access. Signs the query using
   * the private key of the user associated with the writer guid.
   *
   * @param targetGuid
   * @param json
   * @param writer
   * @throws IOException
   * @throws GnsException
   */
  public void update(String targetGuid, JSONObject json, GuidEntry writer) throws IOException, GnsException {
    JSONObject command = createAndSignCommand(writer.getPrivateKey(), REPLACE_USER_JSON, GUID,
            targetGuid, USER_JSON, json.toString(), WRITER, writer.getGuid());
    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  /**
   * Replaces the JSON in guid with JSONObject. Signs the query using
   * the private key of the given guid.
   *
   * @param guid
   * @param json
   * @throws IOException
   * @throws GnsException
   */
  public void update(GuidEntry guid, JSONObject json) throws IOException, GnsException {
    ClientAsynchFull.this.update(guid.getGuid(), json, guid);
  }

  /**
   * Updates the field in the targetGuid. The writer is the guid
   * of the user attempting access. Signs the query using
   * the private key of the writer guid.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws IOException
   * @throws GnsException
   * @throws JSONException
   */
  public void fieldUpdate(String targetGuid, String field, Object value, GuidEntry writer) throws IOException, GnsException, JSONException {
    JSONObject json = new JSONObject();
    json.put(field, value);
    JSONObject command = createAndSignCommand(writer.getPrivateKey(), REPLACE_USER_JSON, GUID,
            targetGuid, USER_JSON, json.toString(), WRITER, writer.getGuid());
    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  /**
   * Updates the field in the targetGuid.
   * Signs the query using the private key of the given guid.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @throws IOException
   * @throws GnsException
   * @throws JSONException
   */
  public void fieldUpdate(GuidEntry targetGuid, String field, Object value) throws IOException, GnsException, JSONException {
    fieldUpdate(targetGuid.getGuid(), field, value, targetGuid);
  }

  /**
   * Reads the JSONObject for the given targetGuid.
   * The reader is the guid of the user attempting access. Signs the query using
   * the private key of the user associated with the reader guid (unsigned if
   * reader is null).
   *
   * @param targetGuid
   * @param reader if null guid must be all fields readable for all users
   * @return a JSONObject
   * @throws Exception
   */
  public JSONObject read(String targetGuid, GuidEntry reader) throws Exception {
    JSONObject command;
    if (reader == null) {
      // this one actually uses the old style read... for now
      command = createCommand(READ_ARRAY, GUID, targetGuid, FIELD, ALL_FIELDS);
    } else {
      // this one actually uses the old style read... for now
      command = createAndSignCommand(reader.getPrivateKey(), READ_ARRAY, GUID, targetGuid, FIELD, ALL_FIELDS,
              READER, reader.getGuid());
    }

    String response = sendCommandAndWait(command);

    return new JSONObject(checkResponse(command, response));
  }

  /**
   * Reads the entire record from the GNS server for the given guid.
   * Signs the query using the private key of the guid.
   *
   * @param guid
   * @return a JSONArray containing the values in the field
   * @throws Exception
   */
  public JSONObject read(GuidEntry guid) throws Exception {
    return read(guid.getGuid(), guid);
  }

  /**
   * Returns true if the field exists in the given targetGuid.
   * Field is a string the naming the field. Field can use dot
   * notation to indicate subfields. The reader is the guid of the
   * user attempting access. This method signs the query using the
   * private key of the user associated with the reader guid (unsigned if
   * reader is null).
   *
   * @param targetGuid
   * @param field
   * @param reader if null the field must be readable for all
   * @return a boolean indicating if the field exists
   * @throws Exception
   */
  public boolean fieldExists(String targetGuid, String field, GuidEntry reader) throws Exception {
    JSONObject command;
    if (reader == null) {
      command = createCommand(READ, GUID, targetGuid, FIELD, field);
    } else {
      command = createAndSignCommand(reader.getPrivateKey(), READ, GUID, targetGuid, FIELD, field,
              READER, reader.getGuid());
    }
    String response = sendCommandAndWait(command);
    try {
      checkResponse(command, response);
      return true;
    } catch (GnsFieldNotFoundException e) {
      return false;
    }
  }

  /**
   * Returns true if the field exists in the given targetGuid.
   * Field is a string the naming the field. Field can use dot
   * notation to indicate subfields. This method signs the query using the
   * private key of the targetGuid.
   *
   * @param targetGuid
   * @param field
   * @return a boolean indicating if the field exists
   * @throws Exception
   */
  public boolean fieldExists(GuidEntry targetGuid, String field) throws Exception {
    return fieldExists(targetGuid.getGuid(), field, targetGuid);
  }

  /**
   * Reads the value of field for the given targetGuid.
   * Field is a string the naming the field. Field can use dot
   * notation to indicate subfields. The reader is the guid of the
   * user attempting access. This method signs the query using the
   * private key of the user associated with the reader guid (unsigned if
   * reader is null).
   *
   * @param targetGuid
   * @param field
   * @param reader if null the field must be readable for all
   * @return a string containing the values in the field
   * @throws Exception
   */
  public String fieldRead(String targetGuid, String field, GuidEntry reader) throws Exception {
    JSONObject command;
    if (reader == null) {
      command = createCommand(READ, GUID, targetGuid, FIELD, field);
    } else {
      command = createAndSignCommand(reader.getPrivateKey(), READ, GUID, targetGuid, FIELD, field,
              READER, reader.getGuid());
    }

    String response = sendCommandAndWait(command);

    return checkResponse(command, response);
  }

  /**
   * Reads the value of field from the targetGuid.
   * Field is a string the naming the field. Field can use dot
   * notation to indicate subfields. This method signs the query using the
   * private key of the targetGuid.
   *
   * @param targetGuid
   * @param field
   * @return
   * @throws Exception
   */
  public String fieldRead(GuidEntry targetGuid, String field) throws Exception {
    return fieldRead(targetGuid.getGuid(), field, targetGuid);
  }

  /**
   * Reads the value of fields for the given targetGuid.
   * Fields is a list of strings the naming the field. Fields can use dot
   * notation to indicate subfields. The reader is the guid of the
   * user attempting access. This method signs the query using the
   * private key of the user associated with the reader guid (unsigned if
   * reader is null).
   *
   * @param targetGuid
   * @param fields
   * @param reader if null the field must be readable for all
   * @return a JSONArray containing the values in the fields
   * @throws Exception
   */
  public String fieldRead(String targetGuid, ArrayList<String> fields, GuidEntry reader) throws Exception {
    JSONObject command;
    if (reader == null) {
      command = createCommand(READ, GUID, targetGuid, FIELDS, fields);
    } else {
      command = createAndSignCommand(reader.getPrivateKey(), READ, GUID, targetGuid,
              FIELDS, fields,
              READER, reader.getGuid());
    }

    String response = sendCommandAndWait(command);

    return checkResponse(command, response);
  }

  /**
   * Reads the value of fields for the given guid.
   * Fields is a list of strings the naming the field. Fields can use dot
   * notation to indicate subfields. This method signs the query using the
   * private key of the guid.
   *
   * @param targetGuid
   * @param fields
   * @return
   * @throws Exception
   */
  public String fieldRead(GuidEntry targetGuid, ArrayList<String> fields) throws Exception {
    return fieldRead(targetGuid.getGuid(), fields, targetGuid);
  }

  /**
   * Removes a field in the JSONObject record of the given targetGuid.
   * The writer is the guid of the user attempting access.
   * Signs the query using the private key of the user
   * associated with the writer guid.
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
    JSONObject command = createAndSignCommand(writer.getPrivateKey(), REMOVE_FIELD, GUID, targetGuid,
            FIELD, field, WRITER, writer.getGuid());
    String response = sendCommandAndWait(command);
    checkResponse(command, response);
  }

  // SELECT COMMANDS
  /**
   * Selects all records that match query.
   * Returns the result of the query as a JSONArray of guids.
   *
   * The query syntax is described here:
   * https://gns.name/wiki/index.php?title=Query_Syntax
   *
   * Currently there are two predefined field names in the GNS client (this is in edu.umass.cs.gnsclient.client.GnsProtocol):
   * LOCATION_FIELD_NAME = "geoLocation"; Defined as a "2d" index in the database.
   * IPADDRESS_FIELD_NAME = "netAddress";
   *
   * There are links in the wiki page abive to find the exact syntax for querying spacial coordinates.
   *
   * @param query - the query
   * @return - a JSONArray of guids
   * @throws Exception
   */
  public JSONArray selectQuery(String query) throws Exception {
    JSONObject command = createCommand(SELECT, QUERY, query);
    String response = sendCommandAndWait(command);

    return new JSONArray(checkResponse(command, response));
  }

  /**
   * Set up a context aware group guid using a query.
   * Requires a accountGuid and a publicKey which are used to set up the new guid
   * or look it up if it already exists.
   *
   * Also returns the result of the query as a JSONArray of guids.
   *
   * The query syntax is described here:
   * https://gns.name/wiki/index.php?title=Query_Syntax
   *
   * @param accountGuid
   * @param publicKey
   * @param query the query
   * @param interval - the refresh interval in seconds - default is 60 - (queries that happens quicker than
   * this will get stale results)
   * @return a JSONArray of guids
   * @throws Exception
   */
  public JSONArray selectSetupGroupQuery(GuidEntry accountGuid, String publicKey, String query, int interval) throws Exception {
    JSONObject command = createCommand(SELECT_GROUP, ACCOUNT_GUID, accountGuid.getGuid(),
            PUBLIC_KEY, publicKey, QUERY, query,
            INTERVAL, interval);
    String response = sendCommandAndWait(command);

    return new JSONArray(checkResponse(command, response));
  }

  /**
   * Look up the value of a context aware group guid using a query.
   * Returns the result of the query as a JSONArray of guids. The results will be
   * stale if the queries that happen more quickly than the refresh interval given during setup.
   *
   * @param guid
   * @return a JSONArray of guids
   * @throws Exception
   */
  public JSONArray selectLookupGroupQuery(String guid) throws Exception {
    JSONObject command = createCommand(SELECT_GROUP, GUID, guid);
    String response = sendCommandAndWait(command);

    return new JSONArray(checkResponse(command, response));
  }

  // GROUP COMMANDS
  /**
   * Return the list of guids that are members of the group. Signs the query
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
    JSONObject command = createAndSignCommand(reader.getPrivateKey(), GET_GROUP_MEMBERS, GUID, groupGuid,
            READER, reader.getGuid());
    String response = sendCommandAndWait(command);

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
   * @param guid the guid we are looking for
   * @param reader the guid of the entity doing the lookup
   * @return the list of groups as a JSONArray
   * @throws IOException if a communication error occurs
   * @throws GnsException if a protocol error occurs or the list cannot be
   * parsed
   * @throws GnsInvalidGuidException if the group guid is invalid
   */
  public JSONArray guidGetGroups(String guid, GuidEntry reader) throws IOException, GnsException,
          GnsInvalidGuidException {
    JSONObject command = createAndSignCommand(reader.getPrivateKey(), GET_GROUPS, GUID, guid,
            READER, reader.getGuid());
    String response = sendCommandAndWait(command);

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
    JSONObject command = createAndSignCommand(writer.getPrivateKey(), ADD_TO_GROUP, GUID, groupGuid,
            MEMBER, guidToAdd, WRITER, writer.getGuid());
    String response = sendCommandAndWait(command);

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
    JSONObject command = createAndSignCommand(writer.getPrivateKey(), ADD_TO_GROUP, GUID, groupGuid,
            MEMBERS, members.toString(), WRITER, writer.getGuid());
    String response = sendCommandAndWait(command);

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
    JSONObject command = createAndSignCommand(writer.getPrivateKey(), REMOVE_FROM_GROUP, GUID, guid,
            MEMBER, guidToRemove, WRITER, writer.getGuid());

    String response = sendCommandAndWait(command);

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
    JSONObject command = createAndSignCommand(writer.getPrivateKey(), REMOVE_FROM_GROUP, GUID, guid,
            MEMBERS, members.toString(), WRITER, writer.getGuid());

    String response = sendCommandAndWait(command);

    checkResponse(command, response);
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
    aclAdd(AccessType.WRITE_WHITELIST, groupGuid, GROUP_ACL, guidToAuthorize);
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
    aclRemove(AccessType.WRITE_WHITELIST, groupGuid, GROUP_ACL, guidToUnauthorize);
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
    aclAdd(AccessType.READ_WHITELIST, groupGuid, GROUP_ACL, guidToAuthorize);
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
    aclRemove(AccessType.READ_WHITELIST, groupGuid, GROUP_ACL, guidToUnauthorize);
  }

  // ACL COMMANDS
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
  public void aclAdd(AccessType accessType, GuidEntry targetGuid, String field, String accesserGuid)
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
  public void aclRemove(AccessType accessType, GuidEntry guid, String field, String accesserGuid)
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
  public JSONArray aclGet(AccessType accessType, GuidEntry guid, String field, String accesserGuid)
          throws Exception {
    return aclGet(accessType.name(), guid, field, accesserGuid);
  }

  // ALIASES
  /**
   * Creates an alias entity name for the given guid. The alias can be used just
   * like the original entity name.
   *
   * @param guid
   * @param name - the alias
   * @throws Exception
   */
  public void addAlias(GuidEntry guid, String name) throws Exception {
    JSONObject command = createAndSignCommand(guid.getPrivateKey(), ADD_ALIAS, GUID, guid.getGuid(),
            NAME, name);
    String response = sendCommandAndWait(command);

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
    JSONObject command = createAndSignCommand(guid.getPrivateKey(), REMOVE_ALIAS, GUID, guid.getGuid(),
            NAME, name);
    String response = sendCommandAndWait(command);

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
    JSONObject command = createAndSignCommand(guid.getPrivateKey(), RETRIEVE_ALIASES, GUID, guid.getGuid());

    String response = sendCommandAndWait(command);
    try {
      return new JSONArray(checkResponse(command, response));
    } catch (JSONException e) {
      throw new GnsException("Invalid alias list", e);
    }
  }

  // ///////////////////////////////
  // // PRIVATE METHODS BELOW /////
  // /////////////////////////////
  private void aclAdd(String accessType, GuidEntry guid, String field, String accesserGuid) throws Exception {
    JSONObject command = createAndSignCommand(guid.getPrivateKey(), ACL_ADD, ACL_TYPE, accessType,
            GUID, guid.getGuid(), FIELD, field, ACCESSER, accesserGuid == null
                    ? ALL_USERS
                    : accesserGuid);
    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  private void aclRemove(String accessType, GuidEntry guid, String field, String accesserGuid) throws Exception {
    JSONObject command = createAndSignCommand(guid.getPrivateKey(), ACL_REMOVE, ACL_TYPE, accessType,
            GUID, guid.getGuid(), FIELD, field, ACCESSER, accesserGuid == null
                    ? ALL_USERS
                    : accesserGuid);
    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  private JSONArray aclGet(String accessType, GuidEntry guid, String field, String accesserGuid) throws Exception {
    JSONObject command = createAndSignCommand(guid.getPrivateKey(), ACL_RETRIEVE, ACL_TYPE, accessType,
            GUID, guid.getGuid(), FIELD, field, ACCESSER, accesserGuid == null
                    ? ALL_USERS
                    : accesserGuid);
    String response = sendCommandAndWait(command);
    try {
      return new JSONArray(checkResponse(command, response));
    } catch (JSONException e) {
      throw new GnsException("Invalid ACL list", e);
    }
  }

}
