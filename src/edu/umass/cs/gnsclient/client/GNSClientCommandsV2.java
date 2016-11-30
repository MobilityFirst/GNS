/* Copyright (c) 2016 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 */
package edu.umass.cs.gnsclient.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnscommon.exceptions.client.EncryptionException;
import org.json.JSONArray;
import org.json.JSONObject;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.client.DuplicateNameException;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnsserver.main.GNSConfig;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.Set;
import org.json.JSONException;
import edu.umass.cs.gnscommon.GNSProtocol;

/**
 
 * This class defines a client to communicate with a GNS instance over TCP. This
 * class adds single field list based commands to the {@link GNSClient}
 * 's JSONObject based commands.
 * 
 * This is an improvement over GNSClientCommands that simply executes GNSCommands.
 * Testing is ongoing.
 *
 * This class contains a concise subset of all available server operations.
 *
 * @author westy
 */
public class GNSClientCommandsV2 extends GNSClient {

  /**
   * @throws IOException
   */
  public GNSClientCommandsV2() throws IOException {
    super((InetSocketAddress) null);
  }

  /**
   * Creates a new {@link GNSClient} object
   *
   * @param anyReconfigurator
   * @throws java.io.IOException
   */
  public GNSClientCommandsV2(InetSocketAddress anyReconfigurator)
          throws IOException {
    super(anyReconfigurator);
  }

  // READ AND WRITE COMMANDS
  /**
   * Updates the JSONObject associated with targetGuid using the given
   * JSONObject. Top-level fields not specified in the given JSONObject are
   * not modified. The writer is the guid of the user attempting access. Signs
   * the query using the private key of the user associated with the writer
   * guid.
   *
   * @param targetGuid
   * @param json
   * @param writer
   * @throws IOException
   * @throws ClientException
   */
  public void update(String targetGuid, JSONObject json, GuidEntry writer)
          throws IOException, ClientException {
    execute(GNSCommand.update(targetGuid, json, writer));
//    getResponse(CommandType.ReplaceUserJSON, writer, GNSProtocol.GUID.toString(), targetGuid,
//            GNSProtocol.USER_JSON.toString(), json, GNSProtocol.WRITER.toString(), writer.getGuid());
  }

  /**
   * Replaces the JSON in guid with JSONObject. Signs the query using the
   * private key of the given guid.
   *
   * @param guid
   * @param json
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void update(GuidEntry guid, JSONObject json) throws IOException,
          ClientException {
    update(guid.getGuid(), json, guid);
  }

  /**
   * Updates the field in the targetGuid. The writer is the guid of the user
   * attempting access. Signs the query using the private key of the writer
   * guid.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void fieldUpdate(String targetGuid, String field, Object value,
          GuidEntry writer) throws IOException, ClientException {
    execute(GNSCommand.fieldUpdate(targetGuid, field, value, writer));
//    getResponse(CommandType.ReplaceUserJSON, writer, GNSProtocol.GUID.toString(), targetGuid,
//            GNSProtocol.USER_JSON.toString(), new JSONObject().put(field, value), GNSProtocol.WRITER.toString(), writer.getGuid());
  }

  /**
   * Creates an index for a field. The guid is only used for authentication
   * purposes.
   *
   * @param guid
   * @param field
   * @param index
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void fieldCreateIndex(GuidEntry guid, String field, String index)
          throws IOException, ClientException {
    execute(GNSCommand.fieldCreateIndex(guid, field, index));
//    getResponse(CommandType.CreateIndex, guid, GNSProtocol.GUID.toString(), guid.getGuid(), GNSProtocol.FIELD.toString(),
//            field, GNSProtocol.VALUE.toString(), index, GNSProtocol.WRITER.toString(), guid.getGuid());
  }

  /**
   * Updates the field in the targetGuid. Signs the query using the private
   * key of the given guid.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void fieldUpdate(GuidEntry targetGuid, String field, Object value)
          throws IOException, ClientException {
    fieldUpdate(targetGuid.getGuid(), field, value, targetGuid);
  }

  /**
   * Reads the JSONObject for the given targetGuid. The reader is the guid of
   * the user attempting access. Signs the query using the private key of the
   * user associated with the reader guid (unsigned if reader is null).
   *
   * @param targetGuid
   * @param reader
   * if null guid must be all fields readable for all users
   * @return a JSONObject
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public JSONObject read(String targetGuid, GuidEntry reader)
          throws ClientException, IOException {
    return execute(GNSCommand.read(targetGuid, reader)).getResultJSONObject();
//    return new JSONObject(getResponse(reader != null ? CommandType.ReadArray
//            : CommandType.ReadArrayUnsigned, reader, GNSProtocol.GUID.toString(),
//            targetGuid, GNSProtocol.FIELD.toString(), GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.READER.toString(),
//            reader != null ? reader.getGuid() : null));
  }

  /**
   * Reads the entire record from the GNS server for the given guid. Signs the
   * query using the private key of the guid.
   *
   * @param guid
   * @return a JSONObject containing the values in the field
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public JSONObject read(GuidEntry guid) throws ClientException, IOException {
    return read(guid.getGuid(), guid);
  }

  /**
   * Returns true if the field exists in the given targetGuid. Field is a
   * string the naming the field. Field can use dot notation to indicate
   * subfields. The reader is the guid of the user attempting access. This
   * method signs the query using the private key of the user associated with
   * the reader guid (unsigned if reader is null).
   *
   * @param targetGuid
   * @param field
   * @param reader
   * if null the field must be readable for all
   * @return a boolean indicating if the field exists
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public boolean fieldExists(String targetGuid, String field, GuidEntry reader)
          throws ClientException, IOException {
    return execute(GNSCommand.fieldExists(targetGuid, field, reader)).getResultBoolean();
//    try {
//      if (reader != null) {
//        getResponse(CommandType.Read, reader,
//                GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(), field,
//                GNSProtocol.READER.toString(), reader.getGuid());
//      } else {
//        getResponse(CommandType.ReadUnsigned, reader,
//                GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(), field,
//                GNSProtocol.READER.toString(), null);
//      }
//
//      return true;
//    } catch (FieldNotFoundException e) {
//      return false;
//    }
  }

  /**
   * Returns true if the field exists in the given targetGuid. Field is a
   * string the naming the field. Field can use dot notation to indicate
   * subfields. This method signs the query using the private key of the
   * targetGuid.
   *
   * @param targetGuid
   * @param field
   * @return a boolean indicating if the field exists
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public boolean fieldExists(GuidEntry targetGuid, String field)
          throws ClientException, IOException {
    return fieldExists(targetGuid.getGuid(), field, targetGuid);
  }

  /**
   * Reads the value of field for the given targetGuid. Field is a string the
   * naming the field. Field can use dot notation to indicate subfields. The
   * reader is the guid of the user attempting access. This method signs the
   * query using the private key of the user associated with the reader guid
   * (unsigned if reader is null).
   *
   * @param targetGuid
   * @param field
   * @param reader
   * if null the field must be readable for all
   * @return a string containing the values in the field
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public String fieldRead(String targetGuid, String field, GuidEntry reader)
          throws ClientException, IOException {
    JSONObject result = execute(GNSCommand.fieldRead(targetGuid, field, reader)).getResultJSONObject();
    if (GNSProtocol.ENTIRE_RECORD.toString().equals(field)) {
      return result.toString();
    } else {
      try {
        return result.getString(field);
      } catch (JSONException e) {
        throw new ClientException(e);
      }
    }
//    if (reader != null) {
//      return CommandUtils.specialCaseSingleField(getResponse(CommandType.Read, reader,
//              GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(), field, 
//              GNSProtocol.READER.toString(), reader.getGuid()));
//    } else {
//      return CommandUtils.specialCaseSingleField(getResponse(CommandType.ReadUnsigned, reader,
//              GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(), field, 
//              GNSProtocol.READER.toString(), null));
//    }
  }

  /**
   * Reads the value of field from the targetGuid. Field is a string the
   * naming the field. Field can use dot notation to indicate subfields. This
   * method signs the query using the private key of the targetGuid.
   *
   * @param targetGuid
   * @param field
   * @return field value
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public String fieldRead(GuidEntry targetGuid, String field)
          throws ClientException, IOException {
    return fieldRead(targetGuid.getGuid(), field, targetGuid);
  }

  /**
   * Reads the value of fields for the given targetGuid. Fields is a list of
   * strings the naming the field. Fields can use dot notation to indicate
   * subfields. The reader is the guid of the user attempting access. This
   * method signs the query using the private key of the user associated with
   * the reader guid (unsigned if reader is null).
   *
   * @param targetGuid
   * @param fields
   * @param reader
   * if null the field must be readable for all
   * @return a JSONObject containing the values in the fields
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public String fieldRead(String targetGuid, ArrayList<String> fields,
          GuidEntry reader) throws ClientException, IOException {
    return execute(GNSCommand.fieldRead(targetGuid, fields, reader)).getResultString();
//    return getResponse(reader != null ? CommandType.ReadMultiField
//            : CommandType.ReadMultiFieldUnsigned, reader, GNSProtocol.GUID.toString(), targetGuid,
//            GNSProtocol.FIELDS.toString(), fields, GNSProtocol.READER.toString(), reader != null ? reader.getGuid()
//                    : null);
  }

  /**
   * Reads the value of fields for the given guid. Fields is a list of strings
   * the naming the field. Fields can use dot notation to indicate subfields.
   * This method signs the query using the private key of the guid.
   *
   * @param targetGuid
   * @param fields
   * @return values of fields
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public String fieldRead(GuidEntry targetGuid, ArrayList<String> fields)
          throws ClientException, IOException {
    return fieldRead(targetGuid.getGuid(), fields, targetGuid);
  }

  /**
   * Removes a field in the JSONObject record of the given targetGuid. The
   * writer is the guid of the user attempting access. Signs the query using
   * the private key of the user associated with the writer guid.
   *
   * @param targetGuid
   * @param field
   * @param writer
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void fieldRemove(String targetGuid, String field, GuidEntry writer)
          throws IOException, ClientException {
    execute(GNSCommand.fieldRemove(targetGuid, field, writer));
//    getResponse(CommandType.RemoveField, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(),
//            field, GNSProtocol.WRITER.toString(), writer.getGuid());
  }

  // SELECT COMMANDS
  /**
   * Selects all records that match query. Returns the result of the query as
   * a JSONArray of guids.
   *
   * The query syntax is described here:
   * https://gns.name/wiki/index.php?title=Query_Syntax
   *
   * Currently there are two predefined field names in the GNS client (this is
   * in edu.umass.cs.gnsclient.client.GNSCommandProtocol): GNSProtocol.LOCATION_FIELD_NAME.toString()
   * = "geoLocation"; Defined as a "2d" index in the database.
   * GNSProtocol.IPADDRESS_FIELD_NAME.toString() = "netAddress";
   *
   * There are links in the wiki page above to find the exact syntax for
   * querying spacial coordinates.
   *
   * @param query
   * - the query
   * @return - a JSONArray of guids
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public JSONArray selectQuery(String query) throws ClientException, IOException {
    return execute(GNSCommand.selectQuery(query)).getResultJSONArray();
    //return new JSONArray(getResponse(CommandType.SelectQuery, GNSProtocol.QUERY.toString(), query));
  }

  /**
   * Set up a context aware group guid using a query. Requires a accountGuid
   * and a publicKey which are used to set up the new guid or look it up if it
   * already exists.
   *
   * Also returns the result of the query as a JSONArray of guids.
   *
   * The query syntax is described here:
   * https://gns.name/wiki/index.php?title=Query_Syntax
   *
   * @param accountGuid
   * @param publicKey
   * @param query
   * the query
   * @param interval
   * - the refresh interval in seconds - default is 60 - (queries
   * that happens quicker than this will get stale results)
   * @return a JSONArray of guids
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public JSONArray selectSetupGroupQuery(GuidEntry accountGuid,
          String publicKey, String query, int interval) throws ClientException, IOException {
    return execute(GNSCommand.selectSetupGroupQuery(accountGuid, publicKey, query, interval)).getResultJSONArray();
//    return new JSONArray(getResponse(CommandType.SelectGroupSetupQuery, GNSProtocol.GUID.toString(), accountGuid.getGuid(),
//            GNSProtocol.PUBLIC_KEY.toString(), publicKey, GNSProtocol.QUERY.toString(), query, GNSProtocol.INTERVAL.toString(), interval));
  }

  /**
   * Look up the value of a context aware group guid using a query. Returns
   * the result of the query as a JSONArray of guids. The results will be
   * stale if the queries that happen more quickly than the refresh interval
   * given during setup.
   *
   * @param guid
   * @return a JSONArray of guids
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public JSONArray selectLookupGroupQuery(String guid) throws ClientException, IOException {
    return execute(GNSCommand.selectLookupGroupQuery(guid)).getResultJSONArray();
    //return new JSONArray(getResponse(CommandType.SelectGroupLookupQuery, GNSProtocol.GUID.toString(), guid));
  }

  // ACCOUNT COMMANDS
  /**
   * Obtains the guid of the alias from the GNS server.
   *
   * @param alias
   * @return guid
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public String lookupGuid(String alias) throws IOException, ClientException {
    return execute(GNSCommand.lookupGUID(alias)).getResultString();
    //return CommandUtils.specialCaseSingleField(getResponse(CommandType.LookupGuid, GNSProtocol.NAME.toString(), alias));
  }

  /**
   * If this is a sub guid returns the account guid it was created under.
   *
   * @param guid
   * @return Account guid of {@code guid}
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public String lookupPrimaryGuid(String guid)
          throws IOException, ClientException {
    return execute(GNSCommand.lookupPrimaryGUID(guid)).getResultString();
//    return CommandUtils.specialCaseSingleField(getResponse(CommandType.LookupPrimaryGuid, GNSProtocol.GUID.toString(),
//            guid));
  }

  /**
   * Returns a JSON object containing all of the guid meta information. This
   * method returns meta data about the guid. If you want any particular field
   * or fields of the guid you'll need to use one of the read methods.
   *
   * @param guid
   * @return {@code guid} meta info
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public JSONObject lookupGuidRecord(String guid) throws IOException,
          ClientException {
    return execute(GNSCommand.lookupGUIDRecord(guid)).getResultJSONObject();
//    try {
//      return new JSONObject(getResponse(CommandType.LookupGuidRecord, GNSProtocol.GUID.toString(), guid));
//    } catch (JSONException e) {
//      throw new ClientException(
//              "Failed to parse LOOKUP_GUID_RECORD response", e);
//    }
  }

  /**
   * Returns a JSON object containing all of the account meta information for
   * an account guid. This method returns meta data about the account
   * associated with this guid if and only if the guid is an account guid. If
   * you want any particular field or fields of the guid you'll need to use
   * one of the read methods.
   *
   * @param accountGuid
   * @return accountGUID meta info
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public JSONObject lookupAccountRecord(String accountGuid)
          throws IOException, ClientException {
    return execute(GNSCommand.lookupAccountRecord(accountGuid)).getResultJSONObject();
//    try {
//      return new JSONObject(getResponse(CommandType.LookupAccountRecord, GNSProtocol.GUID.toString(), accountGuid));
//    } catch (JSONException e) {
//      throw new ClientException("Failed to parse LOOKUP_ACCOUNT_RECORD response", e);
//    }
  }

  /**
   * Get the public key for a given alias.
   *
   * @param alias
   * @return the public key registered for the alias
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public PublicKey publicKeyLookupFromAlias(String alias)
          throws ClientException, IOException {
    return publicKeyLookupFromGuid(lookupGuid(alias));
  }

  /**
   * Get the public key for a given guid.
   *
   * @param guid
   * @return Public key for {@code guid}
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  // FYI: publicKeyLookupFromGUID is implemented wrong in GNSCommand
  public PublicKey publicKeyLookupFromGuid(String guid)
          throws ClientException, IOException {
    JSONObject guidInfo = lookupGuidRecord(guid);
    try {
      String key = guidInfo.getString(GNSProtocol.GUID_RECORD_PUBLICKEY.toString());
      byte[] encodedPublicKey = Base64.decode(key);
      KeyFactory keyFactory = KeyFactory.getInstance(GNSProtocol.RSA_ALGORITHM.toString());
      X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(
              encodedPublicKey);
      return keyFactory.generatePublic(publicKeySpec);
    } catch (JSONException e) {
      throw new ClientException("Failed to parse LOOKUP_USER response", e);
    } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
      throw new EncryptionException("Public key encryption failed", e);
    }

  }

  /**
   * Register a new account guid with the corresponding alias on the GNS
   * server. This generates a new guid and a public / private key pair.
   * Returns a GuidEntry for the new account which contains all of this
   * information.
   *
   * @param alias
   * - a human readable alias to the guid - usually an email
   * address
   * @param password
   * @return GuidEntry for {@code alias}
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public GuidEntry accountGuidCreate(String alias, String password) throws ClientException, IOException {
    try {
      execute(GNSCommand.accountGuidCreate(getGNSProvider(), alias, password));
    } catch (NoSuchAlgorithmException e) {
      throw new ClientException(e);
    }
    GuidEntry guidEntry = GuidUtils.lookupGuidEntryFromDatabase(this, alias);
    // If something went wrong an exception should be thrown above, but we're checking
    // here anyway just to be safe.
    if (guidEntry == null) {
      throw new ClientException("Failed to create account guid for " + alias);
    }
    return guidEntry;

//    GuidEntry entry = GuidUtils.lookupGuidEntryFromDatabase(this, alias);
//    /* arun: Don't recreate pair if one already exists. Otherwise you can
//		 * not get out of the funk where the account creation timed out but
//		 * wasn't rolled back fully at the server. Re-using
//		 * the same guid will at least pass verification as opposed to 
//		 * incurring an GNSProtocol.ACTIVE_REPLICA_EXCEPTION.toString() for a new (non-existent) guid.
//     */
//    if (entry == null) {
//      KeyPair keyPair = KeyPairGenerator.getInstance(GNSProtocol.RSA_ALGORITHM.toString())
//              .generateKeyPair();
//      String guid = SharedGuidUtils.createGuidStringFromPublicKey(keyPair
//              .getPublic().getEncoded());
//      // Squirrel this away now just in case the call below times out.
//      KeyPairUtils.saveKeyPair(getGNSProvider(), alias, guid, keyPair);
//      entry = new GuidEntry(alias, guid, keyPair.getPublic(),
//              keyPair.getPrivate());
//    }
//    assert (entry != null);
//    String returnedGuid = accountGuidCreateHelper(alias,
//            entry, password);
//    // Anything else we want to do here?
//    if (!returnedGuid.equals(entry.guid)) {
//      GNSClientConfig
//              .getLogger()
//              .log(Level.WARNING,
//                      "Returned guid {0} doesn''t match locally created guid {1}",
//                      new Object[]{returnedGuid, entry.guid});
//    }
//    assert returnedGuid.equals(entry.guid);
//
//    return entry;
  }

  /**
   * Verify an account by sending the verification code back to the server.
   *
   * @param guid
   * the account guid to verify
   * @param code
   * the verification code
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void accountGuidVerify(GuidEntry guid, String code) throws ClientException, IOException {
    execute(GNSCommand.accountGuidVerify(guid, code));
//    return getResponse(CommandType.VerifyAccount, guid, GNSProtocol.GUID.toString(),
//            guid.getGuid(), GNSProtocol.CODE.toString(), code);
  }

  /**
   * Resends the authentication email which was originally sent during account creation.
   *
   * @param guid
   * @return the email
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public String accountResendAuthenticationEmail(GuidEntry guid) throws ClientException, IOException {
    return execute(GNSCommand.accountResendAuthenticationEmail(guid)).getResultString();
//    return getResponse(CommandType.ResendAuthenticationEmail, guid, GNSProtocol.GUID.toString(),
//            guid.getGuid());
  }

  /**
   * Deletes the account given by name
   *
   * @param guid
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void accountGuidRemove(GuidEntry guid) throws ClientException, IOException {
    execute(GNSCommand.accountGuidRemove(guid)).getResultString();
//    getResponse(CommandType.RemoveAccount, guid, GNSProtocol.GUID.toString(), guid.getGuid(),
//            GNSProtocol.NAME.toString(), guid.getEntityName());
  }

  /**
   * Deletes the account given by name using the password to authenticate.
   *
   * @param name
   * @param password
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void accountGuidRemoveWithPassword(String name, String password)
          throws ClientException, IOException {
    execute(GNSCommand.accountGuidRemoveWithPassword(name, password));
//    String encodedPassword = Password.encryptAndEncodePassword(password, name);
//    getResponse(CommandType.RemoveAccountWithPassword, GNSProtocol.NAME.toString(), name, 
//            GNSProtocol.PASSWORD.toString(), encodedPassword);
  }

  /**
   * Creates an new guid associated with an account on the GNS server.
   *
   * @param accountGuid
   * @param alias the alias
   * @return the newly created guid entry
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public GuidEntry guidCreate(GuidEntry accountGuid, String alias)
          throws ClientException, IOException {

    execute(GNSCommand.createGUID(getGNSProvider(), accountGuid, alias));
    GuidEntry guidEntry = GuidUtils.lookupGuidEntryFromDatabase(this, alias);
    // If something went wrong an exception should be thrown above, but we're checking
    // here anyway just to be safe.
    if (guidEntry == null) {
      throw new ClientException("Failed to create guid for " + alias);
    }
    return guidEntry;
//    long startTime = System.currentTimeMillis();
//    GuidEntry entry = GuidUtils.createAndSaveGuidEntry(alias,
//            getGNSProvider());
//    DelayProfiler.updateDelay("updatePreferences", startTime);
//    String returnedGuid = guidCreateHelper(accountGuid, alias,
//            entry.getPublicKey());
//    assert returnedGuid.equals(entry.getGuid());
//    // Anything else we want to do here?
//    if (!returnedGuid.equals(entry.getGuid())) {
//      GNSClientConfig.getLogger().log(Level.WARNING,
//              "Returned guid {0}doesn''t match locally created guid{1}",
//              new Object[]{returnedGuid, entry.getGuid()});
//    }
//    DelayProfiler.updateDelay("guidCreateFromAlias", startTime);
//    return entry;
  }

  /**
   * Batch create guids with the given aliases. If createPublicKeys is true,
   * key pairs will be created and saved by the client for the guids. If not,
   * bogus public keys will be uses which will make the guids only accessible
   * using the account guid (which has ACL access to each guid).
   *
   * @param accountGuid
   * @param aliases
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void guidBatchCreate(GuidEntry accountGuid, Set<String> aliases)
          throws ClientException, IOException {

    execute(GNSCommand.batchCreateGUIDs(getGNSProvider(), accountGuid, aliases));
//    List<String> aliasList = new ArrayList<>(aliases);
//    List<String> publicKeys = null;
//    long publicKeyStartTime = System.currentTimeMillis();
//    publicKeys = new ArrayList<>();
//    for (String alias : aliasList) {
//      long singleEntrystartTime = System.currentTimeMillis();
//      GuidEntry entry = GuidUtils.createAndSaveGuidEntry(alias,
//              getGNSProvider());
//      DelayProfiler.updateDelay("updateOnePreference",
//              singleEntrystartTime);
//      byte[] publicKeyBytes = entry.getPublicKey().getEncoded();
//      String publicKeyString = Base64.encodeToString(publicKeyBytes,
//              false);
//      publicKeys.add(publicKeyString);
//    }
//    DelayProfiler.updateDelay("batchCreatePublicKeys", publicKeyStartTime);
//
//    return getResponse(CommandType.AddMultipleGuids, accountGuid, GNSProtocol.GUID.toString(),
//            accountGuid.getGuid(), GNSProtocol.NAMES.toString(), new JSONArray(aliasList),
//            GNSProtocol.PUBLIC_KEYS.toString(), new JSONArray(publicKeys));
  }

  /**
   * Removes a guid.
   * NOTE: Not for account guids - use removeAccountGuid for them.
   *
   * @param guid the guid to remove
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void guidRemove(GuidEntry guid) throws ClientException, IOException {
    execute(GNSCommand.removeGUID(guid));
    //getResponse(CommandType.RemoveGuidNoAccount, guid, GNSProtocol.GUID.toString(), guid.getGuid());
  }

  /**
   * Removes a guid given the guid and the associated account guid.
   *
   * @param accountGuid
   * @param guidToRemove the guid to remove
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void guidRemove(GuidEntry accountGuid, String guidToRemove)
          throws ClientException, IOException {
    execute(GNSCommand.removeGUID(accountGuid, guidToRemove));
//    getResponse(CommandType.RemoveGuid, accountGuid, GNSProtocol.ACCOUNT_GUID.toString(),
//            accountGuid.getGuid(), GNSProtocol.GUID.toString(), guidToRemove);
  }

  // GROUP COMMANDS
  /**
   * Return the list of guids that are members of the group. Signs the query
   * using the private key of the user associated with the guid.
   *
   * @param groupGuid
   * the guid of the group to lookup
   * @param reader
   * the guid of the entity doing the lookup
   * @return the list of guids as a JSONArray
   * @throws IOException
   * if a communication error occurs
   * @throws ClientException
   * if a protocol error occurs or the list cannot be parsed
   */
  public JSONArray groupGetMembers(String groupGuid, GuidEntry reader)
          throws ClientException, IOException {
    return execute(GNSCommand.groupGetMembers(groupGuid, reader)).getResultJSONArray();
//    try {
//      return new JSONArray(getResponse(CommandType.GetGroupMembers,
//              reader, GNSProtocol.GUID.toString(), groupGuid, GNSProtocol.READER.toString(), reader.getGuid()));
//    } catch (JSONException e) {
//      throw new ClientException("Invalid member list", e);
//    }
  }

  /**
   * Return a list of the groups that the guid is a member of. Signs the query
   * using the private key of the user associated with the guid.
   *
   * @param guid
   * the guid we are looking for
   * @param reader
   * the guid of the entity doing the lookup
   * @return the list of groups as a JSONArray
   * @throws IOException
   * if a communication error occurs
   * @throws ClientException
   * if a protocol error occurs or the list cannot be parsed
   */
  public JSONArray guidGetGroups(String guid, GuidEntry reader)
          throws IOException, ClientException {
    return execute(GNSCommand.guidGetGroups(guid, reader)).getResultJSONArray();
//    try {
//      return new JSONArray(getResponse(CommandType.GetGroups, reader,
//              GNSProtocol.GUID.toString(), guid, GNSProtocol.READER.toString(), reader.getGuid()));
//    } catch (JSONException e) {
//      throw new ClientException("Invalid member list", e);
//    }
  }

  /**
   * Add a guid to a group guid. Any guid can be a group guid. Signs the query
   * using the private key of the user associated with the writer.
   *
   * @param groupGuid
   * guid of the group
   * @param guidToAdd
   * guid to add to the group
   * @param writer
   * the guid doing the add
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   *
   */
  public void groupAddGuid(String groupGuid, String guidToAdd,
          GuidEntry writer) throws IOException, ClientException {
    execute(GNSCommand.groupAddGuid(groupGuid, guidToAdd, writer));
//    getResponse(CommandType.AddToGroup, writer, GNSProtocol.GUID.toString(), groupGuid, GNSProtocol.MEMBER.toString(),
//            guidToAdd, GNSProtocol.WRITER.toString(), writer.getGuid());
  }

  /**
   * Add multiple members to a group.
   *
   * @param groupGuid
   * guid of the group
   * @param members
   * guids of members to add to the group
   * @param writer
   * the guid doing the add
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void groupAddGuids(String groupGuid, JSONArray members,
          GuidEntry writer) throws IOException, ClientException {
    execute(GNSCommand.groupAddGUIDs(groupGuid, members, writer));
//    getResponse(CommandType.AddMembersToGroup, writer, GNSProtocol.GUID.toString(), groupGuid,
//            GNSProtocol.MEMBERS.toString(), members, GNSProtocol.WRITER.toString(), writer.getGuid());
  }

  /**
   * Removes a guid from a group guid. Any guid can be a group guid. Signs the
   * query using the private key of the user associated with the writer.
   *
   * @param groupGuid
   * @param guidToRemove
   * guid to remove from the group
   * @param writer
   * the guid of the entity doing the remove
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void groupRemoveGuid(String groupGuid, String guidToRemove,
          GuidEntry writer) throws ClientException, IOException {
    execute(GNSCommand.groupRemoveGuid(groupGuid, guidToRemove, writer));
//    getResponse(CommandType.RemoveFromGroup, writer, GNSProtocol.GUID.toString(), groupGuid, GNSProtocol.MEMBER.toString(),
//            guidToRemove, GNSProtocol.WRITER.toString(), writer.getGuid());
  }

  /**
   * Remove a list of members from a group
   *
   * @param groupGuid
   * guid of the group
   * @param members
   * guids to remove from the group
   * @param writer
   * the guid of the entity doing the remove
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void groupRemoveGuids(String groupGuid, JSONArray members,
          GuidEntry writer) throws ClientException, IOException {
    execute(GNSCommand.groupRemoveGuids(groupGuid, members, writer));
//    getResponse(CommandType.RemoveMembersFromGroup, writer, GNSProtocol.GUID.toString(), guid,
//            GNSProtocol.MEMBERS.toString(), members, GNSProtocol.WRITER.toString(), writer.getGuid());
  }

  /**
   * Authorize guidToAuthorize to add/remove members from the group groupGuid.
   * If guidToAuthorize is null, everyone is authorized to add/remove members
   * to the group. Note that this method can only be called by the group owner
   * (private key required) Signs the query using the private key of the group
   * owner.
   *
   * @param groupGuid
   * the group guid entry
   * @param guidToAuthorize
   * the guid to authorize to manipulate group membership or null
   * for anyone
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void groupAddMembershipUpdatePermission(GuidEntry groupGuid,
          String guidToAuthorize) throws ClientException, IOException {
    execute(GNSCommand.groupAddMembershipUpdatePermission(groupGuid, guidToAuthorize));
//    aclAdd(AclAccessType.WRITE_WHITELIST, groupGuid, GNSProtocol.GROUP_ACL.toString(),
//            guidToAuthorize);
  }

  /**
   * Unauthorize guidToUnauthorize to add/remove members from the group
   * groupGuid. If guidToUnauthorize is null, everyone is forbidden to
   * add/remove members to the group. Note that this method can only be called
   * by the group owner (private key required). Signs the query using the
   * private key of the group owner.
   *
   * @param groupGuid
   * the group guid entry
   * @param guidToUnauthorize
   * the guid to authorize to manipulate group membership or null
   * for anyone
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void groupRemoveMembershipUpdatePermission(GuidEntry groupGuid,
          String guidToUnauthorize) throws ClientException, IOException {
    execute(GNSCommand.groupRemoveMembershipUpdatePermission(groupGuid, guidToUnauthorize));
//    aclRemove(AclAccessType.WRITE_WHITELIST, groupGuid, GNSProtocol.GROUP_ACL.toString(),
//            guidToUnauthorize);
  }

  /**
   * Authorize guidToAuthorize to get the membership list from the group
   * groupGuid. If guidToAuthorize is null, everyone is authorized to list
   * members of the group. Note that this method can only be called by the
   * group owner (private key required). Signs the query using the private key
   * of the group owner.
   *
   * @param groupGuid
   * the group guid entry
   * @param guidToAuthorize
   * the guid to authorize to manipulate group membership or null
   * for anyone
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void groupAddMembershipReadPermission(GuidEntry groupGuid,
          String guidToAuthorize) throws ClientException, IOException {
    execute(GNSCommand.groupAddMembershipReadPermission(groupGuid, guidToAuthorize));
//    aclAdd(AclAccessType.READ_WHITELIST, groupGuid, GNSProtocol.GROUP_ACL.toString(),
//            guidToAuthorize);
  }

  /**
   * Unauthorize guidToUnauthorize to get the membership list from the group
   * groupGuid. If guidToUnauthorize is null, everyone is forbidden from
   * querying the group membership. Note that this method can only be called
   * by the group owner (private key required). Signs the query using the
   * private key of the group owner.
   *
   * @param groupGuid
   * the group guid entry
   * @param guidToUnauthorize
   * the guid to authorize to manipulate group membership or null
   * for anyone
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void groupRemoveMembershipReadPermission(GuidEntry groupGuid,
          String guidToUnauthorize) throws ClientException, IOException {
    execute(GNSCommand.groupRemoveMembershipReadPermission(groupGuid, guidToUnauthorize));
//    aclRemove(AclAccessType.READ_WHITELIST, groupGuid, GNSProtocol.GROUP_ACL.toString(),
//            guidToUnauthorize);
  }

  // ACL COMMANDS
  /**
   * Adds to an access control list of the given field. The accesser can be a
   * guid of a user or a group guid or null which means anyone can access the
   * field. The field can be also be +ALL+ which means all fields can be read
   * by the reader. Signs the query using the private key of the user
   * associated with the guid.
   *
   * @param accessType
   * a value from GnrsProtocol.AclAccessType
   * @param targetGuid
   * guid of the field to be modified
   * @param field
   * field name
   * @param accesserGuid
   * guid to add to the ACL
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void aclAdd(AclAccessType accessType, GuidEntry targetGuid,
          String field, String accesserGuid) throws ClientException, IOException {
    execute(GNSCommand.aclAdd(accessType, targetGuid, field, accesserGuid));
    //aclAdd(accessType.name(), targetGuid, field, accesserGuid);
  }

  /**
   * Removes a guid from an access control list of the given user's field on
   * the GNS server to include the guid specified in the accesser param. The
   * accesser can be a guid of a user or a group guid or null which means
   * anyone can access the field. The field can be also be +ALL+ which means
   * all fields can be read by the reader. Signs the query using the private
   * key of the user associated with the guid.
   *
   * @param accessType
   * @param targetGuid
   * @param field
   * @param accesserGuid
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void aclRemove(AclAccessType accessType, GuidEntry targetGuid,
          String field, String accesserGuid) throws ClientException, IOException {
    execute(GNSCommand.aclRemove(accessType, targetGuid, field, accesserGuid));
    //aclRemove(accessType.name(), targetGuid, field, accesserGuid);
  }

  /**
   * Get an access control list of the given user's field on the GNS server to
   * include the guid specified in the accesser param. The accesser can be a
   * guid of a user or a group guid or null which means anyone can access the
   * field. The field can be also be +ALL+ which means all fields can be read
   * by the reader. Signs the query using the private key of the user
   * associated with the guid.
   *
   * @param accessType
   * @param targetGuid
   * @param field
   * @param readerGuid
   * @return list of GUIDs for that ACL
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public JSONArray aclGet(AclAccessType accessType, GuidEntry targetGuid,
          String field, String readerGuid) throws ClientException, IOException {
    return execute(GNSCommand.aclGet(accessType, targetGuid, field, readerGuid)).getResultJSONArray();
    //return aclGet(accessType.name(), targetGuid, field, readerGuid);
  }

  /**
   * Create an empty ACL for the field in the guid.
   * The writerGuid must have write access to the ACL in the guid.
   *
   * @param accessType
   * @param guid
   * @param field
   * @param writerGuid
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void fieldCreateAcl(AclAccessType accessType, GuidEntry guid, String field,
          String writerGuid) throws ClientException, IOException {
    execute(GNSCommand.fieldCreateAcl(accessType, guid, field, writerGuid));
//    getResponse(CommandType.FieldCreateAcl, guid, GNSProtocol.ACL_TYPE.toString(), accessType.name(), GNSProtocol.GUID.toString(),
//            guid.getGuid(), GNSProtocol.FIELD.toString(), field, GNSProtocol.WRITER.toString(), writerGuid);
  }

  /**
   * Create an empty ACL for the field in the guid.
   *
   * @param accessType
   * @param guid
   * @param field
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void fieldCreateAcl(AclAccessType accessType, GuidEntry guid, String field)
          throws ClientException, IOException {
    fieldCreateAcl(accessType, guid, field, guid.getGuid());
  }

  /**
   *
   * @param accessType
   * @param guid
   * @param field
   * @param writerGuid
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void fieldDeleteAcl(AclAccessType accessType, GuidEntry guid, String field,
          String writerGuid)
          throws ClientException, IOException {
    execute(GNSCommand.fieldDeleteAcl(accessType, guid, field, writerGuid));
//    getResponse(CommandType.FieldDeleteAcl, guid, GNSProtocol.ACL_TYPE.toString(), accessType.name(),
//            GNSProtocol.GUID.toString(), guid.getGuid(), GNSProtocol.FIELD.toString(), field, GNSProtocol.WRITER.toString(), writerGuid);
  }

  /**
   *
   * @param accessType
   * @param guid
   * @param field
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void fieldDeleteAcl(AclAccessType accessType, GuidEntry guid, String field)
          throws ClientException, IOException {
    fieldDeleteAcl(accessType, guid, field, guid.getGuid());
  }

  /**
   *
   * @param accessType
   * @param guid
   * @param field
   * @param readerGuid
   * @return
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public boolean fieldAclExists(AclAccessType accessType, GuidEntry guid, String field,
          String readerGuid) throws ClientException, IOException {
    return execute(GNSCommand.fieldAclExists(accessType, guid, field, readerGuid)).getResultBoolean();
//    return Boolean.valueOf(getResponse(CommandType.FieldAclExists, guid, 
//            GNSProtocol.ACL_TYPE.toString(), accessType.name(),
//            GNSProtocol.GUID.toString(), guid.getGuid(), 
//            GNSProtocol.FIELD.toString(), field, 
//            GNSProtocol.READER.toString(), readerGuid));
  }

  /**
   *
   * @param accessType
   * @param guid
   * @param field
   * @return true if the field exists
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public boolean fieldAclExists(AclAccessType accessType, GuidEntry guid, String field)
          throws ClientException, IOException {
    return fieldAclExists(accessType, guid, field, guid.getGuid());
  }

  // ALIASES
  /**
   * Creates an alias entity name for the given guid. The alias can be used
   * just like the original entity name.
   *
   * @param guid
   * @param name
   * - the alias
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void addAlias(GuidEntry guid, String name) throws ClientException, IOException {
    execute(GNSCommand.addAlias(guid, name));
//    getResponse(CommandType.AddAlias, guid, GNSProtocol.GUID.toString(), guid.getGuid(), GNSProtocol.NAME.toString(),
//            name);
  }

  /**
   * Removes the alias for the given guid.
   *
   * @param guid
   * @param name
   * - the alias
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void removeAlias(GuidEntry guid, String name) throws ClientException, IOException {
    execute(GNSCommand.removeAlias(guid, name));
//    getResponse(CommandType.RemoveAlias, guid, GNSProtocol.GUID.toString(), guid.getGuid(), GNSProtocol.NAME.toString(),
//            name);
  }

  /**
   * Retrieve the aliases associated with the given guid.
   *
   * @param guid
   * @return - a JSONArray containing the aliases
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public JSONArray getAliases(GuidEntry guid) throws ClientException, IOException {
    return execute(GNSCommand.getAliases(guid)).getResultJSONArray();
//    try {
//      return new JSONArray(getResponse(CommandType.RetrieveAliases, guid,
//              GNSProtocol.GUID.toString(), guid.getGuid()));
//    } catch (JSONException e) {
//      throw new ClientException("Invalid alias list", e);
//    }
  }

  // ///////////////////////////////
  // // PRIVATE METHODS BELOW /////
  // /////////////////////////////
//  /**
//   * Creates a new guid associated with an account.
//   *
//   * @param accountGuid
//   * @param name
//   * @param publicKey
//   * @return the guid string
//   * @throws Exception
//   */
//  private String guidCreateHelper(GuidEntry accountGuid, String name,
//          PublicKey publicKey) throws ClientException, IOException {
//    long startTime = System.currentTimeMillis();
//    byte[] publicKeyBytes = publicKey.getEncoded();
//    String publicKeyString = Base64.encodeToString(publicKeyBytes, false);
//    String result = getResponse(CommandType.AddGuid, accountGuid, GNSProtocol.GUID.toString(),
//            accountGuid.getGuid(), GNSProtocol.NAME.toString(), name, GNSProtocol.PUBLIC_KEY.toString(), publicKeyString);
//    DelayProfiler.updateDelay("guidCreate", startTime);
//    return result;
//  }
//  /**
//   * Register a new account guid with the corresponding alias and the given
//   * public key on the GNS server. Returns a new guid.
//   *
//   * @param alias
//   * the alias to register (usually an email address)
//   * @param publicKey
//   * the public key associate with the account
//   * @return guid the guid generated by the GNS
//   * @throws IOException
//   * @throws UnsupportedEncodingException
//   * @throws ClientException
//   * @throws InvalidGuidException
//   * if the user already exists
//   */
//  private String accountGuidCreateHelper(String alias, GuidEntry guidEntry, String password)
//          throws UnsupportedEncodingException, IOException, ClientException,
//          InvalidGuidException, NoSuchAlgorithmException {
//    long startTime = System.currentTimeMillis();
//    String result = getResponse(CommandType.RegisterAccount, guidEntry, GNSProtocol.NAME.toString(), alias,
//            GNSProtocol.PUBLIC_KEY.toString(), Base64.encodeToString(
//                    guidEntry.publicKey.getEncoded(), false), GNSProtocol.PASSWORD.toString(),
//            password != null
//                    ? Password.encryptAndEncodePassword(password, alias)
//                    : "");
//    DelayProfiler.updateDelay("accountGuidCreate", startTime);
//    return result;
//  }
//
//  private void aclAdd(String accessType, GuidEntry guid, String field,
//          String accesserGuid) throws ClientException, IOException {
//    getResponse(CommandType.AclAddSelf, guid, GNSProtocol.ACL_TYPE.toString(), accessType, GNSProtocol.GUID.toString(),
//            guid.getGuid(), GNSProtocol.FIELD.toString(), field, GNSProtocol.ACCESSER.toString(),
//            accesserGuid == null ? GNSProtocol.ALL_GUIDS.toString() : accesserGuid);
//  }
//
//  private void aclRemove(String accessType, GuidEntry guid, String field,
//          String accesserGuid) throws ClientException, IOException {
//    getResponse(CommandType.AclRemoveSelf, guid, GNSProtocol.ACL_TYPE.toString(), accessType,
//            GNSProtocol.GUID.toString(), guid.getGuid(), GNSProtocol.FIELD.toString(), field, GNSProtocol.ACCESSER.toString(),
//            accesserGuid == null ? GNSProtocol.ALL_GUIDS.toString() : accesserGuid);
//  }
//
//  private JSONArray aclGet(String accessType, GuidEntry guid, String field,
//          String readerGuid) throws ClientException, IOException {
//    try {
//      return new JSONArray(getResponse(CommandType.AclRetrieve, guid,
//              GNSProtocol.ACL_TYPE.toString(), accessType, GNSProtocol.GUID.toString(), guid.getGuid(), GNSProtocol.FIELD.toString(), field,
//              GNSProtocol.READER.toString(), readerGuid == null ? GNSProtocol.ALL_GUIDS.toString() : readerGuid));
//    } catch (JSONException e) {
//      throw new ClientException("Invalid ACL list", e);
//    }
//  }
  // Extended commands
  /**
   * Creates a new field with value being the list. Allows a a different guid
   * as the writer. If the writer is different use addToACL first to allow
   * other the guid to write this field.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void fieldCreateList(String targetGuid, String field,
          JSONArray value, GuidEntry writer) throws IOException, ClientException {
    execute(GNSCommand.fieldCreateList(targetGuid, field, value, writer));
//    getResponse(CommandType.CreateList, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(),
//            field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(), writer.getGuid());
  }

  /**
   * Appends the values of the field onto list of values or creates a new
   * field with values in the list if it does not exist.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void fieldAppendOrCreateList(String targetGuid, String field,
          JSONArray value, GuidEntry writer) throws IOException, ClientException {
    execute(GNSCommand.fieldAppendOrCreateList(targetGuid, field, value, writer));
//    getResponse(CommandType.AppendOrCreateList, writer, GNSProtocol.GUID.toString(), targetGuid,
//            GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(), writer.getGuid());
  }

  /**
   * Replaces the values of the field with the list of values or creates a new
   * field with values in the list if it does not exist.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void fieldReplaceOrCreateList(String targetGuid, String field,
          JSONArray value, GuidEntry writer) throws IOException, ClientException {
    execute(GNSCommand.fieldReplaceOrCreateList(targetGuid, field, value, writer));
//    getResponse(CommandType.ReplaceOrCreateList, writer, GNSProtocol.GUID.toString(), targetGuid,
//            GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(), writer.getGuid());
  }

  /**
   * Appends a list of values onto a field.
   *
   * @param targetGuid
   * guid where the field is stored
   * @param field
   * field name
   * @param value
   * list of values
   * @param writer
   * guidEntry of the writer
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void fieldAppend(String targetGuid, String field, JSONArray value,
          GuidEntry writer) throws IOException, ClientException {
    execute(GNSCommand.fieldAppend(targetGuid, field, value, writer));
//    getResponse(CommandType.AppendListWithDuplication, writer, GNSProtocol.GUID.toString(),
//            targetGuid, GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(),
//            writer.getGuid());
  }

  /**
   * Replaces all the values of field with the list of values.
   *
   * @param targetGuid
   * guid where the field is stored
   * @param field
   * field name
   * @param value
   * list of values
   * @param writer
   * guid entry of the writer
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void fieldReplaceList(String targetGuid, String field,
          JSONArray value, GuidEntry writer) throws IOException,
          ClientException {
    execute(GNSCommand.fieldReplaceList(targetGuid, field, value, writer));
//    getResponse(CommandType.ReplaceList, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(),
//            field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(), writer.getGuid());
  }

  /**
   * Removes all the values in the list from the field.
   *
   * @param targetGuid
   * guid where the field is stored
   * @param field
   * field name
   * @param value
   * list of values
   * @param writer
   * guid entry of the writer
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void fieldClear(String targetGuid, String field, JSONArray value,
          GuidEntry writer) throws IOException, ClientException {
    execute(GNSCommand.fieldClear(targetGuid, field, value, writer));
//    getResponse(CommandType.RemoveList, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(),
//            field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(), writer.getGuid());
  }

  /**
   * Removes all values from the field.
   *
   * @param targetGuid
   * guid where the field is stored
   * @param field
   * field name
   * @param writer
   * guid entry of the writer
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void fieldClear(String targetGuid, String field, GuidEntry writer)
          throws IOException, ClientException {
    execute(GNSCommand.fieldClear(targetGuid, field, writer));
//    getResponse(CommandType.Clear, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(), field,
//            GNSProtocol.WRITER.toString(), writer.getGuid());
  }

  /**
   * Reads all the values for a key from the GNS server for the given guid
   * (assumes that value is a array).
   * The guid of the user attempting access is also needed. Signs the query
   * using the private key of the user associated with the reader guid
   * (unsigned if reader is null).
   *
   * @param guid
   * @param field
   * @param reader
   * if null the field must be readable for all
   * @return a JSONArray containing the values in the field
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public JSONArray fieldReadArray(String guid, String field, GuidEntry reader)
          throws ClientException, IOException {
    String response = execute(GNSCommand.fieldReadArray(guid, field, reader)).getResultString();
    try {
      return CommandUtils.commandResponseToJSONArray(field, response);
    } catch (JSONException e) {
      throw new ClientException(e);
    }
//    return commandResponseToJSONArray(field,
//            (getResponse(reader != null ? CommandType.ReadArray
//                    : CommandType.ReadArrayUnsigned, reader, GNSProtocol.GUID.toString(), guid, GNSProtocol.FIELD.toString(),
//                    field, GNSProtocol.READER.toString(), reader != null ? reader.getGuid() : null)));
  }

  /**
   * Sets the nth value (zero-based) indicated by index in the list contained
   * in field to newValue (assumes that value is a array). Index must be less
   * than the current size of the list.
   *
   * @param targetGuid
   * @param field
   * @param newValue
   * @param index
   * @param writer
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void fieldSetElement(String targetGuid, String field,
          String newValue, int index, GuidEntry writer) throws IOException,
          ClientException {
    execute(GNSCommand.fieldSetElement(targetGuid, field, newValue, index, writer));
//    getResponse(CommandType.Set, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(), field,
//            GNSProtocol.VALUE.toString(), newValue, GNSProtocol.N.toString(), Integer.toString(index), GNSProtocol.WRITER.toString(),
//            writer.getGuid());
  }

  /**
   * Sets a field to be null. That is when read field is called a null will be
   * returned.
   *
   * @param targetGuid
   * @param field
   * @param writer
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void fieldSetNull(String targetGuid, String field, GuidEntry writer)
          throws IOException, ClientException {
    execute(GNSCommand.fieldSetNull(targetGuid, field, writer));
//    getResponse(CommandType.SetFieldNull, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(),
//            field, GNSProtocol.WRITER.toString(), writer.getGuid());
  }

  //
  // SELECT
  //
  /**
   * Returns all GUIDs that have a field that contains the given value as a
   * JSONArray containing guids.
   *
   * @param field
   * @param value
   * @return a JSONArray containing the guids of all the matched records
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public JSONArray select(String field, String value) throws ClientException, IOException {
    return execute(GNSCommand.select(field, value)).getResultJSONArray();
//    return new JSONArray(getResponse(CommandType.Select,
//            GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value));
  }

  /**
   * If field is a GeoSpatial field queries the GNS server return all the
   * guids that have fields that are within value which is a bounding box
   * specified as a nested JSONArrays of paired tuples: [[LONG_UL,
   * LAT_UL],[LONG_BR, LAT_BR]]
   *
   * @param field
   * @param value
   * - [[LONG_UL, LAT_UL],[LONG_BR, LAT_BR]]
   * @return a JSONArray containing the guids of all the matched records
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public JSONArray selectWithin(String field, JSONArray value)
          throws ClientException, IOException {
    return execute(GNSCommand.selectWithin(field, value)).getResultJSONArray();
//    return new JSONArray(getResponse(CommandType.SelectWithin, GNSProtocol.FIELD.toString(), field, GNSProtocol.WITHIN.toString(),
//            value));
  }

  /**
   * If field is a GeoSpatial field queries the GNS server and returns all the
   * guids that have fields that are near value which is a point specified as
   * a two element JSONArray: [LONG, LAT]. Max Distance is in meters.
   *
   * @param field
   * @param value
   * - [LONG, LAT]
   * @param maxDistance
   * - distance in meters
   * @return a JSONArray containing the guids of all the matched records
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public JSONArray selectNear(String field, JSONArray value,
          Double maxDistance) throws ClientException, IOException {
    return execute(GNSCommand.selectNear(field, value, maxDistance)).getResultJSONArray();
//    return new JSONArray(getResponse(CommandType.SelectNear, GNSProtocol.FIELD.toString(), field, GNSProtocol.NEAR.toString(), value,
//            GNSProtocol.MAX_DISTANCE.toString(), Double.toString(maxDistance)));
  }

  /**
   * Update the location field for the given GNSProtocol.GUID.toString()
   *
   * @param targetGuid
   * @param longitude
   * the guid longitude
   * @param latitude
   * the guid latitude
   * @param writer
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void setLocation(String targetGuid, double longitude,
          double latitude, GuidEntry writer) throws ClientException, IOException {
    fieldReplaceOrCreateList(targetGuid, GNSProtocol.LOCATION_FIELD_NAME.toString(),
            new JSONArray(Arrays.asList(longitude, latitude)), writer);
  }

  /**
   * Update the location field for the given guid
   *
   * @param longitude
   * the guid longitude
   * @param latitude
   * the guid latitude
   * @param guid
   * the guid to update
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void setLocation(GuidEntry guid, double longitude, double latitude)
          throws ClientException, IOException {
    setLocation(guid.getGuid(), longitude, latitude, guid);
  }

  /**
   * Get the location of the target guid as a JSONArray: [LONG, LAT]
   *
   * @param readerGuid
   * the guid issuing the request
   * @param targetGuid
   * the guid that we want to know the location
   * @return a JSONArray: [LONGITUDE, LATITUDE]
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public JSONArray getLocation(String targetGuid, GuidEntry readerGuid)
          throws ClientException, IOException {
    return execute(GNSCommand.getLocation(targetGuid, readerGuid)).getResultJSONArray();
    //return fieldReadArray(targetGuid, GNSProtocol.LOCATION_FIELD_NAME.toString(), readerGuid);
  }

  /**
   * Get the location of the target guid as a JSONArray: [LONG, LAT]
   *
   * @param guid
   * @return a JSONArray: [LONGITUDE, LATITUDE]
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public JSONArray getLocation(GuidEntry guid) throws ClientException, IOException {
    return execute(GNSCommand.getLocation(guid)).getResultJSONArray();
    //return fieldReadArray(guid.getGuid(), GNSProtocol.LOCATION_FIELD_NAME.toString(), guid);
  }

  /**
   * @param guid
   * @param action
   * @param writerGuid
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  // Active Code
  public void activeCodeClear(String guid, String action, GuidEntry writerGuid)
          throws ClientException, IOException {
    execute(GNSCommand.activeCodeClear(guid, action, writerGuid));
//    getResponse(CommandType.ClearCode, writerGuid, GNSProtocol.GUID.toString(), guid,
//            GNSProtocol.AC_ACTION.toString(), action, GNSProtocol.WRITER.toString(), writerGuid.getGuid());
  }

  /**
   * @param guid
   * @param action
   * @param code
   * @param writerGuid
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  // FIXME: The GNSCommand method expects bytes which it Base64 encodes.
  public void activeCodeSet(String guid, String action, byte[] code,
          GuidEntry writerGuid) throws ClientException, IOException {
    execute(GNSCommand.activeCodeSet(guid, action, code, writerGuid));
//    getResponse(CommandType.SetCode, writerGuid, GNSProtocol.GUID.toString(), guid,
//            GNSProtocol.AC_ACTION.toString(), action, GNSProtocol.AC_CODE.toString(), code,
//            GNSProtocol.WRITER.toString(), writerGuid.getGuid());
  }

  /**
   * @param guid
   * @param action
   * @param readerGuid
   * @return Active code of {@code guid} as byte[]
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public String activeCodeGet(String guid, String action, GuidEntry readerGuid)
          throws ClientException, IOException {
    return execute(GNSCommand.activeCodeGet(guid, action, readerGuid)).getResultString();
//    String code = getResponse(CommandType.GetCode,
//            readerGuid, GNSProtocol.GUID.toString(), guid, 
//            GNSProtocol.AC_ACTION.toString(), action,
//            GNSProtocol.READER.toString(), readerGuid.getGuid());
//    return code;
  }

  // Extended commands
  /**
   * Creates a new field in the target guid with value being the list.
   *
   * @param target
   * @param field
   * @param value
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void fieldCreateList(GuidEntry target, String field, JSONArray value)
          throws IOException, ClientException {
    execute(GNSCommand.fieldCreateList(field, field, value, target));
    //fieldCreateList(target.getGuid(), field, value, target);
  }

  /**
   * Creates a new one element field with single element value being the
   * string. Allows a a different guid as the writer. If the writer is
   * different use addToACL first to allow other the guid to write this field.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void fieldCreateOneElementList(String targetGuid, String field,
          String value, GuidEntry writer) throws IOException, ClientException {
    execute(GNSCommand.fieldCreateOneElementList(targetGuid, field, value, writer));
//    getResponse(CommandType.Create, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(),
//            field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(), writer.getGuid());
  }

  /**
   * Creates a new one element field in the target guid with single element
   * value being the string.
   *
   * @param target
   * @param field
   * @param value
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void fieldCreateOneElementList(GuidEntry target, String field,
          String value) throws IOException, ClientException {
    fieldCreateOneElementList(target.getGuid(), field, value, target);
  }

  /**
   * Appends the single value of the field onto list of values or creates a
   * new field with a single value list if it does not exist. If the writer is
   * different use addToACL first to allow other the guid to write this field.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void fieldAppendOrCreate(String targetGuid, String field,
          String value, GuidEntry writer) throws IOException, ClientException {
    execute(GNSCommand.fieldAppendOrCreate(targetGuid, field, value, writer));
//    getResponse(CommandType.AppendOrCreate, writer, GNSProtocol.GUID.toString(), targetGuid,
//            GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(), writer.getGuid());
  }

  /**
   * Replaces the values of the field in targetGuid with the single value or
   * creates a new field with a single value list if it does not exist. If the
   * writer is different use addToACL first to allow other the guid to write
   * this field.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void fieldReplaceOrCreate(String targetGuid, String field,
          String value, GuidEntry writer) throws IOException, ClientException {
    execute(GNSCommand.fieldReplaceOrCreate(targetGuid, field, value, writer));
//    getResponse(CommandType.ReplaceOrCreate, writer, GNSProtocol.GUID.toString(), targetGuid,
//            GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(), writer.getGuid());
  }

  /**
   * Replaces the values of the field with the list of values or creates a new
   * field with values in the list if it does not exist.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void fieldReplaceOrCreateList(GuidEntry targetGuid, String field,
          JSONArray value) throws IOException, ClientException {
    fieldReplaceOrCreateList(targetGuid.getGuid(), field, value, targetGuid);
  }

  /**
   * Replaces the values of the field in the target guid with the single value
   * or creates a new field with a single value list if it does not exist.
   *
   * @param target
   * @param field
   * @param value
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void fieldReplaceOrCreateList(GuidEntry target, String field,
          String value) throws IOException, ClientException {
    fieldReplaceOrCreate(target.getGuid(), field, value, target);
  }

  /**
   * Replaces all the values of field with the single value. If the writer is
   * different use addToACL first to allow other the guid to write this field.
   *
   * @param targetGuid
   * guid where the field is stored
   * @param field
   * field name
   * @param value
   * the new value
   * @param writer
   * guid entry of the writer
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void fieldReplace(String targetGuid, String field, String value,
          GuidEntry writer) throws IOException, ClientException {
    execute(GNSCommand.fieldReplace(writer, field, value));
//    getResponse(CommandType.Replace, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(),
//            field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(), writer.getGuid());
  }

  /**
   * Replaces all the values of field in target with with the single value.
   *
   * @param target
   * @param field
   * @param value
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void fieldReplace(GuidEntry target, String field, String value)
          throws IOException, ClientException {
    fieldReplace(target.getGuid(), field, value, target);
  }

  /**
   * Replaces all the values of field in target with the list of values.
   *
   * @param target
   * @param field
   * @param value
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void fieldReplace(GuidEntry target, String field, JSONArray value)
          throws IOException, ClientException {
    fieldReplaceList(target.getGuid(), field, value, target);
  }

  /**
   * Appends a single value onto a field. If the writer is different use
   * addToACL first to allow other the guid to write this field.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void fieldAppend(String targetGuid, String field, String value,
          GuidEntry writer) throws IOException, ClientException {
    execute(GNSCommand.fieldAppend(targetGuid, field, value, writer));
//    getResponse(CommandType.AppendWithDuplication, writer, GNSProtocol.GUID.toString(),
//            targetGuid, GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(),
//            writer.getGuid());
  }

  /**
   * Appends a single value onto a field in the target guid.
   *
   * @param target
   * @param field
   * @param value
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void fieldAppend(GuidEntry target, String field, String value)
          throws IOException, ClientException {
    fieldAppend(target.getGuid(), field, value, target);
  }

  /**
   * Appends a list of values onto a field but converts the list to set
   * removing duplicates. If the writer is different use addToACL first to
   * allow other the guid to write this field.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void fieldAppendWithSetSemantics(String targetGuid, String field,
          JSONArray value, GuidEntry writer) throws IOException,
          ClientException {
    execute(GNSCommand.fieldAppendWithSetSemantics(targetGuid, field, value, writer));
//    getResponse(CommandType.AppendList, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(),
//            field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(), writer.getGuid());
  }

  /**
   * Appends a list of values onto a field in target but converts the list to
   * set removing duplicates.
   *
   * @param target
   * @param field
   * @param value
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void fieldAppendWithSetSemantics(GuidEntry target, String field,
          JSONArray value) throws IOException, ClientException {
    fieldAppendWithSetSemantics(target.getGuid(), field, value, target);
  }

  /**
   * Appends a single value onto a field but converts the list to set removing
   * duplicates. If the writer is different use addToACL first to allow other
   * the guid to write this field.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void fieldAppendWithSetSemantics(String targetGuid, String field,
          String value, GuidEntry writer) throws IOException, ClientException {
    execute(GNSCommand.fieldAppendWithSetSemantics(targetGuid, field, value, writer));
//    getResponse(CommandType.Append, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(), field,
//            GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(), writer.getGuid());
  }

  /**
   * Appends a single value onto a field in target but converts the list to
   * set removing duplicates.
   *
   * @param target
   * @param field
   * @param value
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void fieldAppendWithSetSemantics(GuidEntry target, String field,
          String value) throws IOException, ClientException {
    fieldAppendWithSetSemantics(target.getGuid(), field, value, target);
  }

  /**
   * Replaces all the first element of field with the value (assuming that value is a array).
   * If the writer is different use addToACL first to allow other the guid to write this field.
   * If writer is null the command is sent unsigned.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws IOException
   * @throws ClientException
   */
  @Deprecated
  public void fieldReplaceFirstElement(String targetGuid, String field,
          String value, GuidEntry writer) throws IOException, ClientException {
    execute(GNSCommand.fieldReplaceFirstElement(targetGuid, field, value, writer));
//    if (writer == null) {
//      throw new ClientException(
//              "Can not perform an update without querier information");
//    }
//    getResponse(CommandType.Replace, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(),
//            field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(), writer != null ? writer.getGuid()
//                    : null);
  }

  /**
   * Replaces the first element of field in target with the value
   * (assuming that value is a array).
   * Note: This is a legacy command used by the unit tests. Will be phased out.
   *
   * @param target
   * @param field
   * @param value
   * @throws IOException
   * @throws ClientException
   */
  @Deprecated
  public void fieldReplaceFirstElement(GuidEntry target, String field,
          String value) throws IOException, ClientException {
    fieldReplaceFirstElement(target.getGuid(), field, value, target);
  }

  /**
   * Substitutes the value for oldValue in the list of values of a field. If
   * the writer is different use addToACL first to allow other the guid to
   * write this field.
   *
   * @param targetGuid
   * guid where the field is stored
   * @param field
   * field name
   * @param newValue
   * @param oldValue
   * @param writer
   * guid entry of the writer
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void fieldSubstitute(String targetGuid, String field,
          String newValue, String oldValue, GuidEntry writer)
          throws IOException, ClientException {
    execute(GNSCommand.fieldSubstitute(writer, field, newValue, oldValue));
//    getResponse(CommandType.Substitute, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(),
//            field, GNSProtocol.VALUE.toString(), newValue, GNSProtocol.OLD_VALUE.toString(), oldValue, GNSProtocol.WRITER.toString(),
//            writer.getGuid());
  }

  /**
   * Substitutes the value for oldValue in the list of values of a field in
   * the target.
   *
   * @param target
   * @param field
   * @param newValue
   * @param oldValue
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void fieldSubstitute(GuidEntry target, String field,
          String newValue, String oldValue) throws IOException,
          ClientException {
    fieldSubstitute(target.getGuid(), field, newValue, oldValue, target);
  }

  /**
   * Pairwise substitutes all the values for the oldValues in the list of
   * values of a field. If the writer is different use addToACL first to allow
   * other the guid to write this field.
   *
   *
   * @param targetGuid
   * guid where the field is stored
   * @param field
   * @param newValue
   * list of new values
   * @param oldValue
   * list of old values
   * @param writer
   * guid entry of the writer
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void fieldSubstitute(String targetGuid, String field,
          JSONArray newValue, JSONArray oldValue, GuidEntry writer)
          throws IOException, ClientException {
    execute(GNSCommand.fieldSubstitute(writer, field, newValue, oldValue));
//    getResponse(CommandType.SubstituteList, writer, GNSProtocol.GUID.toString(), targetGuid,
//            GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), newValue, GNSProtocol.OLD_VALUE.toString(),
//            oldValue, GNSProtocol.WRITER.toString(), writer.getGuid());
  }

  /**
   * Pairwise substitutes all the values for the oldValues in the list of
   * values of a field in the target.
   *
   * @param target
   * @param field
   * @param newValue
   * @param oldValue
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void fieldSubstitute(GuidEntry target, String field,
          JSONArray newValue, JSONArray oldValue) throws IOException,
          ClientException {
    fieldSubstitute(target.getGuid(), field, newValue, oldValue, target);
  }

  /**
   * Reads the first value (assuming that value is a array) for a key
   * from the GNS server for the given guid.
   * The guid of the user attempting access is also needed. Signs the query
   * using the private key of the reader guid
   * (unsigned if reader is null).
   *
   * @param guid
   * @param field
   * @param reader
   * @return First value of {@code field} whose value is expected to be an
   * array.
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  @Deprecated
  public String fieldReadArrayFirstElement(String guid, String field,
          GuidEntry reader) throws ClientException, IOException {
    return execute(GNSCommand.fieldReadArrayFirstElement(guid, field, reader)).getResultString();
//    return getResponse(reader != null ? CommandType.ReadArrayOne
//            : CommandType.ReadArrayOneUnsigned, reader, GNSProtocol.GUID.toString(), guid, GNSProtocol.FIELD.toString(),
//            field, GNSProtocol.READER.toString(), reader != null ? reader.getGuid() : null);
  }

  /**
   * Reads the first value for a key in the guid. Assuming that value is a array.
   * Signs the query using the private key of the guid.
   *
   * @param guid
   * @param field
   * @return First value of {@code field} whose value is expected to be an
   * array.
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  @Deprecated
  public String fieldReadArrayFirstElement(GuidEntry guid, String field)
          throws ClientException, IOException {
    return fieldReadArrayFirstElement(guid.getGuid(), field, guid);
  }

  /**
   * Removes a field in the JSONObject record of the given guid. Signs the
   * query using the private key of the guid. A convenience method™.
   *
   * @param guid
   * @param field
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public void fieldRemove(GuidEntry guid, String field)
          throws IOException, ClientException {
    execute(GNSCommand.fieldRemove(field, field, guid));
    //fieldRemove(guid.getGuid(), field, guid);
  }

  /**
   *
   * @return The contents of the GNS.
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * if a protocol error occurs or the list cannot be parsed
   * @throws java.io.IOException
   * if a communication error occurs
   */
  public String dump() throws ClientException, IOException {
    //Create the admin account if it doesn't already exist.
    try {
      accountGuidCreate("Admin", GNSConfig.getInternalOpSecret());
    } catch (DuplicateNameException dne) {
      //Do nothing if it already exists.
    }
    return execute(GNSCommand.dump()).getResultString();
  }

  @Override
  public void close() {
    super.close();
  }
}
