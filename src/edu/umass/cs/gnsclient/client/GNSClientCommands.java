/* Copyright (c) 2015 University of Massachusetts
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
 * Initial developer(s): Westy, Arun, Emmanuel */
package edu.umass.cs.gnsclient.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.util.Arrays;

import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.client.util.Password;
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnscommon.SharedGuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.EncryptionException;

import org.json.JSONArray;
import org.json.JSONObject;

import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.client.DuplicateNameException;
import edu.umass.cs.gnscommon.exceptions.client.FieldNotFoundException;
import edu.umass.cs.gnscommon.exceptions.client.InvalidGuidException;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.packets.ResponsePacket;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.utils.DelayProfiler;

import java.io.UnsupportedEncodingException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Level;

import org.json.JSONException;
import static edu.umass.cs.gnsclient.client.CommandUtils.commandResponseToJSONArray;
import edu.umass.cs.gnscommon.GNSProtocol;

/**
 * This class defines a client to communicate with a GNS instance over TCP. This
 * class adds single field list based commands to the {@link GNSClient}
 * 's JSONObject based commands.
 *
 * This class contains a concise subset of all available server operations.
 *
 * @author arun, <a href="mailto:westy@cs.umass.edu">Westy</a>
 * @version 1.0
 */
public class GNSClientCommands extends GNSClient //implements GNSClientInterface 
{

  /**
   * @throws IOException
   */
  public GNSClientCommands() throws IOException {
    super((InetSocketAddress) null);
  }

  /**
   * Creates a new {@link GNSClient} object
   *
   * @param anyReconfigurator
   * @throws java.io.IOException
   */
  public GNSClientCommands(InetSocketAddress anyReconfigurator)
          throws IOException {
    super(anyReconfigurator);
  }

  private long readTimeout = 8000;

  /**
   * Returns the timeout value (milliseconds) used when sending commands to
   * the server.
   *
   * @return value in milliseconds
   */
  public long getReadTimeout() {
    return readTimeout;
  }

  /**
   * Sets the timeout value (milliseconds) used when sending commands to the
   * server.
   *
   * @param readTimeout
   * in milliseconds
   */
  public void setReadTimeout(long readTimeout) {
    this.readTimeout = readTimeout;
  }

  /**
   * Invariant: A single CommandPacket should have complete information about
   * how to handle the command.
   */
  /**
   * arun: All occurrences of checkResponse( createAndSignCommand have been
   * replaced by this getResponse method.
   *
   * The response here is converted to a String for legacy reasons. Otherwise,
   * all responses should be of type {@link ResponsePacket}.
   */
  private String getResponse(CommandType commandType, GuidEntry querier,
          Object... keysAndValues) throws ClientException, IOException {
    CommandPacket commandPacket;
    return record(// just instrumentation
            commandType,
            CommandUtils.checkResponse(this
                    .getCommandValueReturnPacket(commandPacket = getCommand(commandType,
                            querier, keysAndValues), this.getReadTimeout()), commandPacket));
  }

  private static final boolean RECORD_ENABLED = true;
  /**
   * only for instrumentation to decode return value types
   */
  public static final Map<CommandType, Set<String>> REVERSE_ENGINEER = new TreeMap<CommandType, Set<String>>();
  /**
   * only for instrumentation to decode return value types
   */
  public static final Map<CommandType, Set<String>> RETURN_VALUE_EXAMPLE = new TreeMap<CommandType, Set<String>>();

  private static final String record(CommandType type, Object responseObj) {
    if (!RECORD_ENABLED || responseObj == null) {
      return (String) responseObj;
    }
    String response = responseObj instanceof ResponsePacket ? ((ResponsePacket) responseObj).getReturnValue()
            : responseObj.toString();
    if (REVERSE_ENGINEER.get(type) == null) {
      REVERSE_ENGINEER.put(type, new HashSet<String>());
    }
    if (RETURN_VALUE_EXAMPLE.get(type) == null) {
      RETURN_VALUE_EXAMPLE.put(type, new HashSet<String>());
    }
    if (response != null) {
      REVERSE_ENGINEER.get(type).add(JSONPacket.couldBeJSONObject(response) ? "JSONObject"
              : JSONPacket.couldBeJSONArray(response) ? "JSONArray"
              : "String");
    }
    if (response != null) {
      RETURN_VALUE_EXAMPLE.get(type).add(response);
    }
    return response;
  }

  private String getResponse(CommandType commandType, Object... keysAndValues)
          throws ClientException, IOException {
    return this.getResponse(commandType, null, keysAndValues);
  }

  /**
   * Constructs the command.
   *
   * @param type
   * @param querier
   * @param keysAndValues
   * @return Constructed CommandPacket
   * @throws ClientException
   */
  private static CommandPacket getCommand(CommandType type, GuidEntry querier,
          Object... keysAndValues) throws ClientException {
    CommandPacket packet = GNSCommand.getCommand(type, querier, keysAndValues);
    return packet;
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
    getResponse(CommandType.ReplaceUserJSON, writer, GNSProtocol.GUID.toString(), targetGuid,
            GNSProtocol.USER_JSON.toString(), json, GNSProtocol.WRITER.toString(), writer.getGuid());
  }

  /**
   * Replaces the JSON in guid with JSONObject. Signs the query using the
   * private key of the given guid.
   *
   * @param guid
   * @param json
   * @throws IOException
   * @throws ClientException
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
   * @throws IOException
   * @throws ClientException
   * @throws JSONException
   */
  public void fieldUpdate(String targetGuid, String field, Object value,
          GuidEntry writer) throws IOException, ClientException {
    try {
      getResponse(CommandType.ReplaceUserJSON, writer,
              GNSProtocol.GUID.toString(), targetGuid,
              GNSProtocol.USER_JSON.toString(), new JSONObject().put(field, value),
              GNSProtocol.WRITER.toString(), writer.getGuid());
    } catch (JSONException e) {
      throw new ClientException(e);
    }
  }

  /**
   * Creates an index for a field. The guid is only used for authentication
   * purposes.
   *
   * @param guid
   * @param field
   * @param index
   * @throws IOException
   * @throws ClientException
   * @throws JSONException
   */
  public void fieldCreateIndex(GuidEntry guid, String field, String index)
          throws IOException, ClientException, JSONException {
    getResponse(CommandType.CreateIndex, guid, GNSProtocol.GUID.toString(), guid.getGuid(), GNSProtocol.FIELD.toString(),
            field, GNSProtocol.VALUE.toString(), index, GNSProtocol.WRITER.toString(), guid.getGuid());
  }

  /**
   * Updates the field in the targetGuid. Signs the query using the private
   * key of the given guid.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @throws IOException
   * @throws ClientException
   * @throws JSONException
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
   * @throws Exception
   */
  public JSONObject read(String targetGuid, GuidEntry reader)
          throws Exception {
    return new JSONObject(getResponse(reader != null ? CommandType.ReadArray
            : CommandType.ReadArrayUnsigned, reader, GNSProtocol.GUID.toString(),
            targetGuid, GNSProtocol.FIELD.toString(), GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.READER.toString(),
            reader != null ? reader.getGuid() : null));
  }

  /**
   * Reads the entire record from the GNS server for the given guid. Signs the
   * query using the private key of the guid.
   *
   * @param guid
   * @return a JSONObject containing the values in the field
   * @throws Exception
   */
  public JSONObject read(GuidEntry guid) throws Exception {
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
   * @throws Exception
   */
  public boolean fieldExists(String targetGuid, String field, GuidEntry reader)
          throws Exception {
    try {
      if (reader != null) {
        getResponse(CommandType.Read, reader,
                GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(), field,
                GNSProtocol.READER.toString(), reader.getGuid());
      } else {
        getResponse(CommandType.ReadUnsigned, reader,
                GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(), field,
                GNSProtocol.READER.toString(), null);
      }

      return true;
    } catch (FieldNotFoundException e) {
      return false;
    }
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
   * @throws Exception
   */
  public boolean fieldExists(GuidEntry targetGuid, String field)
          throws Exception {
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
   * @throws Exception
   */
  public String fieldRead(String targetGuid, String field, GuidEntry reader)
          throws Exception {
    if (reader != null) {
      return CommandUtils.specialCaseSingleField(getResponse(CommandType.Read, reader,
              GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(), field,
              GNSProtocol.READER.toString(), reader.getGuid()));
    } else {
      return CommandUtils.specialCaseSingleField(getResponse(CommandType.ReadUnsigned, reader,
              GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(), field,
              GNSProtocol.READER.toString(), null));
    }
  }

  /**
   * Reads the value of field from the targetGuid. Field is a string the
   * naming the field. Field can use dot notation to indicate subfields. This
   * method signs the query using the private key of the targetGuid.
   *
   * @param targetGuid
   * @param field
   * @return field value
   * @throws Exception
   */
  public String fieldRead(GuidEntry targetGuid, String field)
          throws Exception {
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
   * @throws Exception
   */
  public String fieldRead(String targetGuid, ArrayList<String> fields,
          GuidEntry reader) throws Exception {
    return getResponse(reader != null ? CommandType.ReadMultiField
            : CommandType.ReadMultiFieldUnsigned, reader, GNSProtocol.GUID.toString(), targetGuid,
            GNSProtocol.FIELDS.toString(), fields, GNSProtocol.READER.toString(), reader != null ? reader.getGuid()
                    : null);
  }

  /**
   * Reads the value of fields for the given guid. Fields is a list of strings
   * the naming the field. Fields can use dot notation to indicate subfields.
   * This method signs the query using the private key of the guid.
   *
   * @param targetGuid
   * @param fields
   * @return values of fields
   * @throws Exception
   */
  public String fieldRead(GuidEntry targetGuid, ArrayList<String> fields)
          throws Exception {
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
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   * @throws ClientException
   */
  public void fieldRemove(String targetGuid, String field, GuidEntry writer)
          throws IOException, InvalidKeyException, NoSuchAlgorithmException,
          SignatureException, ClientException {
    getResponse(CommandType.RemoveField, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(),
            field, GNSProtocol.WRITER.toString(), writer.getGuid());
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
   * There are links in the wiki page abive to find the exact syntax for
   * querying spacial coordinates.
   *
   * @param query
   * - the query
   * @return - a JSONArray of guids
   * @throws Exception
   */
  public JSONArray selectQuery(String query) throws Exception {

    return new JSONArray(getResponse(CommandType.SelectQuery, GNSProtocol.QUERY.toString(), query));
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
   * @throws Exception
   */
  public JSONArray selectSetupGroupQuery(GuidEntry accountGuid,
          String publicKey, String query, int interval) throws Exception {
    return new JSONArray(getResponse(CommandType.SelectGroupSetupQuery, GNSProtocol.GUID.toString(), accountGuid.getGuid(),
            GNSProtocol.PUBLIC_KEY.toString(), publicKey, GNSProtocol.QUERY.toString(), query, GNSProtocol.INTERVAL.toString(), interval));
  }

  /**
   * Look up the value of a context aware group guid using a query. Returns
   * the result of the query as a JSONArray of guids. The results will be
   * stale if the queries that happen more quickly than the refresh interval
   * given during setup.
   *
   * @param guid
   * @return a JSONArray of guids
   * @throws Exception
   */
  public JSONArray selectLookupGroupQuery(String guid) throws Exception {
    return new JSONArray(getResponse(CommandType.SelectGroupLookupQuery, GNSProtocol.GUID.toString(), guid));
  }

  // ACCOUNT COMMANDS
  /**
   * Obtains the guid of the alias from the GNS server.
   *
   * @param alias
   * @return guid
   * @throws IOException
   * @throws UnsupportedEncodingException
   * @throws ClientException
   */
  public String lookupGuid(String alias) throws IOException, ClientException {

    return CommandUtils.specialCaseSingleField(getResponse(CommandType.LookupGuid, GNSProtocol.NAME.toString(), alias));
  }

  /**
   * If this is a sub guid returns the account guid it was created under.
   *
   * @param guid
   * @return Account guid of {@code guid}
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws ClientException
   */
  public String lookupPrimaryGuid(String guid)
          throws UnsupportedEncodingException, IOException, ClientException {
    return CommandUtils.specialCaseSingleField(getResponse(CommandType.LookupPrimaryGuid, GNSProtocol.GUID.toString(),
            guid));
  }

  /**
   * Returns a JSON object containing all of the guid meta information. This
   * method returns meta data about the guid. If you want any particular field
   * or fields of the guid you'll need to use one of the read methods.
   *
   * @param guid
   * @return {@code guid} meta info
   * @throws IOException
   * @throws ClientException
   */
  public JSONObject lookupGuidRecord(String guid) throws IOException,
          ClientException {
    try {
      return new JSONObject(getResponse(CommandType.LookupGuidRecord, GNSProtocol.GUID.toString(), guid));
    } catch (JSONException e) {
      throw new ClientException(
              "Failed to parse LOOKUP_GUID_RECORD response", e);
    }
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
   * @throws IOException
   * @throws ClientException
   */
  public JSONObject lookupAccountRecord(String accountGuid)
          throws IOException, ClientException {
    try {
      return new JSONObject(getResponse(CommandType.LookupAccountRecord, GNSProtocol.GUID.toString(), accountGuid));
    } catch (JSONException e) {
      throw new ClientException("Failed to parse LOOKUP_ACCOUNT_RECORD response", e);
    }
  }

  /**
   * Get the public key for a given alias.
   *
   * @param alias
   * @return the public key registered for the alias
   * @throws InvalidGuidException
   * @throws ClientException
   * @throws IOException
   */
  public PublicKey publicKeyLookupFromAlias(String alias)
          throws InvalidGuidException, ClientException, IOException {

    String guid = lookupGuid(alias);
    return publicKeyLookupFromGuid(guid);
  }

  /**
   * Get the public key for a given guid.
   *
   * @param guid
   * @return Public key for {@code guid}
   * @throws InvalidGuidException
   * @throws ClientException
   * @throws IOException
   */
  public PublicKey publicKeyLookupFromGuid(String guid)
          throws InvalidGuidException, ClientException, IOException {
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
   * @throws Exception
   */
  public GuidEntry accountGuidCreate(String alias, String password)
          throws Exception {

    GuidEntry entry = lookupOrCreateGuidEntry(getGNSProvider(), alias);
    assert (entry != null);
    String returnedGuid = accountGuidCreateHelper(alias, password, CommandType.RegisterAccount, entry);
    // Anything else we want to do here?
    if (!returnedGuid.equals(entry.guid)) {
      GNSClientConfig
              .getLogger()
              .log(Level.WARNING,
                      "Returned guid {0} doesn''t match locally created guid {1}",
                      new Object[]{returnedGuid, entry.guid});
    }
    assert returnedGuid.equals(entry.guid);
    return entry;
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
   * @throws Exception
   */
  public GuidEntry accountGuidCreateSecure(String alias, String password)
          throws Exception {

    GuidEntry entry = lookupOrCreateGuidEntry(getGNSProvider(), alias);
    assert (entry != null);
    String returnedGuid = accountGuidCreateHelper(alias, password, CommandType.RegisterAccountSecured, entry);
    // Anything else we want to do here?
    if (!returnedGuid.equals(entry.guid)) {
      GNSClientConfig
              .getLogger()
              .log(Level.WARNING,
                      "Returned guid {0} doesn''t match locally created guid {1}",
                      new Object[]{returnedGuid, entry.guid});
    }
    assert returnedGuid.equals(entry.guid);
    return entry;
  }

  /**
   * Verify an account by sending the verification code back to the server.
   *
   * @param guid
   * the account guid to verify
   * @param code
   * the verification code
   * @return ?
   * @throws Exception
   */
  public String accountGuidVerify(GuidEntry guid, String code)
          throws Exception {
    return getResponse(CommandType.VerifyAccount, guid, GNSProtocol.GUID.toString(),
            guid.getGuid(), GNSProtocol.CODE.toString(), code);
  }

  /**
   *
   * @param guid
   * @return the email
   * @throws Exception
   */
  public String accountResendAuthenticationEmail(GuidEntry guid)
          throws Exception {
    return getResponse(CommandType.ResendAuthenticationEmail, guid, GNSProtocol.GUID.toString(),
            guid.getGuid());
  }

  /**
   * Deletes the account given by name
   *
   * @param guid
   * @throws Exception
   */
  public void accountGuidRemove(GuidEntry guid) throws Exception {
    getResponse(CommandType.RemoveAccount, guid, GNSProtocol.GUID.toString(), guid.getGuid(),
            GNSProtocol.NAME.toString(), guid.getEntityName());
  }

  /**
   * Deletes the account given by name using the password to authenticate.
   *
   * @param name
   * @param password
   * @throws Exception
   */
  public void accountGuidRemoveWithPassword(String name, String password) throws Exception {
    String encodedPassword = Password.encryptAndEncodePassword(password, name);
    getResponse(CommandType.RemoveAccountWithPassword, GNSProtocol.NAME.toString(), name, GNSProtocol.PASSWORD.toString(), encodedPassword);
  }

  /**
   * Creates an new guid associated with an account on the GNS server.
   *
   * @param accountGuid
   * @param alias
   * the alias
   * @return the newly created guid entry
   * @throws Exception
   */
  public GuidEntry guidCreate(GuidEntry accountGuid, String alias)
          throws Exception {

    long startTime = System.currentTimeMillis();
    GuidEntry entry = GuidUtils.createAndSaveGuidEntry(alias,
            getGNSProvider());
    DelayProfiler.updateDelay("updatePreferences", startTime);
    String returnedGuid = guidCreateHelper(accountGuid, alias,
            entry.getPublicKey());
    assert returnedGuid.equals(entry.getGuid());
    // Anything else we want to do here?
    if (!returnedGuid.equals(entry.getGuid())) {
      GNSClientConfig.getLogger().log(Level.WARNING,
              "Returned guid {0}doesn''t match locally created guid{1}",
              new Object[]{returnedGuid, entry.getGuid()});
    }
    DelayProfiler.updateDelay("guidCreateFromAlias", startTime);
    return entry;
  }

  /**
   * Batch create guids with the given aliases. If createPublicKeys is true,
   * key pairs will be created and saved by the client for the guids. If not,
   * bogus public keys will be uses which will make the guids only accessible
   * using the account guid (which has ACL access to each guid).
   *
   * @param accountGuid
   * @param aliases
   * @return ???
   * @throws Exception
   */
  public String guidBatchCreate(GuidEntry accountGuid, Set<String> aliases)
          throws Exception {

    List<String> aliasList = new ArrayList<>(aliases);
    List<String> publicKeys = null;
    long publicKeyStartTime = System.currentTimeMillis();
    publicKeys = new ArrayList<>();
    for (String alias : aliasList) {
      long singleEntrystartTime = System.currentTimeMillis();
      GuidEntry entry = GuidUtils.createAndSaveGuidEntry(alias,
              getGNSProvider());
      DelayProfiler.updateDelay("updateOnePreference",
              singleEntrystartTime);
      byte[] publicKeyBytes = entry.getPublicKey().getEncoded();
      String publicKeyString = Base64.encodeToString(publicKeyBytes,
              false);
      publicKeys.add(publicKeyString);
    }
    DelayProfiler.updateDelay("batchCreatePublicKeys", publicKeyStartTime);

    return getResponse(CommandType.AddMultipleGuids, accountGuid, GNSProtocol.GUID.toString(),
            accountGuid.getGuid(), GNSProtocol.NAMES.toString(), new JSONArray(aliasList),
            GNSProtocol.PUBLIC_KEYS.toString(), new JSONArray(publicKeys));
  }

  /**
   * Removes a guid (not for account Guids - use removeAccountGuid for them).
   *
   * @param guid
   * the guid to remove
   * @throws Exception
   */
  public void guidRemove(GuidEntry guid) throws Exception {
    getResponse(CommandType.RemoveGuidNoAccount, guid, GNSProtocol.GUID.toString(), guid.getGuid());
  }

  /**
   * Removes a guid given the guid and the associated account guid.
   *
   * @param accountGuid
   * @param guidToRemove
   * @throws Exception
   */
  public void guidRemove(GuidEntry accountGuid, String guidToRemove)
          throws Exception {
    getResponse(CommandType.RemoveGuid, accountGuid, GNSProtocol.ACCOUNT_GUID.toString(),
            accountGuid.getGuid(), GNSProtocol.GUID.toString(), guidToRemove);
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
   * @throws InvalidGuidException
   * if the group guid is invalid
   */
  public JSONArray groupGetMembers(String groupGuid, GuidEntry reader)
          throws IOException, ClientException, InvalidGuidException {
    try {
      return new JSONArray(getResponse(CommandType.GetGroupMembers,
              reader, GNSProtocol.GUID.toString(), groupGuid, GNSProtocol.READER.toString(), reader.getGuid()));
    } catch (JSONException e) {
      throw new ClientException("Invalid member list", e);
    }
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
   * @throws InvalidGuidException
   * if the group guid is invalid
   */
  public JSONArray guidGetGroups(String guid, GuidEntry reader)
          throws IOException, ClientException, InvalidGuidException {
    try {
      return new JSONArray(getResponse(CommandType.GetGroups, reader,
              GNSProtocol.GUID.toString(), guid, GNSProtocol.READER.toString(), reader.getGuid()));
    } catch (JSONException e) {
      throw new ClientException("Invalid member list", e);
    }
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
   * @throws IOException
   * @throws InvalidGuidException
   * if the group guid does not exist
   * @throws ClientException
   */
  public void groupAddGuid(String groupGuid, String guidToAdd,
          GuidEntry writer) throws IOException, InvalidGuidException,
          ClientException {
    getResponse(CommandType.AddToGroup, writer, GNSProtocol.GUID.toString(), groupGuid, GNSProtocol.MEMBER.toString(),
            guidToAdd, GNSProtocol.WRITER.toString(), writer.getGuid());
  }

  /**
   * Add multiple members to a group
   *
   * @param groupGuid
   * guid of the group
   * @param members
   * guids of members to add to the group
   * @param writer
   * the guid doing the add
   * @throws IOException
   * @throws InvalidGuidException
   * @throws ClientException
   */
  public void groupAddGuids(String groupGuid, JSONArray members,
          GuidEntry writer) throws IOException, ClientException {
    getResponse(CommandType.AddMembersToGroup, writer, GNSProtocol.GUID.toString(), groupGuid,
            GNSProtocol.MEMBERS.toString(), members, GNSProtocol.WRITER.toString(), writer.getGuid());
  }

  /**
   * Removes a guid from a group guid. Any guid can be a group guid. Signs the
   * query using the private key of the user associated with the writer.
   *
   * @param guid
   * guid of the group
   * @param guidToRemove
   * guid to remove from the group
   * @param writer
   * the guid of the entity doing the remove
   * @throws IOException
   * @throws InvalidGuidException
   * if the group guid does not exist
   * @throws ClientException
   */
  public void groupRemoveGuid(String guid, String guidToRemove,
          GuidEntry writer) throws IOException, InvalidGuidException,
          ClientException {
    getResponse(CommandType.RemoveFromGroup, writer, GNSProtocol.GUID.toString(), guid, GNSProtocol.MEMBER.toString(),
            guidToRemove, GNSProtocol.WRITER.toString(), writer.getGuid());
  }

  /**
   * Remove a list of members from a group
   *
   * @param guid
   * guid of the group
   * @param members
   * guids to remove from the group
   * @param writer
   * the guid of the entity doing the remove
   * @throws IOException
   * @throws InvalidGuidException
   * @throws ClientException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   */
  public void groupRemoveGuids(String guid, JSONArray members,
          GuidEntry writer) throws IOException, InvalidGuidException,
          ClientException, InvalidKeyException, NoSuchAlgorithmException,
          SignatureException {
    getResponse(CommandType.RemoveMembersFromGroup, writer, GNSProtocol.GUID.toString(), guid,
            GNSProtocol.MEMBERS.toString(), members, GNSProtocol.WRITER.toString(), writer.getGuid());
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
   * @throws Exception
   */
  public void groupAddMembershipUpdatePermission(GuidEntry groupGuid,
          String guidToAuthorize) throws Exception {
    aclAdd(AclAccessType.WRITE_WHITELIST, groupGuid, GNSProtocol.GROUP_ACL.toString(),
            guidToAuthorize);
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
   * @throws Exception
   */
  public void groupRemoveMembershipUpdatePermission(GuidEntry groupGuid,
          String guidToUnauthorize) throws Exception {
    aclRemove(AclAccessType.WRITE_WHITELIST, groupGuid, GNSProtocol.GROUP_ACL.toString(),
            guidToUnauthorize);
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
   * @throws Exception
   */
  public void groupAddMembershipReadPermission(GuidEntry groupGuid,
          String guidToAuthorize) throws Exception {
    aclAdd(AclAccessType.READ_WHITELIST, groupGuid, GNSProtocol.GROUP_ACL.toString(),
            guidToAuthorize);
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
   * @throws Exception
   */
  public void groupRemoveMembershipReadPermission(GuidEntry groupGuid,
          String guidToUnauthorize) throws Exception {
    aclRemove(AclAccessType.READ_WHITELIST, groupGuid, GNSProtocol.GROUP_ACL.toString(),
            guidToUnauthorize);
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
   * @throws Exception
   * @throws ClientException
   * if the query is not accepted by the server.
   */
  public void aclAdd(AclAccessType accessType, GuidEntry targetGuid,
          String field, String accesserGuid) throws Exception {
    aclAdd(accessType.name(), targetGuid, field, accesserGuid);
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
   * @param guid
   * @param field
   * @param accesserGuid
   * @throws Exception
   * @throws ClientException
   * if the query is not accepted by the server.
   */
  public void aclRemove(AclAccessType accessType, GuidEntry guid,
          String field, String accesserGuid) throws Exception {
    aclRemove(accessType.name(), guid, field, accesserGuid);
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
   * @param guid
   * @param field
   * @param readerGuid
   * @return list of GUIDs for that ACL
   * @throws Exception
   * @throws ClientException
   * if the query is not accepted by the server.
   */
  public JSONArray aclGet(AclAccessType accessType, GuidEntry guid,
          String field, String readerGuid) throws Exception {
    return aclGet(accessType.name(), guid, field, readerGuid);
  }

  /**
   *
   * @param accessType
   * @param guid
   * @param field
   * @param writerGuid
   * @throws Exception
   */
  public void fieldCreateAcl(AclAccessType accessType, GuidEntry guid, String field,
          String writerGuid) throws Exception {
    getResponse(CommandType.FieldCreateAcl, guid, GNSProtocol.ACL_TYPE.toString(), accessType.name(), GNSProtocol.GUID.toString(),
            guid.getGuid(), GNSProtocol.FIELD.toString(), field, GNSProtocol.WRITER.toString(), writerGuid);
  }

  /**
   *
   * @param accessType
   * @param guid
   * @param field
   * @throws Exception
   */
  public void fieldCreateAcl(AclAccessType accessType, GuidEntry guid, String field) throws Exception {
    GNSClientCommands.this.fieldCreateAcl(accessType, guid, field, guid.getGuid());
  }

  /**
   *
   * @param accessType
   * @param guid
   * @param field
   * @param writerGuid
   * @throws Exception
   */
  public void fieldDeleteAcl(AclAccessType accessType, GuidEntry guid, String field,
          String writerGuid) throws Exception {
    getResponse(CommandType.FieldDeleteAcl, guid, GNSProtocol.ACL_TYPE.toString(), accessType.name(),
            GNSProtocol.GUID.toString(), guid.getGuid(), GNSProtocol.FIELD.toString(), field, GNSProtocol.WRITER.toString(), writerGuid);
  }

  /**
   *
   * @param accessType
   * @param guid
   * @param field
   * @throws Exception
   */
  public void fieldDeleteAcl(AclAccessType accessType, GuidEntry guid, String field) throws Exception {
    GNSClientCommands.this.fieldDeleteAcl(accessType, guid, field, guid.getGuid());
  }

  /**
   *
   * @param accessType
   * @param guid
   * @param field
   * @param readerGuid
   * @return
   * @throws Exception
   */
  public boolean fieldAclExists(AclAccessType accessType, GuidEntry guid, String field,
          String readerGuid) throws Exception {
    return Boolean.valueOf(getResponse(CommandType.FieldAclExists, guid, GNSProtocol.ACL_TYPE.toString(), accessType.name(),
            GNSProtocol.GUID.toString(), guid.getGuid(), GNSProtocol.FIELD.toString(), field, GNSProtocol.READER.toString(), readerGuid));
  }

  /**
   *
   * @param accessType
   * @param guid
   * @param field
   * @return
   * @throws Exception
   */
  public boolean fieldAclExists(AclAccessType accessType, GuidEntry guid, String field) throws Exception {
    return GNSClientCommands.this.fieldAclExists(accessType, guid, field, guid.getGuid());
  }

  // ALIASES
  /**
   * Creates an alias entity name for the given guid. The alias can be used
   * just like the original entity name.
   *
   * @param guid
   * @param name
   * - the alias
   * @throws Exception
   */
  public void addAlias(GuidEntry guid, String name) throws Exception {
    getResponse(CommandType.AddAlias, guid, GNSProtocol.GUID.toString(), guid.getGuid(), GNSProtocol.NAME.toString(),
            name);
  }

  /**
   * Removes the alias for the given guid.
   *
   * @param guid
   * @param name
   * - the alias
   * @throws Exception
   */
  public void removeAlias(GuidEntry guid, String name) throws Exception {
    getResponse(CommandType.RemoveAlias, guid, GNSProtocol.GUID.toString(), guid.getGuid(), GNSProtocol.NAME.toString(),
            name);
  }

  /**
   * Retrieve the aliases associated with the given guid.
   *
   * @param guid
   * @return - a JSONArray containing the aliases
   * @throws Exception
   */
  public JSONArray getAliases(GuidEntry guid) throws Exception {
    try {
      return new JSONArray(getResponse(CommandType.RetrieveAliases, guid,
              GNSProtocol.GUID.toString(), guid.getGuid()));
    } catch (JSONException e) {
      throw new ClientException("Invalid alias list", e);
    }
  }

  // ///////////////////////////////
  // // PRIVATE METHODS BELOW /////
  // /////////////////////////////
  /**
   * Creates a new guid associated with an account.
   *
   * @param accountGuid
   * @param name
   * @param publicKey
   * @return the guid string
   * @throws Exception
   */
  private String guidCreateHelper(GuidEntry accountGuid, String name,
          PublicKey publicKey) throws Exception {
    long startTime = System.currentTimeMillis();
    byte[] publicKeyBytes = publicKey.getEncoded();
    String publicKeyString = Base64.encodeToString(publicKeyBytes, false);
    String result = getResponse(CommandType.AddGuid, accountGuid, GNSProtocol.GUID.toString(),
            accountGuid.getGuid(), GNSProtocol.NAME.toString(), name, GNSProtocol.PUBLIC_KEY.toString(), publicKeyString);
    DelayProfiler.updateDelay("guidCreate", startTime);
    return result;
  }

  private GuidEntry lookupOrCreateGuidEntry(String gnsInstance,
          String alias) throws NoSuchAlgorithmException, EncryptionException {
     GuidEntry entry = GuidUtils.lookupGuidEntryFromDatabase(this, alias);
    /* arun: Don't recreate pair if one already exists. Otherwise you can
     * not get out of the funk where the account creation timed out but
     * wasn't rolled back fully at the server. Re-using
     * the same guid will at least pass verification as opposed to 
     * incurring an GNSProtocol.ACTIVE_REPLICA_EXCEPTION.toString() for a new (non-existent) guid.
     */
    if (entry == null) {
      KeyPair keyPair = KeyPairGenerator.getInstance(GNSProtocol.RSA_ALGORITHM.toString())
              .generateKeyPair();
      String guid = SharedGuidUtils.createGuidStringFromPublicKey(keyPair
              .getPublic().getEncoded());
      // Squirrel this away now just in case the call below times out.
      KeyPairUtils.saveKeyPair(gnsInstance, alias, guid, keyPair);
      entry = new GuidEntry(alias, guid, keyPair.getPublic(),
              keyPair.getPrivate());
    }
    return entry;
  }
  
  /**
   * Register a new account guid with the corresponding alias and the given
   * public key on the GNS server. Returns a new guid.
   *
   * @param alias
   * the alias to register (usually an email address)
   * @param publicKey
   * the public key associate with the account
   * @return guid the guid generated by the GNS
   * @throws IOException
   * @throws UnsupportedEncodingException
   * @throws ClientException
   * @throws InvalidGuidException
   * if the user already exists
   */
  private String accountGuidCreateHelper(String alias, String password, 
          CommandType commandType, GuidEntry guidEntry)
          throws UnsupportedEncodingException, IOException, ClientException,
          InvalidGuidException, NoSuchAlgorithmException {
    long startTime = System.currentTimeMillis();
    String result = getResponse(commandType, guidEntry, GNSProtocol.NAME.toString(), alias,
            GNSProtocol.PUBLIC_KEY.toString(), Base64.encodeToString(
                    guidEntry.publicKey.getEncoded(), false), GNSProtocol.PASSWORD.toString(),
            password != null
                    ? Password.encryptAndEncodePassword(password, alias)
                    : "");
    DelayProfiler.updateDelay("accountGuidCreate", startTime);
    return result;
  }

  private void aclAdd(String accessType, GuidEntry guid, String field,
          String accesserGuid) throws Exception {
    getResponse(CommandType.AclAddSelf, guid, GNSProtocol.ACL_TYPE.toString(), accessType, GNSProtocol.GUID.toString(),
            guid.getGuid(), GNSProtocol.FIELD.toString(), field, GNSProtocol.ACCESSER.toString(),
            accesserGuid == null ? GNSProtocol.ALL_GUIDS.toString() : accesserGuid);
  }

  private void aclRemove(String accessType, GuidEntry guid, String field,
          String accesserGuid) throws Exception {
    getResponse(CommandType.AclRemoveSelf, guid, GNSProtocol.ACL_TYPE.toString(), accessType,
            GNSProtocol.GUID.toString(), guid.getGuid(), GNSProtocol.FIELD.toString(), field, GNSProtocol.ACCESSER.toString(),
            accesserGuid == null ? GNSProtocol.ALL_GUIDS.toString() : accesserGuid);
  }

  private JSONArray aclGet(String accessType, GuidEntry guid, String field,
          String readerGuid) throws Exception {
    try {
      return new JSONArray(getResponse(CommandType.AclRetrieve, guid,
              GNSProtocol.ACL_TYPE.toString(), accessType, GNSProtocol.GUID.toString(), guid.getGuid(), GNSProtocol.FIELD.toString(), field,
              GNSProtocol.READER.toString(), readerGuid == null ? GNSProtocol.ALL_GUIDS.toString() : readerGuid));
    } catch (JSONException e) {
      throw new ClientException("Invalid ACL list", e);
    }
  }

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
   * @throws IOException
   * @throws ClientException
   */
  public void fieldCreateList(String targetGuid, String field,
          JSONArray value, GuidEntry writer) throws IOException,
          ClientException {
    getResponse(CommandType.CreateList, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(),
            field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(), writer.getGuid());
  }

  /**
   * Appends the values of the field onto list of values or creates a new
   * field with values in the list if it does not exist.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws IOException
   * @throws ClientException
   */
  public void fieldAppendOrCreateList(String targetGuid, String field,
          JSONArray value, GuidEntry writer) throws IOException,
          ClientException {
    getResponse(CommandType.AppendOrCreateList, writer, GNSProtocol.GUID.toString(), targetGuid,
            GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(), writer.getGuid());
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
   * @throws ClientException
   */
  public void fieldReplaceOrCreateList(String targetGuid, String field,
          JSONArray value, GuidEntry writer) throws IOException,
          ClientException {
    getResponse(CommandType.ReplaceOrCreateList, writer, GNSProtocol.GUID.toString(), targetGuid,
            GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(), writer.getGuid());
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
   * guid entry of the writer
   * @throws IOException
   * @throws ClientException
   */
  public void fieldAppend(String targetGuid, String field, JSONArray value,
          GuidEntry writer) throws IOException, ClientException {
    getResponse(CommandType.AppendListWithDuplication, writer, GNSProtocol.GUID.toString(),
            targetGuid, GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(),
            writer.getGuid());
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
   * @throws IOException
   * @throws ClientException
   */
  public void fieldReplaceList(String targetGuid, String field,
          JSONArray value, GuidEntry writer) throws IOException,
          ClientException {
    getResponse(CommandType.ReplaceList, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(),
            field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(), writer.getGuid());
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
   * @throws IOException
   * @throws ClientException
   */
  public void fieldClear(String targetGuid, String field, JSONArray value,
          GuidEntry writer) throws IOException, ClientException {
    getResponse(CommandType.RemoveList, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(),
            field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(), writer.getGuid());
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
   * @throws IOException
   * @throws ClientException
   */
  public void fieldClear(String targetGuid, String field, GuidEntry writer)
          throws IOException, ClientException {
    getResponse(CommandType.Clear, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(), field,
            GNSProtocol.WRITER.toString(), writer.getGuid());
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
   * @throws Exception
   */
  public JSONArray fieldReadArray(String guid, String field, GuidEntry reader)
          throws Exception {
    return commandResponseToJSONArray(field,
            (getResponse(reader != null ? CommandType.ReadArray
                    : CommandType.ReadArrayUnsigned, reader, GNSProtocol.GUID.toString(), guid, GNSProtocol.FIELD.toString(),
                    field, GNSProtocol.READER.toString(), reader != null ? reader.getGuid() : null)));
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
   * @throws IOException
   * @throws ClientException
   */
  public void fieldSetElement(String targetGuid, String field,
          String newValue, int index, GuidEntry writer) throws IOException,
          ClientException {
    getResponse(CommandType.Set, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(), field,
            GNSProtocol.VALUE.toString(), newValue, GNSProtocol.N.toString(), Integer.toString(index), GNSProtocol.WRITER.toString(),
            writer.getGuid());
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
   * @throws ClientException
   */
  public void fieldSetNull(String targetGuid, String field, GuidEntry writer)
          throws IOException, InvalidKeyException, NoSuchAlgorithmException,
          SignatureException, ClientException {
    getResponse(CommandType.SetFieldNull, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(),
            field, GNSProtocol.WRITER.toString(), writer.getGuid());
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
   * @throws Exception
   */
  public JSONArray select(String field, String value) throws Exception {
    return new JSONArray(getResponse(CommandType.Select,
            GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value));
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
   * @throws Exception
   */
  public JSONArray selectWithin(String field, JSONArray value)
          throws Exception {
    return new JSONArray(getResponse(CommandType.SelectWithin, GNSProtocol.FIELD.toString(), field, GNSProtocol.WITHIN.toString(),
            value));
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
   * @throws Exception
   */
  public JSONArray selectNear(String field, JSONArray value,
          Double maxDistance) throws Exception {
    return new JSONArray(getResponse(CommandType.SelectNear, GNSProtocol.FIELD.toString(), field, GNSProtocol.NEAR.toString(), value,
            GNSProtocol.MAX_DISTANCE.toString(), Double.toString(maxDistance)));
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
   * @throws Exception
   * if a GNS error occurs
   */
  public void setLocation(String targetGuid, double longitude,
          double latitude, GuidEntry writer) throws Exception {
    fieldReplaceOrCreateList(targetGuid, GNSProtocol.LOCATION_FIELD_NAME.toString(),
            new JSONArray(Arrays.asList(longitude, latitude)), writer);
  }

  /**
   * Update the location field for the given GNSProtocol.GUID.toString()
   *
   * @param longitude
   * the guid longitude
   * @param latitude
   * the guid latitude
   * @param guid
   * the guid to update
   * @throws Exception
   * if a GNS error occurs
   */
  public void setLocation(GuidEntry guid, double longitude, double latitude)
          throws Exception {
    setLocation(guid.getGuid(), longitude, latitude, guid);
  }

  /**
   * Get the location of the target GNSProtocol.GUID.toString() as a JSONArray: [LONG, LAT]
   *
   * @param readerGuid
   * the guid issuing the request
   * @param targetGuid
   * the guid that we want to know the location
   * @return a JSONArray: [LONGITUDE, LATITUDE]
   * @throws Exception
   * if a GNS error occurs
   */
  public JSONArray getLocation(String targetGuid, GuidEntry readerGuid)
          throws Exception {
    return fieldReadArray(targetGuid, GNSProtocol.LOCATION_FIELD_NAME.toString(), readerGuid);
  }

  /**
   * Get the location of the target guid as a JSONArray: [LONG, LAT]
   *
   * @param guid
   * @return a JSONArray: [LONGITUDE, LATITUDE]
   * @throws Exception
   * if a GNS error occurs
   */
  public JSONArray getLocation(GuidEntry guid) throws Exception {
    return fieldReadArray(guid.getGuid(), GNSProtocol.LOCATION_FIELD_NAME.toString(), guid);
  }

  /**
   * @param guid
   * @param action
   * @param writerGuid
   * @throws ClientException
   * @throws IOException
   */
  // Active Code
  public void activeCodeClear(String guid, String action, GuidEntry writerGuid)
          throws ClientException, IOException {
    getResponse(CommandType.ClearCode, writerGuid, GNSProtocol.GUID.toString(), guid,
            GNSProtocol.AC_ACTION.toString(), action, GNSProtocol.WRITER.toString(), writerGuid.getGuid());
  }

  /**
   * @param guid
   * @param action
   * @param code
   * @param writerGuid
   * @throws ClientException
   * @throws IOException
   */
  public void activeCodeSet(String guid, String action, String code,
          GuidEntry writerGuid) throws ClientException, IOException {
    getResponse(CommandType.SetCode, writerGuid, GNSProtocol.GUID.toString(), guid,
            GNSProtocol.AC_ACTION.toString(), action, GNSProtocol.AC_CODE.toString(), code,
            GNSProtocol.WRITER.toString(), writerGuid.getGuid());
  }

  /**
   * @param guid
   * @param action
   * @param readerGuid
   * @return Active code of {@code guid} as byte[]
   * @throws Exception
   */
  public String activeCodeGet(String guid, String action, GuidEntry readerGuid)
          throws Exception {
    String code = getResponse(CommandType.GetCode,
            readerGuid, GNSProtocol.GUID.toString(), guid, GNSProtocol.AC_ACTION.toString(), action, GNSProtocol.READER.toString(),
            readerGuid.getGuid());
    return code;
  }

  // Extended commands
  /**
   * Creates a new field in the target guid with value being the list.
   *
   * @param target
   * @param field
   * @param value
   * @throws IOException
   * @throws ClientException
   */
  public void fieldCreateList(GuidEntry target, String field, JSONArray value)
          throws IOException, ClientException {
    fieldCreateList(target.getGuid(), field, value, target);
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
   * @throws IOException
   * @throws ClientException
   */
  public void fieldCreateOneElementList(String targetGuid, String field,
          String value, GuidEntry writer) throws IOException, ClientException {
    getResponse(CommandType.Create, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(),
            field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(), writer.getGuid());
  }

  /**
   * Creates a new one element field in the target guid with single element
   * value being the string.
   *
   * @param target
   * @param field
   * @param value
   * @throws IOException
   * @throws ClientException
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
   * @throws IOException
   * @throws ClientException
   */
  public void fieldAppendOrCreate(String targetGuid, String field,
          String value, GuidEntry writer) throws IOException, ClientException {
    getResponse(CommandType.AppendOrCreate, writer, GNSProtocol.GUID.toString(), targetGuid,
            GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(), writer.getGuid());
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
   * @throws IOException
   * @throws ClientException
   */
  public void fieldReplaceOrCreate(String targetGuid, String field,
          String value, GuidEntry writer) throws IOException, ClientException {
    getResponse(CommandType.ReplaceOrCreate, writer, GNSProtocol.GUID.toString(), targetGuid,
            GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(), writer.getGuid());
  }

  /**
   * Replaces the values of the field with the list of values or creates a new
   * field with values in the list if it does not exist.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @throws IOException
   * @throws ClientException
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
   * @throws IOException
   * @throws ClientException
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
   * @throws IOException
   * @throws ClientException
   */
  public void fieldReplace(String targetGuid, String field, String value,
          GuidEntry writer) throws IOException, ClientException {
    getResponse(CommandType.Replace, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(),
            field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(), writer.getGuid());
  }

  /**
   * Replaces all the values of field in target with with the single value.
   *
   * @param target
   * @param field
   * @param value
   * @throws IOException
   * @throws ClientException
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
   * @throws IOException
   * @throws ClientException
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
   * @throws IOException
   * @throws ClientException
   */
  public void fieldAppend(String targetGuid, String field, String value,
          GuidEntry writer) throws IOException, ClientException {
    getResponse(CommandType.AppendWithDuplication, writer, GNSProtocol.GUID.toString(),
            targetGuid, GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(),
            writer.getGuid());
  }

  /**
   * Appends a single value onto a field in the target guid.
   *
   * @param target
   * @param field
   * @param value
   * @throws IOException
   * @throws ClientException
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
   * @throws IOException
   * @throws ClientException
   */
  public void fieldAppendWithSetSemantics(String targetGuid, String field,
          JSONArray value, GuidEntry writer) throws IOException,
          ClientException {
    getResponse(CommandType.AppendList, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(),
            field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(), writer.getGuid());
  }

  /**
   * Appends a list of values onto a field in target but converts the list to
   * set removing duplicates.
   *
   * @param target
   * @param field
   * @param value
   * @throws IOException
   * @throws ClientException
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
   * @throws IOException
   * @throws ClientException
   */
  public void fieldAppendWithSetSemantics(String targetGuid, String field,
          String value, GuidEntry writer) throws IOException, ClientException {
    getResponse(CommandType.Append, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(), field,
            GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(), writer.getGuid());
  }

  /**
   * Appends a single value onto a field in target but converts the list to
   * set removing duplicates.
   *
   * @param target
   * @param field
   * @param value
   * @throws IOException
   * @throws ClientException
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
    if (writer == null) {
      throw new ClientException(
              "Can not perform an update without querier information");
    }
    getResponse(CommandType.Replace, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(),
            field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(), writer != null ? writer.getGuid()
                    : null);
  }

  /**
   * For testing only.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws IOException
   * @throws ClientException
   */
  @Deprecated
  public void fieldReplaceFirstElementTest(String targetGuid, String field,
          String value, GuidEntry writer) throws IOException, ClientException {
    if (writer == null) {
      getResponse(CommandType.ReplaceUnsigned, GNSProtocol.GUID.toString(),
              targetGuid, GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value);
    } else {
      this.fieldReplaceFirstElement(targetGuid, field, value, writer);
    }
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
   * @throws IOException
   * @throws ClientException
   */
  public void fieldSubstitute(String targetGuid, String field,
          String newValue, String oldValue, GuidEntry writer)
          throws IOException, ClientException {
    getResponse(CommandType.Substitute, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(),
            field, GNSProtocol.VALUE.toString(), newValue, GNSProtocol.OLD_VALUE.toString(), oldValue, GNSProtocol.WRITER.toString(),
            writer.getGuid());
  }

  /**
   * Substitutes the value for oldValue in the list of values of a field in
   * the target.
   *
   * @param target
   * @param field
   * @param newValue
   * @param oldValue
   * @throws IOException
   * @throws ClientException
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
   * @throws IOException
   * @throws ClientException
   */
  public void fieldSubstitute(String targetGuid, String field,
          JSONArray newValue, JSONArray oldValue, GuidEntry writer)
          throws IOException, ClientException {
    getResponse(CommandType.SubstituteList, writer, GNSProtocol.GUID.toString(), targetGuid,
            GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), newValue, GNSProtocol.OLD_VALUE.toString(),
            oldValue, GNSProtocol.WRITER.toString(), writer.getGuid());
  }

  /**
   * Pairwise substitutes all the values for the oldValues in the list of
   * values of a field in the target.
   *
   * @param target
   * @param field
   * @param newValue
   * @param oldValue
   * @throws IOException
   * @throws ClientException
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
   * @throws Exception
   */
  @Deprecated
  public String fieldReadArrayFirstElement(String guid, String field,
          GuidEntry reader) throws Exception {
    return getResponse(reader != null ? CommandType.ReadArrayOne
            : CommandType.ReadArrayOneUnsigned, reader, GNSProtocol.GUID.toString(), guid, GNSProtocol.FIELD.toString(),
            field, GNSProtocol.READER.toString(), reader != null ? reader.getGuid() : null);
  }

  /**
   * Reads the first value for a key in the guid. Assuming that value is a array.
   * Signs the query using the private key of the guid.
   *
   * @param guid
   * @param field
   * @return First value of {@code field} whose value is expected to be an
   * array.
   * @throws Exception
   */
  @Deprecated
  public String fieldReadArrayFirstElement(GuidEntry guid, String field)
          throws Exception {
    return fieldReadArrayFirstElement(guid.getGuid(), field, guid);
  }

  /**
   * Removes a field in the JSONObject record of the given guid. Signs the
   * query using the private key of the guid. A convenience method™.
   *
   * @param guid
   * @param field
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   * @throws ClientException
   */
  public void fieldRemove(GuidEntry guid, String field) throws IOException,
          InvalidKeyException, NoSuchAlgorithmException, SignatureException,
          ClientException {
    fieldRemove(guid.getGuid(), field, guid);
  }

  /**
   *
   * @return The contents of the GNS.
   * @throws Exception
   */
  public String dump() throws Exception {
    //Create the admin account if it doesn't already exist.
    try {
      accountGuidCreate("Admin",
              GNSConfig.getInternalOpSecret()
      //Config.getGlobalString(GNSConfig.GNSC.INTERNAL_OP_SECRET)
      );
    } catch (DuplicateNameException dne) {
      //Do nothing if it already exists.
    }
    return getResponse(CommandType.Dump, GNSProtocol.NAME.toString(),
            "Admin");
  }

  @Override
  public void close() {
    super.close();
  }
}
