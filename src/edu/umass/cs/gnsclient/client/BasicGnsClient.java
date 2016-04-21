/*
 * Copyright (C) 2016
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsclient.client;

import static edu.umass.cs.gnsclient.client.CommandUtils.checkResponse;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.client.util.Password;
import edu.umass.cs.gnscommon.GnsProtocol;
import static edu.umass.cs.gnscommon.GnsProtocol.*;
import edu.umass.cs.gnscommon.exceptions.client.EncryptionException;
import edu.umass.cs.gnscommon.exceptions.client.GnsClientException;
import edu.umass.cs.gnscommon.exceptions.client.GnsFieldNotFoundException;
import edu.umass.cs.gnscommon.exceptions.client.GnsInvalidGuidException;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandType;
import edu.umass.cs.utils.DelayProfiler;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * 
 * This class defines a client to communicate with a GNS instance over TCP.
 * This class contains a concise set of read and write commands
 * which read and write JSON Objects.
 * See also {@link GnsClient} for the full set of client commands.
 * 
 * It also contains a set of commands to use context aware group guids.
 * 
 * It uses AbstractGnsClient to provide the essential connectivity to the server.
 * 
 * Uses on gigapaxos' async client.
 *
 * @author westy
 */
public class BasicGnsClient extends AbstractGnsClient implements GNSClientInterface {

  /**
   * Creates a new client for communication with the GNS.
   * Has the basic command functionality needed to created records
   * and read and write fields.
   *
   * @param anyReconfigurator
   * @param disableSSL
   * @throws IOException
   */
  @Deprecated
  public BasicGnsClient(InetSocketAddress anyReconfigurator, boolean disableSSL)
          throws IOException {
    super(anyReconfigurator, disableSSL);
  }

  /**
   * Creates a new client for communication with the GNS.
   * Has the basic command functionality needed to created records
   * and read and write fields.
   *
   * @param disableSSL
   * @throws IOException
   */
  @Deprecated
  public BasicGnsClient(boolean disableSSL)
          throws IOException {
    this(null, disableSSL);
  }
  
  /**
   * Creates a new client for communication with the GNS.
   * Has the basic command functionality needed to created records
   * and read and write fields.
   *
   * @throws IOException
   */
  public BasicGnsClient()
          throws IOException {
    this(null, false);
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
   * @throws GnsClientException
   */
  public void update(String targetGuid, JSONObject json, GuidEntry writer) throws IOException, GnsClientException {
    JSONObject command = createAndSignCommand(CommandType.ReplaceUserJSON,
            writer.getPrivateKey(),
            REPLACE_USER_JSON, GUID,
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
   * @throws GnsClientException
   */
  public void update(GuidEntry guid, JSONObject json) throws IOException, GnsClientException {
    update(guid.getGuid(), json, guid);
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
   * @throws GnsClientException
   * @throws JSONException
   */
  public void fieldUpdate(String targetGuid, String field, Object value, GuidEntry writer)
          throws IOException, GnsClientException, JSONException {
    JSONObject json = new JSONObject();
    json.put(field, value);
    JSONObject command = createAndSignCommand(CommandType.ReplaceUserJSON,
            writer.getPrivateKey(), REPLACE_USER_JSON,
            GUID, targetGuid, USER_JSON, json.toString(), WRITER, writer.getGuid());
    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  /**
   * Creates an index for a field. The guid is only used for
   * authentication purposes.
   *
   * @param guid
   * @param field
   * @param index
   * @throws IOException
   * @throws GnsClientException
   * @throws JSONException
   */
  public void fieldCreateIndex(GuidEntry guid, String field, String index)
          throws IOException, GnsClientException, JSONException {
    JSONObject command = createAndSignCommand(CommandType.CreateIndex,
            guid.getPrivateKey(), CREATE_INDEX,
            GUID, guid.getGuid(), FIELD, field, VALUE, index, WRITER, guid.getGuid());
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
   * @throws GnsClientException
   * @throws JSONException
   */
  public void fieldUpdate(GuidEntry targetGuid, String field, Object value)
          throws IOException, GnsClientException, JSONException {
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
      command = createCommand(CommandType.ReadArrayUnsigned,
              READ_ARRAY, GUID, targetGuid, FIELD, ALL_FIELDS);
    } else {
      // this one actually uses the old style read... for now
      command = createAndSignCommand(CommandType.ReadArray,
              reader.getPrivateKey(), READ_ARRAY, GUID, targetGuid, FIELD, ALL_FIELDS,
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
      command = createCommand(CommandType.ReadUnsigned,
              READ, GUID, targetGuid, FIELD, field);
    } else {
      command = createAndSignCommand(CommandType.Read,
              reader.getPrivateKey(), READ, GUID, targetGuid, FIELD, field,
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
      command = createCommand(CommandType.ReadUnsigned,
              READ, GUID, targetGuid, FIELD, field);
    } else {
      command = createAndSignCommand(CommandType.Read,
              reader.getPrivateKey(), READ, GUID, targetGuid, FIELD, field,
              READER, reader.getGuid());
    }

    long t = System.currentTimeMillis();
    String response = sendCommandAndWait(command);
    DelayProfiler.updateDelay("fieldRead", t);
    GNSClientConfig.getLogger().fine(DelayProfiler.getStats());

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
      command = createCommand(CommandType.ReadMultiFieldUnsigned,
              READ, GUID, targetGuid, FIELDS, fields);
    } else {
      command = createAndSignCommand(CommandType.ReadMultiField,
              reader.getPrivateKey(), READ, GUID, targetGuid,
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
   * @throws GnsClientException
   */
  public void fieldRemove(String targetGuid, String field, GuidEntry writer) throws IOException, InvalidKeyException,
          NoSuchAlgorithmException, SignatureException, GnsClientException {
    JSONObject command = createAndSignCommand(CommandType.RemoveField,
            writer.getPrivateKey(), REMOVE_FIELD, GUID, targetGuid,
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
    JSONObject command = createCommand(CommandType.SelectQuery,
            SELECT, QUERY, query);
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
    JSONObject command = createCommand(CommandType.SelectGroupSetupQuery,
            SELECT_GROUP, GUID, accountGuid.getGuid(),
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
    JSONObject command = createCommand(CommandType.SelectGroupLookupQuery,
            SELECT_GROUP, GUID, guid);
    String response = sendCommandAndWait(command);

    return new JSONArray(checkResponse(command, response));
  }

  // ACCOUNT COMMANDS
  /**
   * Obtains the guid of the alias from the GNS server.
   *
   * @param alias
   * @return guid
   * @throws IOException
   * @throws UnsupportedEncodingException
   * @throws GnsClientException
   */
  public String lookupGuid(String alias) throws IOException, GnsClientException {
    JSONObject command = createCommand(CommandType.LookupGuid,
            LOOKUP_GUID, NAME, alias);
    String response = sendCommandAndWait(command);

    return checkResponse(command, response);
  }

  /**
   * If this is a sub guid returns the account guid it was created under.
   *
   * @param guid
   * @return
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws GnsClientException
   */
  public String lookupPrimaryGuid(String guid) throws UnsupportedEncodingException, IOException, GnsClientException {
    JSONObject command = createCommand(CommandType.LookupPrimaryGuid,
            LOOKUP_PRIMARY_GUID, GUID, guid);
    String response = sendCommandAndWait(command);

    return checkResponse(command, response);
  }

  /**
   * Returns a JSON object containing all of the guid meta information.
   * This method returns meta data about the guid.
   * If you want any particular field or fields of the guid
   * you'll need to use one of the read methods.
   *
   * @param guid
   * @return
   * @throws IOException
   * @throws GnsClientException
   */
  @Override
  public JSONObject lookupGuidRecord(String guid) throws IOException, GnsClientException {
    JSONObject command = createCommand(CommandType.LookupGuidRecord,
            LOOKUP_GUID_RECORD, GUID, guid);
    String response = sendCommandAndWait(command);
    checkResponse(command, response);
    try {
      return new JSONObject(response);
    } catch (JSONException e) {
      throw new GnsClientException("Failed to parse LOOKUP_GUID_RECORD response", e);
    }
  }

  /**
   * Returns a JSON object containing all of the account meta information for an
   * account guid.
   * This method returns meta data about the account associated with this guid
   * if and only if the guid is an account guid.
   * If you want any particular field or fields of the guid
   * you'll need to use one of the read methods.
   *
   * @param accountGuid
   * @return
   * @throws IOException
   * @throws GnsClientException
   */
  public JSONObject lookupAccountRecord(String accountGuid) throws IOException, GnsClientException {
    JSONObject command = createCommand(CommandType.LookupAccountRecord,
            LOOKUP_ACCOUNT_RECORD, GUID, accountGuid);
    String response = sendCommandAndWait(command);
    checkResponse(command, response);
    try {
      return new JSONObject(response);
    } catch (JSONException e) {
      throw new GnsClientException("Failed to parse LOOKUP_ACCOUNT_RECORD response", e);
    }
  }

  /**
   * Get the public key for a given alias.
   *
   * @param alias
   * @return the public key registered for the alias
   * @throws GnsInvalidGuidException
   * @throws GnsClientException
   * @throws IOException
   */
  public PublicKey publicKeyLookupFromAlias(String alias) throws GnsInvalidGuidException, GnsClientException, IOException {

    String guid = lookupGuid(alias);
    return publicKeyLookupFromGuid(guid);
  }

  /**
   * Get the public key for a given GUID.
   *
   * @param guid
   * @return
   * @throws GnsInvalidGuidException
   * @throws GnsClientException
   * @throws IOException
   */
  public PublicKey publicKeyLookupFromGuid(String guid) throws GnsInvalidGuidException, GnsClientException, IOException {
    JSONObject guidInfo = lookupGuidRecord(guid);
    try {
      String key = guidInfo.getString(GUID_RECORD_PUBLICKEY);
      byte[] encodedPublicKey = Base64.decode(key);
      KeyFactory keyFactory = KeyFactory.getInstance(RSA_ALGORITHM);
      X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(encodedPublicKey);
      return keyFactory.generatePublic(publicKeySpec);
    } catch (JSONException e) {
      throw new GnsClientException("Failed to parse LOOKUP_USER response", e);
    } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
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
  @Override
  public GuidEntry accountGuidCreate(String alias, String password) throws Exception {

    KeyPair keyPair = KeyPairGenerator.getInstance(RSA_ALGORITHM).generateKeyPair();
    String guid = GuidUtils.createGuidFromPublicKey(keyPair.getPublic().getEncoded());
    // Squirrel this away now just in case the call below times out.
    KeyPairUtils.saveKeyPair(getGNSInstance(), alias, guid, keyPair);
    String returnedGuid = accountGuidCreateHelper(alias, keyPair.getPublic(), keyPair.getPrivate(), password);
    assert returnedGuid.equals(guid);
    // Anything else we want to do here?
    if (!returnedGuid.equals(guid)) {
      GNSClientConfig.getLogger().log(Level.WARNING,
              "Returned guid {0} doesn''t match locally created guid {1}",
              new Object[]{returnedGuid, guid});
    }
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
  @Override
  public String accountGuidVerify(GuidEntry guid, String code) throws Exception {
    JSONObject command = createAndSignCommand(CommandType.VerifyAccount,
            guid.getPrivateKey(), VERIFY_ACCOUNT, GUID, guid.getGuid(),
            CODE, code);
    String response = sendCommandAndWait(command);
    return checkResponse(command, response);
  }

  /**
   * Deletes the account given by name
   *
   * @param guid GuidEntry
   * @throws Exception
   */
  public void accountGuidRemove(GuidEntry guid) throws Exception {
    JSONObject command = createAndSignCommand(CommandType.RemoveAccount,
            guid.getPrivateKey(),
            REMOVE_ACCOUNT,
            GUID, guid.getGuid(),
            NAME, guid.getEntityName());
    String response = sendCommandAndWait(command);
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
  @Override
  public GuidEntry guidCreate(GuidEntry accountGuid, String alias) throws Exception {

    long startTime = System.currentTimeMillis();
    GuidEntry entry = GuidUtils.createAndSaveGuidEntry(alias, getGNSInstance());
    DelayProfiler.updateDelay("updatePreferences", startTime);
    String returnedGuid = guidCreateHelper(accountGuid, alias, entry.getPublicKey());
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
   * Batch create guids with the given aliases. If
   * createPublicKeys is true, key pairs will be created and saved
   * by the client for the guids. If not, bogus public keys will
   * be uses which will make the guids only accessible using the 
   * account guid (which has ACL access to each guid).
   *
   * @param accountGuid
   * @param aliases
   * @param createPublicKeys
   * @return
   * @throws Exception
   */
  public String guidBatchCreate(GuidEntry accountGuid, Set<String> aliases,
          boolean createPublicKeys) throws Exception {

    List<String> aliasList = new ArrayList<>(aliases);
    List<String> publicKeys = null;
    if (createPublicKeys) {
      long publicKeyStartTime = System.currentTimeMillis();
      publicKeys = new ArrayList<>();
      for (String alias : aliasList) {
        long singleEntrystartTime = System.currentTimeMillis();
        GuidEntry entry = GuidUtils.createAndSaveGuidEntry(alias, getGNSInstance());
        DelayProfiler.updateDelay("updateOnePreference", singleEntrystartTime);
        byte[] publicKeyBytes = entry.getPublicKey().getEncoded();
        String publicKeyString = Base64.encodeToString(publicKeyBytes, false);
        publicKeys.add(publicKeyString);
      }
      DelayProfiler.updateDelay("batchCreatePublicKeys", publicKeyStartTime);
    }

    System.out.println(DelayProfiler.getStats());
    JSONObject command;
    if (createPublicKeys) {
      command = createAndSignCommand(CommandType.AddMultipleGuids,
              accountGuid.getPrivateKey(), ADD_MULTIPLE_GUIDS,
              GUID, accountGuid.getGuid(),
              NAMES, new JSONArray(aliasList),
              PUBLIC_KEYS, new JSONArray(publicKeys));
    } else {
      // This version creates guids that have bogus public keys
      command = createAndSignCommand(CommandType.AddMultipleGuidsFast,
              accountGuid.getPrivateKey(), ADD_MULTIPLE_GUIDS,
              GUID, accountGuid.getGuid(),
              NAMES, new JSONArray(aliasList));
    }
    String result = checkResponse(command, sendCommandAndWait(command));
    return result;
  }

  /**
   * Batch create a number guids. These guids will have
   * random aliases and can be accessed using the account guid.
   *
   * @param accountGuid
   * @param guidCnt
   * @return
   * @throws Exception
   */
  public String guidBatchCreateFast(GuidEntry accountGuid, int guidCnt) throws Exception {
    JSONObject command;
    // This version creates guids that have bogus public keys
    command = createAndSignCommand(CommandType.AddMultipleGuidsFastRandom,
            accountGuid.getPrivateKey(), ADD_MULTIPLE_GUIDS,
            GUID, accountGuid.getGuid(),
            GUIDCNT, guidCnt);
    String result = checkResponse(command, sendCommandAndWait(command));
    return result;
  }

  /**
   * Removes a guid (not for account Guids - use removeAccountGuid for them).
   *
   * @param guid the guid to remove
   * @throws Exception
   */
  public void guidRemove(GuidEntry guid) throws Exception {
    JSONObject command = createAndSignCommand(CommandType.RemoveGuidNoAccount,
            guid.getPrivateKey(),
            REMOVE_GUID, GUID, guid.getGuid());
    String response = sendCommandAndWait(command);

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
    JSONObject command = createAndSignCommand(CommandType.RemoveGuid,
            accountGuid.getPrivateKey(),
            REMOVE_GUID,
            ACCOUNT_GUID, accountGuid.getGuid(),
            GUID, guidToRemove);
    String response = sendCommandAndWait(command);

    checkResponse(command, response);
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
   * @throws GnsClientException if a protocol error occurs or the list cannot be
   * parsed
   * @throws GnsInvalidGuidException if the group guid is invalid
   */
  public JSONArray groupGetMembers(String groupGuid, GuidEntry reader) throws IOException, GnsClientException,
          GnsInvalidGuidException {
    JSONObject command = createAndSignCommand(CommandType.GetGroupMembers,
            reader.getPrivateKey(), GET_GROUP_MEMBERS, GUID, groupGuid,
            READER, reader.getGuid());
    String response = sendCommandAndWait(command);

    try {
      return new JSONArray(checkResponse(command, response));
    } catch (JSONException e) {
      throw new GnsClientException("Invalid member list", e);
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
   * @throws GnsClientException if a protocol error occurs or the list cannot be
   * parsed
   * @throws GnsInvalidGuidException if the group guid is invalid
   */
  public JSONArray guidGetGroups(String guid, GuidEntry reader) throws IOException, GnsClientException,
          GnsInvalidGuidException {
    JSONObject command = createAndSignCommand(CommandType.GetGroups,
            reader.getPrivateKey(), GET_GROUPS, GUID, guid,
            READER, reader.getGuid());
    String response = sendCommandAndWait(command);

    try {
      return new JSONArray(checkResponse(command, response));
    } catch (JSONException e) {
      throw new GnsClientException("Invalid member list", e);
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
   * @throws GnsClientException
   */
  public void groupAddGuid(String groupGuid, String guidToAdd, GuidEntry writer) throws IOException,
          GnsInvalidGuidException, GnsClientException {
    JSONObject command = createAndSignCommand(CommandType.AddToGroup,
            writer.getPrivateKey(), ADD_TO_GROUP, GUID, groupGuid,
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
   * @throws GnsClientException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   */
  public void groupAddGuids(String groupGuid, JSONArray members, GuidEntry writer) throws IOException,
          GnsInvalidGuidException, GnsClientException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    JSONObject command = createAndSignCommand(CommandType.AddMembersToGroup,
            writer.getPrivateKey(), ADD_TO_GROUP, GUID, groupGuid,
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
   * @throws GnsClientException
   */
  public void groupRemoveGuid(String guid, String guidToRemove, GuidEntry writer) throws IOException,
          GnsInvalidGuidException, GnsClientException {
    JSONObject command = createAndSignCommand(CommandType.RemoveFromGroup,
            writer.getPrivateKey(), REMOVE_FROM_GROUP, GUID, guid,
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
   * @throws GnsClientException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   */
  public void groupRemoveGuids(String guid, JSONArray members, GuidEntry writer) throws IOException,
          GnsInvalidGuidException, GnsClientException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    JSONObject command = createAndSignCommand(CommandType.RemoveMembersFromGroup,
            writer.getPrivateKey(), REMOVE_FROM_GROUP, GUID, guid,
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
    aclAdd(GnsProtocol.AccessType.WRITE_WHITELIST, groupGuid, GROUP_ACL, guidToAuthorize);
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
    aclRemove(GnsProtocol.AccessType.WRITE_WHITELIST, groupGuid, GROUP_ACL, guidToUnauthorize);
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
    aclAdd(GnsProtocol.AccessType.READ_WHITELIST, groupGuid, GROUP_ACL, guidToAuthorize);
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
    aclRemove(GnsProtocol.AccessType.READ_WHITELIST, groupGuid, GROUP_ACL, guidToUnauthorize);
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
   * @throws GnsClientException if the query is not accepted by the server.
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
   * @throws GnsClientException if the query is not accepted by the server.
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
   * @throws GnsClientException if the query is not accepted by the server.
   */
  public JSONArray aclGet(GnsProtocol.AccessType accessType, GuidEntry guid, String field, String accesserGuid)
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
    JSONObject command = createAndSignCommand(CommandType.AddAlias,
            guid.getPrivateKey(), ADD_ALIAS, GUID, guid.getGuid(),
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
    JSONObject command = createAndSignCommand(CommandType.RemoveAlias,
            guid.getPrivateKey(), REMOVE_ALIAS, GUID, guid.getGuid(),
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
    JSONObject command = createAndSignCommand(CommandType.RetrieveAliases,
            guid.getPrivateKey(), RETRIEVE_ALIASES, GUID, guid.getGuid());

    String response = sendCommandAndWait(command);
    try {
      return new JSONArray(checkResponse(command, response));
    } catch (JSONException e) {
      throw new GnsClientException("Invalid alias list", e);
    }
  }

  // TAGS
  /**
   * Creates a tag to the tags of the guid.
   *
   * @param guid
   * @param tag
   * @throws Exception
   */
  @Override
  public void addTag(GuidEntry guid, String tag) throws Exception {
    JSONObject command = createAndSignCommand(CommandType.AddTag,
            guid.getPrivateKey(), ADD_TAG,
            GUID, guid.getGuid(), NAME, tag);
    String response = sendCommandAndWait(command);

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
    JSONObject command = createAndSignCommand(CommandType.RemoveTag,
            guid.getPrivateKey(), REMOVE_TAG,
            GUID, guid.getGuid(), NAME, tag);
    String response = sendCommandAndWait(command);

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
    JSONObject command = createCommand(CommandType.Dump,
            DUMP, NAME, tag);
    String response = sendCommandAndWait(command);

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
    JSONObject command = createCommand(CommandType.ClearTagged,
            CLEAR_TAGGED, NAME, tag);
    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  // ///////////////////////////////
  // // PRIVATE METHODS BELOW /////
  // /////////////////////////////
  /**
   * Creates a new GUID associated with an account.
   *
   * @param accountGuid
   * @param name
   * @param publicKey
   * @return
   * @throws Exception
   */
  private String guidCreateHelper(GuidEntry accountGuid, String name, PublicKey publicKey) throws Exception {
    long startTime = System.currentTimeMillis();
    byte[] publicKeyBytes = publicKey.getEncoded();
    String publicKeyString = Base64.encodeToString(publicKeyBytes, false);
    JSONObject command = createAndSignCommand(CommandType.AddGuid,
            accountGuid.getPrivateKey(), ADD_GUID,
            GUID, accountGuid.getGuid(),
            NAME, name,
            PUBLIC_KEY, publicKeyString);
    String result = checkResponse(command, sendCommandAndWait(command));
    DelayProfiler.updateDelay("guidCreate", startTime);
    return result;
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
   * @throws GnsClientException
   * @throws GnsInvalidGuidException if the user already exists
   */
  private String accountGuidCreateHelper(String alias, PublicKey publicKey, PrivateKey privateKey, String password) throws UnsupportedEncodingException,
          IOException, GnsClientException, GnsInvalidGuidException, NoSuchAlgorithmException {
    JSONObject command;
    long startTime = System.currentTimeMillis();
    if (password != null) {
      command = createAndSignCommand(CommandType.RegisterAccount,
              privateKey, REGISTER_ACCOUNT,
              NAME, alias,
              PUBLIC_KEY, Base64.encodeToString(publicKey.getEncoded(), false),
              PASSWORD, Base64.encodeToString(Password.encryptPassword(password, alias), false));
    } else {
      command = createAndSignCommand(CommandType.RegisterAccountSansPassword,
              privateKey, REGISTER_ACCOUNT,
              NAME, alias,
              PUBLIC_KEY, Base64.encodeToString(publicKey.getEncoded(), false));
    }
    String result = checkResponse(command, sendCommandAndWait(command));
    DelayProfiler.updateDelay("accountGuidCreate", startTime);
    return result;
  }

  private void aclAdd(String accessType, GuidEntry guid, String field, String accesserGuid) throws Exception {
    JSONObject command = createAndSignCommand(CommandType.AclAddSelf,
            guid.getPrivateKey(), ACL_ADD, ACL_TYPE, accessType,
            GUID, guid.getGuid(), FIELD, field,
            ACCESSER, accesserGuid == null ? ALL_USERS : accesserGuid);
    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  private void aclRemove(String accessType, GuidEntry guid, String field, String accesserGuid) throws Exception {
    JSONObject command = createAndSignCommand(CommandType.AclRemoveSelf,
            guid.getPrivateKey(), ACL_REMOVE, ACL_TYPE, accessType,
            GUID, guid.getGuid(), FIELD, field,
            ACCESSER, accesserGuid == null ? ALL_USERS : accesserGuid);
    String response = sendCommandAndWait(command);

    checkResponse(command, response);
  }

  private JSONArray aclGet(String accessType, GuidEntry guid, String field, String readerGuid) throws Exception {
    JSONObject command = createAndSignCommand(CommandType.AclRetrieve,
            guid.getPrivateKey(), ACL_RETRIEVE, ACL_TYPE, accessType,
            GUID, guid.getGuid(), FIELD, field,
            READER, readerGuid == null ? ALL_USERS : readerGuid);
    String response = sendCommandAndWait(command);
    try {
      return new JSONArray(checkResponse(command, response));
    } catch (JSONException e) {
      throw new GnsClientException("Invalid ACL list", e);
    }
  }
}
