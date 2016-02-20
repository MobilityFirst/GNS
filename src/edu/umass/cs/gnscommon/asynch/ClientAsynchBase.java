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
package edu.umass.cs.gnscommon.asynch;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GuidEntry;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.SignatureException;
import java.util.Random;
import org.json.JSONException;
import org.json.JSONObject;
import static edu.umass.cs.gnscommon.GnsProtocol.*;
import edu.umass.cs.gnsserver.gnsApp.packet.CommandPacket;
import edu.umass.cs.gnscommon.utils.ByteUtils;
import edu.umass.cs.gnscommon.utils.CanonicalJSON;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnscommon.exceptions.client.EncryptionException;
import edu.umass.cs.gnscommon.exceptions.client.GnsACLException;
import edu.umass.cs.gnscommon.exceptions.client.GnsDuplicateNameException;
import edu.umass.cs.gnscommon.exceptions.client.GnsClientException;
import edu.umass.cs.gnscommon.exceptions.client.GnsFieldNotFoundException;
import edu.umass.cs.gnscommon.exceptions.client.GnsInvalidFieldException;
import edu.umass.cs.gnscommon.exceptions.client.GnsInvalidGroupException;
import edu.umass.cs.gnscommon.exceptions.client.GnsInvalidGuidException;
import edu.umass.cs.gnscommon.exceptions.client.GnsInvalidUserException;
import edu.umass.cs.gnscommon.exceptions.client.GnsVerificationException;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.AccountAccess;
import static edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.AccountAccess.ACCOUNT_INFO;
import static edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.AccountAccess.GUID_INFO;
import static edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.AccountAccess.HRN_GUID;
import static edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.AccountAccess.PRIMARY_GUID;
import static edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.AccountAccess.createACL;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.AccountInfo;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.GuidInfo;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.MetaDataTypeName;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.UpdateOperation;
import edu.umass.cs.gnsserver.gnsApp.packet.Packet;
import edu.umass.cs.gnsserver.utils.ResultValue;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.nio.nioutils.StringifiableDefault;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This class defines a basic asynchronous client to communicate with a GNS instance over TCP.
 *
 * @author Westy
 */
public class ClientAsynchBase extends ReconfigurableAppClientAsync {

  private static Set<IntegerPacketType> clientPacketTypes
          = new HashSet<>(Arrays.asList(Packet.PacketType.COMMAND, Packet.PacketType.COMMAND_RETURN_VALUE));
  /**
   * Used to generate unique ids
   */
  private final Random randomID = new Random();
  /**
   * Used as a key into database of public private key pairs.
   */
  private InetSocketAddress keyPairHostIndex;

  /**
   * Creates a new ClientAsynchBase instance using the default reconfigurators.
   *
   * @throws IOException
   */
  public ClientAsynchBase() throws IOException {
    this(ReconfigurationConfig.getReconfiguratorAddresses());
  }

  /**
   * Creates a new ClientAsynchBase instance using the given reconfigurators.
   *
   * @param addresses
   * @throws java.io.IOException
   */
  public ClientAsynchBase(Set<InetSocketAddress> addresses) throws IOException {
    //super(addresses);
    // will use above code once we get rid of  ReconfigurationConfig accessors
    super(addresses,
            ReconfigurationConfig.getClientSSLMode(),
            ReconfigurationConfig.getClientPortOffset());
    if (isDebuggingEnabled()) {
      System.out.println("Reconfigurators " + addresses);
      System.out.println("Client port offset " + ReconfigurationConfig.getClientPortOffset());
      System.out.println("SSL Mode is " + ReconfigurationConfig.getClientSSLMode());
    }
    keyPairHostIndex = addresses.iterator().next();
  }

  private static Stringifiable<String> unstringer = new StringifiableDefault<String>("");

  @Override
  // This needs to return null for packet types that we don't want to handle.
  public Request getRequest(String stringified) throws RequestParseException {
    Request request = null;
    try {
      JSONObject json = new JSONObject(stringified);
      if (clientPacketTypes.contains(Packet.getPacketType(json))) {
        request = (Request) Packet.createInstance(json, unstringer);
      }
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return request;
  }

  @Override
  public Set<IntegerPacketType> getRequestTypes() {
    return Collections.unmodifiableSet(clientPacketTypes);
  }

  /**
   * Sends a command packet to an active replica.
   *
   * @param command
   * @param callback
   * @return the command id
   * @throws IOException
   * @throws JSONException
   */
  private long sendCommandAsynch(JSONObject command, RequestCallback callback) throws IOException, JSONException {
    int id = generateNextRequestID();
    CommandPacket packet = new CommandPacket(id, null, -1, command);
    return sendRequest(packet, callback);
  }

  /**
   * Creates an account guid.
   *
   * @param alias
   * @param password
   * @param callback
   * @return a guid entry
   * @throws Exception
   */
  public GuidEntry accountGuidCreate(String alias, String password, RequestCallback callback) throws Exception {
    KeyPair keyPair = KeyPairGenerator.getInstance(RSA_ALGORITHM).generateKeyPair();
    String guid = GuidUtils.createGuidFromPublicKey(keyPair.getPublic().getEncoded());
    // Squirrel this away now just in case the call below times out.
    KeyPairUtils.saveKeyPair(keyPairHostIndex.getHostString() + ":" + keyPairHostIndex.getPort(), alias, guid, keyPair);
    JSONObject jsonHRN = new JSONObject();
    jsonHRN.put(HRN_GUID, guid);
    AccountInfo accountInfo = new AccountInfo(alias, guid, password);
    accountInfo.noteUpdate();
    JSONObject jsonGuid = new JSONObject();
    jsonGuid.put(ACCOUNT_INFO, accountInfo.toJSONObject());
    GuidInfo guidInfo = new GuidInfo(alias, guid, Base64.encodeToString(keyPair.getPublic().getEncoded(), false));
    jsonGuid.put(GUID_INFO, guidInfo.toJSONObject());
    // set up ACL to look like this
    //"_GNS_ACL": {
    //  "READ_WHITELIST": {"+ALL+": {"MD": "+ALL+"]}}}
    JSONObject acl = AccountAccess.createACL(ALL_FIELDS, Arrays.asList(EVERYONE), null, null);
    // prefix is the same for all acls so just pick one to use here
    jsonGuid.put(MetaDataTypeName.READ_WHITELIST.getPrefix(), acl);

    // now we batch create both of the records
    Map<String, String> state = new HashMap<>();
    state.put(alias, jsonHRN.toString());
    state.put(guid, jsonGuid.toString());
    CreateServiceName createPacket = new CreateServiceName(null, state);
    if (isDebuggingEnabled()) {
      GNSClient.getLogger().info("##### Sending: " + createPacket.toString());
    }
    sendRequest(createPacket, callback);
    GuidEntry entry = new GuidEntry(alias, guid, keyPair.getPublic(), keyPair.getPrivate());
    return entry;
  }

  /**
   * Verify an account guid by sending the verification code to the server.
   *
   * @param guid the account GUID to verify
   * @param code the verification code
   * @param callback
   * @return
   * @throws Exception
   */
  public long accountGuidVerify(GuidEntry guid, String code, RequestCallback callback) throws Exception {
    return sendCommandAsynch(createAndSignCommand(guid.getPrivateKey(), VERIFY_ACCOUNT, GUID, guid.getGuid(),
            CODE, code), callback);
  }

  /**
   * Deletes the account given by name
   *
   * @param guid GuidEntry
   * @param callback
   * @return the command id
   * @throws Exception
   */
  public long accountGuidRemove(GuidEntry guid, RequestCallback callback) throws Exception {
    return sendCommandAsynch(createAndSignCommand(guid.getPrivateKey(), REMOVE_ACCOUNT, GUID, guid.getGuid(),
            NAME, guid.getEntityName()), callback);
  }

  /**
   * Creates an new GUID associated with an account on the GNS server.
   *
   * @param accountGuid
   * @param alias the alias
   * @return the newly created GUID entry
   * @throws Exception
   */
  public GuidEntry guidCreate(GuidEntry accountGuid, String alias, RequestCallback callback) throws Exception {

    KeyPair keyPair = KeyPairGenerator.getInstance(RSA_ALGORITHM).generateKeyPair();
    String guid = GuidUtils.createGuidFromPublicKey(keyPair.getPublic().getEncoded());
    // Squirrel this away now just in case the call below times out.
    KeyPairUtils.saveKeyPair(keyPairHostIndex.getHostString() + ":" + keyPairHostIndex.getPort(), alias, guid, keyPair);
    JSONObject jsonHRN = new JSONObject();
    jsonHRN.put(HRN_GUID, guid);
    JSONObject jsonGuid = new JSONObject();
    GuidInfo guidInfo = new GuidInfo(alias, guid, Base64.encodeToString(keyPair.getPublic().getEncoded(), false));
    jsonGuid.put(GUID_INFO, guidInfo.toJSONObject());
    jsonGuid.put(PRIMARY_GUID, accountGuid.getGuid());
    // set up ACL to look like this
    //"_GNS_ACL": {
    //  "READ_WHITELIST": {"+ALL+": {"MD": [<publickey>, "+ALL+"]}},
    //  "WRITE_WHITELIST": {"+ALL+": {"MD": [<publickey>]}}
    JSONObject acl = createACL(ALL_FIELDS, Arrays.asList(EVERYONE, accountGuid.getPublicKeyString()),
            ALL_FIELDS, Arrays.asList(accountGuid.getPublicKeyString()));
    // prefix is the same for all acls so just pick one to use here
    jsonGuid.put(MetaDataTypeName.READ_WHITELIST.getPrefix(), acl);

    // now we batch create both of the records
    Map<String, String> state = new HashMap<>();
    state.put(alias, jsonHRN.toString());
    state.put(guid, jsonGuid.toString());
    CreateServiceName createPacket = new CreateServiceName(null, state);
    if (isDebuggingEnabled()) {
      GNSClient.getLogger().info("##### Sending: " + createPacket.toString());
    }
    sendRequest(createPacket, callback);
    GuidEntry entry = new GuidEntry(alias, guid, keyPair.getPublic(), keyPair.getPrivate());
    return entry;
  }

  /**
   * Batch create guids with the given aliases. If
   * createPublicKeys is true, key pairs will be created and saved
   * by the client for the guids.
   *
   * @param accountGuid
   * @param aliases
   * @param callback
   * @throws Exception
   */
  public void guidBatchCreate(GuidEntry accountGuid, Set<String> aliases, RequestCallback callback)
          throws Exception {

    Map<String, String> state = new HashMap<>();
    long publicKeyStartTime = System.currentTimeMillis();
    for (String alias : aliases) {
      long singleEntrystartTime = System.currentTimeMillis();
      // Probably should reuse code from account and guid create
      GuidEntry entry = GuidUtils.createAndSaveGuidEntry(alias,
              keyPairHostIndex.getHostString(), keyPairHostIndex.getPort());
      DelayProfiler.updateDelay("updateOnePreference", singleEntrystartTime);
      JSONObject jsonHRN = new JSONObject();
      jsonHRN.put(HRN_GUID, entry.getGuid());
      JSONObject jsonGuid = new JSONObject();
      GuidInfo guidInfo = new GuidInfo(alias, entry.getGuid(),
              Base64.encodeToString(entry.getPublicKey().getEncoded(), false));
      jsonGuid.put(GUID_INFO, guidInfo.toJSONObject());
      jsonGuid.put(PRIMARY_GUID, accountGuid.getGuid());
      // set up ACL to look like this
      //"_GNS_ACL": {
      //  "READ_WHITELIST": {"+ALL+": {"MD": [<publickey>, "+ALL+"]}},
      //  "WRITE_WHITELIST": {"+ALL+": {"MD": [<publickey>]}}
      JSONObject acl = createACL(ALL_FIELDS, Arrays.asList(EVERYONE, accountGuid.getPublicKeyString()),
              ALL_FIELDS, Arrays.asList(accountGuid.getPublicKeyString()));
      // prefix is the same for all acls so just pick one to use here
      jsonGuid.put(MetaDataTypeName.READ_WHITELIST.getPrefix(), acl);
      state.put(alias, jsonHRN.toString());
      state.put(entry.getGuid(), jsonGuid.toString());
    }
    DelayProfiler.updateDelay("batchCreatePublicKeys", publicKeyStartTime);

    System.out.println(DelayProfiler.getStats());
    CreateServiceName createPacket = new CreateServiceName(null, state);
    if (isDebuggingEnabled()) {
      GNSClient.getLogger().info("##### Sending: " + createPacket.toString());
    }
    sendRequest(createPacket, callback);
  }

  /**
   * Removes a guid (not for account Guids - use removeAccountGuid for them).
   *
   * @param guid the guid to remove
   * @throws Exception
   */
  public long guidRemove(GuidEntry guid, RequestCallback callback) throws Exception {
    return sendCommandAsynch(createAndSignCommand(guid.getPrivateKey(),
            REMOVE_GUID, GUID, guid.getGuid()), callback);
  }

  /**
   * Removes a guid given the guid and the associated account guid.
   *
   * @param accountGuid
   * @param guidToRemove
   * @throws Exception
   */
  public long guidRemove(GuidEntry accountGuid, String guidToRemove, RequestCallback callback) throws Exception {
    return sendCommandAsynch(createAndSignCommand(accountGuid.getPrivateKey(), REMOVE_GUID, GUID, guidToRemove,
            ACCOUNT_GUID, accountGuid.getGuid()), callback);
  }

  /**
   * Obtains the guid of the alias from the GNS server.
   *
   * @param alias
   * @throws IOException
   * @throws UnsupportedEncodingException
   * @throws GnsClientException
   */
  public long lookupGuid(String alias, RequestCallback callback) throws IOException, JSONException, GnsClientException {
    return sendCommandAsynch(createCommand(LOOKUP_GUID, NAME, alias), callback);
  }

  /**
   * If this is a sub guid returns the account guid it was created under.
   *
   * @param guid
   * @return the command id
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws GnsClientException
   */
  public long lookupPrimaryGuid(String guid, RequestCallback callback)
          throws UnsupportedEncodingException, IOException, GnsClientException, JSONException {
    return sendCommandAsynch(createCommand(LOOKUP_PRIMARY_GUID, GUID, guid), callback);
  }

  /**
   * Returns a JSON object containing all of the guid meta information.
   * This method returns meta data about the guid.
   * If you want any particular field or fields of the guid
   * you'll need to use one of the read methods.
   *
   * @param guid
   * @return the command id
   * @throws IOException
   * @throws GnsClientException
   */
  public long lookupGuidRecord(String guid, RequestCallback callback)
          throws IOException, GnsClientException, JSONException {
    return sendCommandAsynch(createCommand(LOOKUP_GUID_RECORD, GUID, guid), callback);
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
   * @return the command id
   * @throws IOException
   * @throws GnsClientException
   */
  public long lookupAccountRecord(String accountGuid, RequestCallback callback)
          throws IOException, GnsClientException, JSONException {
    return sendCommandAsynch(createCommand(LOOKUP_ACCOUNT_RECORD, GUID, accountGuid), callback);
  }

  /**
   * Read a field from a GNS server.
   *
   * @param guid
   * @param field
   * @param callback
   * @return a request id
   * @throws IOException
   * @throws org.json.JSONException
   * @throws UnsupportedEncodingException
   * @throws GnsClientException
   */
  public long fieldRead(String guid, String field, RequestCallback callback) throws IOException, JSONException, GnsClientException {
    // Send a read command that doesn't need authentication.
    return sendCommandAsynch(createCommand(READ, GUID, guid, FIELD, field), callback);
  }

  public long fieldReadArray(String guid, String field, RequestCallback callback) throws IOException, JSONException, GnsClientException {
    // Send a read command that doesn't need authentication.
    return sendCommandAsynch(createCommand(READ_ARRAY, GUID, guid, FIELD, field), callback);
  }

  public long fieldUpdate(String guid, String field, Object value, RequestCallback callback) throws IOException, JSONException, GnsClientException {
    // Send a read command that doesn't need authentication.
    JSONObject json = new JSONObject();
    json.put(field, value);
    return sendCommandAsynch(createCommand(REPLACE_USER_JSON, GUID, guid,
            USER_JSON, json.toString(),
            WRITER, MAGIC_STRING), callback);
  }
  
   public long fieldUpdateArray(String guid, String field, ResultValue value, RequestCallback callback) throws IOException, JSONException, GnsClientException {
    // Send a read command that doesn't need authentication.
    return sendCommandAsynch(createCommand(REPLACE_OR_CREATE_LIST, GUID, guid,
            USER_JSON, value.toString(),
            WRITER, MAGIC_STRING), callback);
  }
  
  public long fieldRemove(String guid, String field, Object value, RequestCallback callback) throws IOException, JSONException, GnsClientException {
    // Send a remove command that doesn't need authentication.
    JSONObject json = new JSONObject();
    json.put(field, value);
    return sendCommandAsynch(createCommand(REMOVE, GUID, guid, 
            FIELD, field, VALUE, value.toString(),
            WRITER, MAGIC_STRING), callback);
  }
  
  public long fieldRemoveMultiple(String guid, String field, ResultValue value, RequestCallback callback) throws IOException, JSONException, GnsClientException {
    // Send a remove command that doesn't need authentication.
    JSONObject json = new JSONObject();
    json.put(field, value);
    return sendCommandAsynch(createCommand(REMOVE_LIST, GUID, guid, 
            FIELD, field, VALUE, value.toString(),
            WRITER, MAGIC_STRING), callback);
  }

  public long update(String guid, String field, JSONObject json, RequestCallback callback) throws IOException, JSONException, GnsClientException {
    // Send a read command that doesn't need authentication.
    return sendCommandAsynch(createCommand(REPLACE_USER_JSON, GUID, guid,
            USER_JSON, json.toString(),
            WRITER, MAGIC_STRING), callback);
  }

  // NEED TO CREATE A DUMMY GUID FOR THIS TO WORK
  /**
   * Check that the connectivity with the host:port can be established
   *
   * @throws IOException throws exception if a communication error occurs
   */
//  public void checkConnectivity() throws IOException {
//    
//    int originalReadTimeout = getReadTimeout();
//    setReadTimeout(7000);
//    JSONObject command;
//    try {
//      command = createCommand(CONNECTION_CHECK);
//      String commandResult = sendCommandAndWait(command);
//      if (!commandResult.startsWith(OK_RESPONSE)) {
//        String[] results = commandResult.split(" ");
//        throw new IOException(results.length == 2 ? results[1] : commandResult);
//      }
//    } catch (GnsClientException e) {
//      throw new IOException("Unable to create connectivity command.");
//    } finally {
//      setReadTimeout(originalReadTimeout);
//    }
//  }
  // ///////////////////////////////
  // // PRIVATE METHODS BELOW /////
  // /////////////////////////////
  /**
   * Checks the response from a command request for proper syntax as well as
   * converting error responses into the appropriate thrown GNS exceptions.
   *
   * @param command
   * @param response
   * @return the result of the command
   * @throws GnsClientException
   */
  // Saving this for later use.
  private String checkResponse(JSONObject command, String response) throws GnsClientException {
    // System.out.println("response:" + response);
    if (response.startsWith(BAD_RESPONSE)) {
      String results[] = response.split(" ");
      // System.out.println("results length:" + results.length);
      if (results.length < 2) {
        throw new GnsClientException("Invalid bad response indicator: " + response + " Command: " + command.toString());
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

        if (error.startsWith(BAD_SIGNATURE)) {
          throw new EncryptionException();
        }
        if (error.startsWith(BAD_GUID) || error.startsWith(BAD_ACCESSOR_GUID)
                || error.startsWith(DUPLICATE_GUID) || error.startsWith(BAD_ACCOUNT)) {
          throw new GnsInvalidGuidException(error + rest);
        }
        if (error.startsWith(DUPLICATE_FIELD)) {
          throw new GnsInvalidFieldException(error + rest);
        }
        if (error.startsWith(BAD_FIELD) || error.startsWith(FIELD_NOT_FOUND)) {
          throw new GnsFieldNotFoundException(error + rest);
        }
        if (error.startsWith(BAD_USER) || error.startsWith(DUPLICATE_USER)) {
          throw new GnsInvalidUserException(error + rest);
        }
        if (error.startsWith(BAD_GROUP) || error.startsWith(DUPLICATE_GROUP)) {
          throw new GnsInvalidGroupException(error + rest);
        }

        if (error.startsWith(ACCESS_DENIED)) {
          throw new GnsACLException(error + rest);
        }

        if (error.startsWith(DUPLICATE_NAME)) {
          throw new GnsDuplicateNameException(error + rest);
        }

        if (error.startsWith(VERIFICATION_ERROR)) {
          throw new GnsVerificationException(error + rest);
        }
        throw new GnsClientException("General command failure: " + error + rest);
      }
    }
    if (response.startsWith(NULL_RESPONSE)) {
      return null;
    } else {
      return response;
    }
  }

  /**
   * Creates a command object from the given action string and a variable
   * number of key and value pairs.
   *
   * @param action
   * @param keysAndValues
   * @return the query string
   * @throws edu.umass.cs.gnscommon.exceptions.client.GnsClientException
   */
  public JSONObject createCommand(String action, Object... keysAndValues) throws GnsClientException {
    long startTime = System.currentTimeMillis();
    try {
      JSONObject result = new JSONObject();
      String key;
      Object value;
      result.put(COMMANDNAME, action);
      for (int i = 0; i < keysAndValues.length; i = i + 2) {
        key = (String) keysAndValues[i];
        value = keysAndValues[i + 1];
        result.put(key, value);
      }
      DelayProfiler.updateDelay("createCommand", startTime);
      return result;
    } catch (JSONException e) {
      throw new GnsClientException("Error encoding message", e);
    }
  }

  /**
   * Creates a command object from the given action string and a variable
   * number of key and value pairs with a signature parameter. The signature is
   * generated from the query signed by the given guid.
   *
   * @param privateKey
   * @param action
   * @param keysAndValues
   * @return the query string
   * @throws GnsClientException
   */
  public JSONObject createAndSignCommand(PrivateKey privateKey, String action, Object... keysAndValues) throws GnsClientException {
    long startTime = System.currentTimeMillis();
    try {
      JSONObject result = createCommand(action, keysAndValues);
      String canonicalJSON = CanonicalJSON.getCanonicalForm(result);
      //String canonicalJSON = JSONUtils.getCanonicalJSONString(result);
      String signature = signDigestOfMessage(privateKey, canonicalJSON);
      //System.out.println("SIGNING THIS: " + canonicalJSON);
      result.put(SIGNATURE, signature);
      DelayProfiler.updateDelay("createAndSignCommand", startTime);
      return result;
    } catch (GnsClientException | NoSuchAlgorithmException | InvalidKeyException | SignatureException | JSONException e) {
      throw new GnsClientException("Error encoding message", e);
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
  private String signDigestOfMessage(PrivateKey privateKey, String message) throws NoSuchAlgorithmException,
          InvalidKeyException, SignatureException {
    long startTime = System.currentTimeMillis();
    Signature instance = Signature.getInstance(SIGNATURE_ALGORITHM);

    instance.initSign(privateKey);
    // instance.update(messageDigest);
    instance.update(message.getBytes());
    byte[] signature = instance.sign();
    String result = ByteUtils.toHex(signature);
    DelayProfiler.updateDelay("signDigestOfMessage", startTime);
    return result;
  }

  /**
   * Return a new request id. Probably should use longs here.
   *
   * @return
   */
  public synchronized int generateNextRequestID() {
    return randomID.nextInt();
  }

  // Enables all the debug logging statements in the client.
  private boolean debuggingEnabled = true;

  /**
   * @return the debuggingEnabled
   */
  public boolean isDebuggingEnabled() {
    return debuggingEnabled;
  }

  /**
   * @param debuggingEnabled the debuggingEnabled to set
   */
  public void setDebuggingEnabled(boolean debuggingEnabled) {
    this.debuggingEnabled = debuggingEnabled;
  }
}
