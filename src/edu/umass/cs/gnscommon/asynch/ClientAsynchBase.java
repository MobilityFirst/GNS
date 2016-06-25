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
import edu.umass.cs.gnsclient.client.CommandUtils;
import static edu.umass.cs.gnsclient.client.CommandUtils.getRandomRequestId;
import edu.umass.cs.gnsclient.client.GNSClientConfig;
import edu.umass.cs.gnsclient.client.GuidEntry;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Random;
import org.json.JSONException;
import org.json.JSONObject;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.*;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnscommon.SharedGuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import static edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountAccess.ACCOUNT_INFO;
import static edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountAccess.GUID_INFO;
import static edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountAccess.HRN_GUID;
import static edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountAccess.PRIMARY_GUID;
import static edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountAccess.createACL;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountAccess;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountInfo;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.GuidInfo;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.MetaDataTypeName;
import edu.umass.cs.gnsserver.gnsapp.packet.CommandPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectRequestPacket;
import edu.umass.cs.gnsserver.utils.ResultValue;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.nio.nioutils.StringifiableDefault;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.gnscommon.GNSCommandProtocol;
import edu.umass.cs.gnscommon.utils.CanonicalJSON;
import edu.umass.cs.gnscommon.utils.Format;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnsserver.gnsapp.clientSupport.ClientSupportConfig;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SignatureException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;
import static edu.umass.cs.gnsclient.client.CommandUtils.signDigestOfMessage;

/**
 * This class defines a basic asynchronous client to communicate with a GNS instance over TCP.
 *
 * @author Westy
 */
// FIXME: This might be redundant with the AsyncClient internal class used in GNSClient.
public class ClientAsynchBase extends ReconfigurableAppClientAsync {

  public static final Set<IntegerPacketType> CLIENT_PACKET_TYPES
          = new HashSet<>(Arrays.asList(Packet.PacketType.COMMAND,
                  Packet.PacketType.COMMAND_RETURN_VALUE,
                  Packet.PacketType.SELECT_REQUEST,
                  Packet.PacketType.SELECT_RESPONSE
          ));
  /**
   * The default interval (in seconds) before which a query will not be refreshed. In other words
   * if you wait this interval you will get the latest from the database, otherwise you will get the
   * cached value.
   */
  public static final int DEFAULT_MIN_REFRESH_INTERVAL_FOR_SELECT = 60; //seconds
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
    super(addresses, ReconfigurationConfig.getClientSSLMode(),
            ReconfigurationConfig.getClientPortOffset(), false);

    ClientSupportConfig.getLogger().log(Level.INFO, "Reconfigurators {0}", addresses);
    ClientSupportConfig.getLogger().log(Level.INFO, "Client port offset {0}", ReconfigurationConfig.getClientPortOffset());
    ClientSupportConfig.getLogger().log(Level.INFO, "SSL Mode is {0}", ReconfigurationConfig.getClientSSLMode());

    keyPairHostIndex = addresses.iterator().next();
    this.enableJSONPackets();
  }

  private static Stringifiable<String> unstringer = new StringifiableDefault<>("");

  @Override
  // This needs to return null for packet types that we don't want to handle.
  public Request getRequest(String stringified) throws RequestParseException {
    Request request = null;
    try {
      return getRequestFromJSON(new JSONObject(stringified));
    } catch (JSONException e) {
      ClientSupportConfig.getLogger().log(Level.WARNING, "Problem handling JSON request: {0}", e);
    }
    return request;
  }

  @Override
  // This needs to return null for packet types that we don't want to handle.
  public Request getRequestFromJSON(JSONObject json) throws RequestParseException {
    Request request = null;
    try {
      if (CLIENT_PACKET_TYPES.contains(Packet.getPacketType(json))) {
        request = (Request) Packet.createInstance(json, unstringer);
      }
    } catch (JSONException e) {
      ClientSupportConfig.getLogger().log(Level.WARNING, "Problem handling JSON request: {0}", e);
    }
    return request;
  }

  @Override
  public Set<IntegerPacketType> getRequestTypes() {
    return Collections.unmodifiableSet(CLIENT_PACKET_TYPES);
  }

  /**
   * Sends a command packet to an active replica.
   *
   * @param command
   * @param callback
   * @return the request id
   * @throws IOException
   * @throws JSONException
   */
  private long sendCommandAsynch(JSONObject command, RequestCallback callback) throws IOException, JSONException {
    long id = generateNextRequestID();
    CommandPacket packet = new CommandPacket(id, command);
    ClientSupportConfig.getLogger().log(Level.FINER, "{0} sending remote query {1}", new Object[]{this, packet.getSummary()});
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
    String guid = SharedGuidUtils.createGuidStringFromPublicKey(keyPair.getPublic().getEncoded());
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
    GNSClientConfig.getLogger().log(Level.FINE, "##### Sending: {0}", createPacket.toString());
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
   * @return a request id
   * @throws Exception
   */
  public long accountGuidVerify(GuidEntry guid, String code, RequestCallback callback) throws Exception {
    return sendCommandAsynch(createAndSignCommand(CommandType.VerifyAccount, 
            guid.getPrivateKey(), GUID, guid.getGuid(),
            CODE, code), callback);
  }

  /**
   * Deletes the account given by name.
   *
   * @param guid GuidEntry
   * @param callback
   * @return the request id
   * @throws Exception
   */
  public long accountGuidRemove(GuidEntry guid, RequestCallback callback) throws Exception {
    return sendCommandAsynch(createAndSignCommand(CommandType.RemoveAccount, 
            guid.getPrivateKey(), GUID, guid.getGuid(),
            NAME, guid.getEntityName()), callback);
  }

  /**
   * Creates an new GUID associated with an account on the GNS server.
   *
   * @param accountGuid
   * @param alias the alias
   * @param callback
   * @return the newly created GUID entry
   * @throws Exception
   */
  public GuidEntry guidCreate(GuidEntry accountGuid, String alias, RequestCallback callback) throws Exception {

    KeyPair keyPair = KeyPairGenerator.getInstance(RSA_ALGORITHM).generateKeyPair();
    String guid = SharedGuidUtils.createGuidStringFromPublicKey(keyPair.getPublic().getEncoded());
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
    GNSClientConfig.getLogger().log(Level.FINE, "##### Sending: {0}", createPacket.toString());
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
    for (String alias : aliases) {
      // Probably should reuse code from account and guid create
      GuidEntry entry = GuidUtils.createAndSaveGuidEntry(alias,
              keyPairHostIndex.getHostString() + ":" + keyPairHostIndex.getPort());
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

    CreateServiceName createPacket = new CreateServiceName(null, state);
    GNSClientConfig.getLogger().log(Level.FINE, "##### Sending: {0}", createPacket.toString());
    sendRequest(createPacket, callback);
  }

  /**
   * Removes a guid (not for account Guids - use removeAccountGuid for them).
   *
   * @param guid the guid to remove
   * @param callback
   * @return
   * @throws Exception
   */
  public long guidRemove(GuidEntry guid, RequestCallback callback) throws Exception {
    return sendCommandAsynch(createAndSignCommand(CommandType.RemoveGuid, guid.getPrivateKey(),
            GUID, guid.getGuid()), callback);
  }

  /**
   * Removes a guid given the guid and the associated account guid.
   *
   * @param accountGuid
   * @param guidToRemove
   * @param callback
   * @return
   * @throws Exception
   */
  public long guidRemove(GuidEntry accountGuid, String guidToRemove, RequestCallback callback) throws Exception {
    return sendCommandAsynch(createAndSignCommand(CommandType.RemoveGuid, accountGuid.getPrivateKey(),
            ACCOUNT_GUID, accountGuid.getGuid(),
            GUID, guidToRemove), callback);
  }

  /**
   * Obtains the guid of the alias from the remote replica.
   *
   * @param alias
   * @param callback
   * @return
   * @throws IOException
   * @throws org.json.JSONException
   * @throws UnsupportedEncodingException
   * @throws ClientException
   */
  public long lookupGuid(String alias, RequestCallback callback) throws IOException, JSONException, ClientException {
    return sendCommandAsynch(createCommand(CommandType.LookupGuid, NAME, alias), callback);
  }

  /**
   * If this is a sub guid returns the account guid it was created under from a remote replica.
   *
   * @param guid
   * @param callback
   * @return the request id
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws ClientException
   * @throws org.json.JSONException
   */
  public long lookupPrimaryGuid(String guid, RequestCallback callback)
          throws UnsupportedEncodingException, IOException, ClientException, JSONException {
    return sendCommandAsynch(createCommand(CommandType.LookupPrimaryGuid, GUID, guid), callback);
  }

  /**
   * Returns a JSON object containing all of the guid meta information from a remote replica.
   * This method returns meta data about the guid.
   * If you want any particular field or fields of the guid
   * you'll need to use one of the read methods.
   *
   * @param guid
   * @param callback
   * @return the request id
   * @throws IOException
   * @throws ClientException
   * @throws org.json.JSONException
   */
  public long lookupGuidRecord(String guid, RequestCallback callback)
          throws IOException, ClientException, JSONException {
    return sendCommandAsynch(createCommand(CommandType.LookupGuidRecord, GUID, guid), callback);
  }

  /**
   * Returns a JSON object containing all of the account meta information for an
   * account guid from a remote replica.
   *
   * This method returns meta data about the account associated with this guid
   * if and only if the guid is an account guid.
   * If you want any particular field or fields of the guid
   * you'll need to use one of the read methods.
   *
   * @param accountGuid
   * @param callback
   * @return the request id
   * @throws IOException
   * @throws ClientException
   * @throws org.json.JSONException
   */
  public long lookupAccountRecord(String accountGuid, RequestCallback callback)
          throws IOException, ClientException, JSONException {
    return sendCommandAsynch(createCommand(CommandType.LookupGuidRecord, GUID, accountGuid), callback);
  }

  /**
   * Read a field from a remote replica.
   *
   * @param guid
   * @param field
   * @param callback
   * @return a request id
   * @throws IOException
   * @throws org.json.JSONException
   * @throws UnsupportedEncodingException
   * @throws ClientException
   */
  public long fieldRead(String guid, String field, RequestCallback callback)
          throws IOException, JSONException, ClientException {
    // Send a read command that doesn't need authentication.
    // FIXME: MAGIC_STRING stuff is currently on the server.
    return sendCommandAsynch(createCommand(CommandType.ReadUnsigned, GUID, guid, FIELD, field), callback);
  }

  /**
   * Read a field that is an array from a remote replica.
   *
   * @param guid
   * @param field
   * @param callback
   * @return a request id
   * @throws IOException
   * @throws JSONException
   * @throws ClientException
   */
  public long fieldReadArray(String guid, String field, RequestCallback callback)
          throws IOException, JSONException, ClientException {
    // Send a read command that doesn't need authentication.
    // FIXME: MAGIC_STRING stuff is currently on the server.
    return sendCommandAsynch(createCommand(CommandType.ReadArrayUnsigned, GUID, guid, FIELD, field), callback);
  }

  /**
   * Updates a field at a remote replica.
   *
   * @param guid
   * @param field
   * @param value
   * @param callback
   * @return the request id
   * @throws IOException
   * @throws JSONException
   * @throws ClientException
   */
  public long fieldUpdate(String guid, String field, Object value, RequestCallback callback) throws IOException, JSONException, ClientException {
    // Send a read command that doesn't need authentication.
    JSONObject json = new JSONObject();
    json.put(field, value);
    return sendCommandAsynch(createCommand(CommandType.ReplaceUserJSONUnsigned,
            GUID, guid,
            USER_JSON, json.toString(),
            WRITER, MAGIC_STRING), callback);
  }

  /**
   * Updates or replaces a field that is an array at a remote replica.
   *
   * @param guid
   * @param field
   * @param value
   * @param callback
   * @return the request id
   * @throws IOException
   * @throws JSONException
   * @throws ClientException
   */
  public long fieldReplaceOrCreateArray(String guid, String field, ResultValue value, RequestCallback callback) throws IOException, JSONException, ClientException {
    // Send a read command that doesn't need authentication.
    return sendCommandAsynch(createCommand(CommandType.ReplaceOrCreateListUnsigned,
            GUID, guid,
            FIELD, field,
            VALUE, value.toString(),
            WRITER, MAGIC_STRING), callback);
  }

  /**
   * Appends a value to a field that is an array at a remote replica.
   *
   * @param guid
   * @param field
   * @param value
   * @param callback
   * @return the request id
   * @throws IOException
   * @throws JSONException
   * @throws ClientException
   */
  public long fieldAppendToArray(String guid, String field, ResultValue value, RequestCallback callback) throws IOException, JSONException, ClientException {
    // Send a read command that doesn't need authentication.
    return sendCommandAsynch(createCommand(CommandType.AppendListUnsigned,
            GUID, guid,
            FIELD, field,
            VALUE, value.toString(),
            WRITER, MAGIC_STRING), callback);
  }

  /**
   * Removes a value from field at a remote replica.
   *
   * @param guid
   * @param field
   * @param value
   * @param callback
   * @return the request id
   * @throws IOException
   * @throws JSONException
   * @throws ClientException
   */
  public long fieldRemove(String guid, String field, Object value, RequestCallback callback) throws IOException, JSONException, ClientException {
    // Send a remove command that doesn't need authentication.
    JSONObject json = new JSONObject();
    json.put(field, value);
    return sendCommandAsynch(createCommand(CommandType.RemoveUnsigned,
            GUID, guid,
            FIELD, field, VALUE, value.toString(),
            WRITER, MAGIC_STRING), callback);
  }

  /**
   * Removes all the values from from field that is an array at a remote replica.
   *
   * @param guid
   * @param field
   * @param value
   * @param callback
   * @return the request id
   * @throws IOException
   * @throws JSONException
   * @throws ClientException
   */
  public long fieldRemoveMultiple(String guid, String field, ResultValue value, RequestCallback callback) throws IOException, JSONException, ClientException {
    // Send a remove command that doesn't need authentication.
    JSONObject json = new JSONObject();
    json.put(field, value);
    return sendCommandAsynch(createCommand(CommandType.RemoveListUnsigned,
            GUID, guid,
            FIELD, field, VALUE, value.toString(),
            WRITER, MAGIC_STRING), callback);
  }

  /**
   * Updates all the fields in the given JSON in a field at a remote replica.
   *
   * @param guid
   * @param field
   * @param json
   * @param callback
   * @return the request id
   * @throws IOException
   * @throws JSONException
   * @throws ClientException
   */
  public long update(String guid, String field, JSONObject json, RequestCallback callback) throws IOException, JSONException, ClientException {
    // Send a read command that doesn't need authentication.
    return sendCommandAsynch(createCommand(CommandType.ReplaceUserJSONUnsigned,
            GUID, guid,
            USER_JSON, json.toString(),
            WRITER, MAGIC_STRING), callback);
  }

  /**
   * Sends a select packet to a remote replica.
   *
   * @param packet
   * @param callback
   * @return the request id
   * @throws IOException
   */
  public long sendSelectPacket(SelectRequestPacket<String> packet, RequestCallback callback) throws IOException {
    long id = generateNextRequestID();
    packet.setRequestId(id);
    ClientSupportConfig.getLogger().log(Level.FINE, "{0} sending select packet {1}",
            new Object[]{this, packet.getSummary()});
    return sendRequest(packet, callback);
  }

  /**
   * Creates a command object from the given CommandType and a variable
   * number of key and value pairs with a signature parameter. The signature is
   * generated from the query signed by the given guid.
   *
   * @param commandType
   * @param privateKey
   * @param keysAndValues
   * @return the query string
   * @throws ClientException
   */
  // Same as the version on CommandUtils but it forces coordinated reads (see createCommand)
  // FIXME: Consolidate these separate versions.
  public JSONObject createAndSignCommand(CommandType commandType,
          PrivateKey privateKey, Object... keysAndValues)
          throws ClientException {
    try {
      JSONObject result = createCommand(commandType, keysAndValues);
      result.put(GNSCommandProtocol.TIMESTAMP, Format.formatDateISO8601UTC(new Date()));
      result.put(GNSCommandProtocol.SEQUENCE_NUMBER, getRandomRequestId());

      String canonicalJSON = CanonicalJSON.getCanonicalForm(result);
      String signatureString = signDigestOfMessage(privateKey, canonicalJSON);
      result.put(GNSCommandProtocol.SIGNATURE, signatureString);
      return result;
    } catch (JSONException | NoSuchAlgorithmException | InvalidKeyException | SignatureException | UnsupportedEncodingException e) {
      throw new ClientException("Error encoding message", e);
    }
  }

  /**
   * Creates a command object from the given action string and a variable
   * number of key and value pairs.
   * 
   * Note: Same as the version in CommandUtils but it forces coordinated reads.
   * @param commandType
   * @param keysAndValues
   * @return the query string
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   */
  public JSONObject createCommand(CommandType commandType,
          Object... keysAndValues) throws ClientException {
    try {
      JSONObject result = CommandUtils.createCommand(commandType, keysAndValues);
      result.put(COORDINATE_READS, true);
      return result;
    } catch (JSONException e) {
      throw new ClientException("Error encoding message", e);
    }
  }

  /**
   * Return a new request id. Probably should use longs here.
   *
   * @return
   */
  public synchronized long generateNextRequestID() {
    return randomID.nextLong();
  }
}
