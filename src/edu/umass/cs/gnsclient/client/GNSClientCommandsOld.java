
package edu.umass.cs.gnsclient.client;

import edu.umass.cs.gnsclient.client.util.GUIDUtilsGNSClient;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.client.util.Password;
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.SharedGuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.client.DuplicateNameException;
import edu.umass.cs.gnscommon.exceptions.client.EncryptionException;
import edu.umass.cs.gnscommon.exceptions.client.FieldNotFoundException;
import edu.umass.cs.gnscommon.exceptions.client.InvalidGuidException;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.packets.ResponsePacket;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.utils.DelayProfiler;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Level;

import static edu.umass.cs.gnsclient.client.CommandUtils.commandResponseToJSONArray;


public class GNSClientCommandsOld extends GNSClient //implements GNSClientInterface 
{


  public GNSClientCommandsOld() throws IOException {
    super((InetSocketAddress) null);
  }


  public GNSClientCommandsOld(InetSocketAddress anyReconfigurator)
          throws IOException {
    super(anyReconfigurator);
  }

  private long readTimeout = 8000;


  public long getReadTimeout() {
    return readTimeout;
  }


  public void setReadTimeout(long readTimeout) {
    this.readTimeout = readTimeout;
  }



  private String getResponse(CommandType commandType, GuidEntry querier,
          Object... keysAndValues) throws ClientException, IOException {
    CommandPacket commandPacket;
    return record(// just instrumentation
            commandType,
            CommandUtils.checkResponse(this
                    .getResponsePacket(commandPacket = getCommand(commandType,
                            querier, keysAndValues), this.getReadTimeout()), commandPacket));
  }

  private static final boolean RECORD_ENABLED = true;

  public static final Map<CommandType, Set<String>> REVERSE_ENGINEER = new TreeMap<CommandType, Set<String>>();

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


  private static CommandPacket getCommand(CommandType type, GuidEntry querier,
          Object... keysAndValues) throws ClientException {
    CommandPacket packet = GNSCommand.getCommand(type, querier, keysAndValues);
    return packet;
  }

  // READ AND WRITE COMMANDS

  public void update(String targetGuid, JSONObject json, GuidEntry writer)
          throws IOException, ClientException {
    getResponse(CommandType.ReplaceUserJSON, writer, GNSProtocol.GUID.toString(), targetGuid,
            GNSProtocol.USER_JSON.toString(), json, GNSProtocol.WRITER.toString(), writer.getGuid());
  }


  public void update(GuidEntry guid, JSONObject json) throws IOException,
          ClientException {
    update(guid.getGuid(), json, guid);
  }


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


  public void fieldCreateIndex(GuidEntry guid, String field, String index)
          throws IOException, ClientException {
    getResponse(CommandType.CreateIndex, guid, GNSProtocol.GUID.toString(), guid.getGuid(), GNSProtocol.FIELD.toString(),
            field, GNSProtocol.VALUE.toString(), index, GNSProtocol.WRITER.toString(), guid.getGuid());
  }


  public void fieldUpdate(GuidEntry targetGuid, String field, Object value)
          throws IOException, ClientException {
    fieldUpdate(targetGuid.getGuid(), field, value, targetGuid);
  }


  public JSONObject read(String targetGuid, GuidEntry reader)
          throws Exception {
    return new JSONObject(getResponse(reader != null ? CommandType.ReadArray
            : CommandType.ReadArrayUnsigned, reader, GNSProtocol.GUID.toString(),
            targetGuid, GNSProtocol.FIELD.toString(), GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.READER.toString(),
            reader != null ? reader.getGuid() : null));
  }


  public JSONObject read(GuidEntry guid) throws Exception {
    return read(guid.getGuid(), guid);
  }


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


  public boolean fieldExists(GuidEntry targetGuid, String field)
          throws Exception {
    return fieldExists(targetGuid.getGuid(), field, targetGuid);
  }


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


  public String fieldRead(GuidEntry targetGuid, String field)
          throws Exception {
    return fieldRead(targetGuid.getGuid(), field, targetGuid);
  }


  public String fieldRead(String targetGuid, ArrayList<String> fields,
          GuidEntry reader) throws Exception {
    return getResponse(reader != null ? CommandType.ReadMultiField
            : CommandType.ReadMultiFieldUnsigned, reader, GNSProtocol.GUID.toString(), targetGuid,
            GNSProtocol.FIELDS.toString(), fields, GNSProtocol.READER.toString(), reader != null ? reader.getGuid()
                    : null);
  }


  public String fieldRead(GuidEntry targetGuid, ArrayList<String> fields)
          throws Exception {
    return fieldRead(targetGuid.getGuid(), fields, targetGuid);
  }


  public void fieldRemove(String targetGuid, String field, GuidEntry writer)
          throws IOException, ClientException {
    getResponse(CommandType.RemoveField, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(),
            field, GNSProtocol.WRITER.toString(), writer.getGuid());
  }

  // SELECT COMMANDS

  public JSONArray selectQuery(String query) throws Exception {

    return new JSONArray(getResponse(CommandType.SelectQuery, GNSProtocol.QUERY.toString(), query));
  }


  public JSONArray selectSetupGroupQuery(GuidEntry accountGuid,
          String publicKey, String query, int interval) throws Exception {
    return new JSONArray(getResponse(CommandType.SelectGroupSetupQuery, GNSProtocol.GUID.toString(), accountGuid.getGuid(),
            GNSProtocol.PUBLIC_KEY.toString(), publicKey, GNSProtocol.QUERY.toString(), query, GNSProtocol.INTERVAL.toString(), interval));
  }


  public JSONArray selectLookupGroupQuery(String guid) throws Exception {
    return new JSONArray(getResponse(CommandType.SelectGroupLookupQuery, GNSProtocol.GUID.toString(), guid));
  }

  // ACCOUNT COMMANDS

  public String lookupGuid(String alias) throws IOException, ClientException {

    return CommandUtils.specialCaseSingleField(getResponse(CommandType.LookupGuid, GNSProtocol.NAME.toString(), alias));
  }


  public String lookupPrimaryGuid(String guid)
          throws UnsupportedEncodingException, IOException, ClientException {
    return CommandUtils.specialCaseSingleField(getResponse(CommandType.LookupPrimaryGuid, GNSProtocol.GUID.toString(),
            guid));
  }


  public JSONObject lookupGuidRecord(String guid) throws IOException,
          ClientException {
    try {
      return new JSONObject(getResponse(CommandType.LookupGuidRecord, GNSProtocol.GUID.toString(), guid));
    } catch (JSONException e) {
      throw new ClientException(
              "Failed to parse LOOKUP_GUID_RECORD response", e);
    }
  }


  public JSONObject lookupAccountRecord(String accountGuid)
          throws IOException, ClientException {
    try {
      return new JSONObject(getResponse(CommandType.LookupAccountRecord, GNSProtocol.GUID.toString(), accountGuid));
    } catch (JSONException e) {
      throw new ClientException("Failed to parse LOOKUP_ACCOUNT_RECORD response", e);
    }
  }


  public PublicKey publicKeyLookupFromAlias(String alias)
          throws InvalidGuidException, ClientException, IOException {

    String guid = lookupGuid(alias);
    return publicKeyLookupFromGuid(guid);
  }


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


  public String accountGuidVerify(GuidEntry guid, String code)
          throws Exception {
    return getResponse(CommandType.VerifyAccount, guid, GNSProtocol.GUID.toString(),
            guid.getGuid(), GNSProtocol.CODE.toString(), code);
  }


  public String accountResendAuthenticationEmail(GuidEntry guid)
          throws Exception {
    return getResponse(CommandType.ResendAuthenticationEmail, guid, GNSProtocol.GUID.toString(),
            guid.getGuid());
  }


  public void accountGuidRemove(GuidEntry guid) throws Exception {
    getResponse(CommandType.RemoveAccount, guid, GNSProtocol.GUID.toString(), guid.getGuid(),
            GNSProtocol.NAME.toString(), guid.getEntityName());
  }


  public void accountGuidRemoveSecure(String name) throws Exception {
    getResponse(CommandType.RemoveAccountSecured, GNSProtocol.NAME.toString(), name);
  }


  public void accountGuidRemoveWithPassword(String name, String password) throws Exception {
    String encodedPassword = Password.encryptAndEncodePassword(password, name);
    getResponse(CommandType.RemoveAccountWithPassword, GNSProtocol.NAME.toString(), name, GNSProtocol.PASSWORD.toString(), encodedPassword);
  }


  public GuidEntry guidCreate(GuidEntry accountGuid, String alias) throws NoSuchAlgorithmException, ClientException, IOException
           {

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


  public void guidRemove(GuidEntry guid) throws ClientException, IOException  {
    getResponse(CommandType.RemoveGuidNoAccount, guid, GNSProtocol.GUID.toString(), guid.getGuid());
  }


  public void guidRemove(GuidEntry accountGuid, String guidToRemove) throws ClientException, IOException
           {
    getResponse(CommandType.RemoveGuid, accountGuid, GNSProtocol.ACCOUNT_GUID.toString(),
            accountGuid.getGuid(), GNSProtocol.GUID.toString(), guidToRemove);
  }

  // GROUP COMMANDS

  public JSONArray groupGetMembers(String groupGuid, GuidEntry reader)
          throws IOException, ClientException, InvalidGuidException {
    try {
      return new JSONArray(getResponse(CommandType.GetGroupMembers,
              reader, GNSProtocol.GUID.toString(), groupGuid, GNSProtocol.READER.toString(), reader.getGuid()));
    } catch (JSONException e) {
      throw new ClientException("Invalid member list", e);
    }
  }


  public JSONArray guidGetGroups(String guid, GuidEntry reader)
          throws IOException, ClientException, InvalidGuidException {
    try {
      return new JSONArray(getResponse(CommandType.GetGroups, reader,
              GNSProtocol.GUID.toString(), guid, GNSProtocol.READER.toString(), reader.getGuid()));
    } catch (JSONException e) {
      throw new ClientException("Invalid member list", e);
    }
  }


  public void groupAddGuid(String groupGuid, String guidToAdd,
          GuidEntry writer) throws IOException, InvalidGuidException,
          ClientException {
    getResponse(CommandType.AddToGroup, writer, GNSProtocol.GUID.toString(), groupGuid, GNSProtocol.MEMBER.toString(),
            guidToAdd, GNSProtocol.WRITER.toString(), writer.getGuid());
  }


  public void groupAddGuids(String groupGuid, JSONArray members,
          GuidEntry writer) throws IOException, ClientException {
    getResponse(CommandType.AddMembersToGroup, writer, GNSProtocol.GUID.toString(), groupGuid,
            GNSProtocol.MEMBERS.toString(), members, GNSProtocol.WRITER.toString(), writer.getGuid());
  }


  public void groupRemoveGuid(String guid, String guidToRemove,
          GuidEntry writer) throws IOException, InvalidGuidException,
          ClientException {
    getResponse(CommandType.RemoveFromGroup, writer, GNSProtocol.GUID.toString(), guid, GNSProtocol.MEMBER.toString(),
            guidToRemove, GNSProtocol.WRITER.toString(), writer.getGuid());
  }


  public void groupRemoveGuids(String guid, JSONArray members,
          GuidEntry writer) throws IOException, ClientException {
    getResponse(CommandType.RemoveMembersFromGroup, writer, GNSProtocol.GUID.toString(), guid,
            GNSProtocol.MEMBERS.toString(), members, GNSProtocol.WRITER.toString(), writer.getGuid());
  }


  public void groupAddMembershipUpdatePermission(GuidEntry groupGuid,
          String guidToAuthorize) throws Exception {
    aclAdd(AclAccessType.WRITE_WHITELIST, groupGuid, GNSProtocol.GROUP_ACL.toString(),
            guidToAuthorize);
  }


  public void groupRemoveMembershipUpdatePermission(GuidEntry groupGuid,
          String guidToUnauthorize) throws Exception {
    aclRemove(AclAccessType.WRITE_WHITELIST, groupGuid, GNSProtocol.GROUP_ACL.toString(),
            guidToUnauthorize);
  }


  public void groupAddMembershipReadPermission(GuidEntry groupGuid,
          String guidToAuthorize) throws Exception {
    aclAdd(AclAccessType.READ_WHITELIST, groupGuid, GNSProtocol.GROUP_ACL.toString(),
            guidToAuthorize);
  }


  public void groupRemoveMembershipReadPermission(GuidEntry groupGuid,
          String guidToUnauthorize) throws Exception {
    aclRemove(AclAccessType.READ_WHITELIST, groupGuid, GNSProtocol.GROUP_ACL.toString(),
            guidToUnauthorize);
  }

  // ACL COMMANDS

  public void aclAdd(AclAccessType accessType, GuidEntry targetGuid,
          String field, String accesserGuid) throws ClientException, IOException   {
    aclAdd(accessType.name(), targetGuid, field, accesserGuid);
  }


  public void aclAddSecure(AclAccessType accessType,
          String guid, String field, String accesserGuid)
          throws ClientException, IOException {
    getResponse(CommandType.AclAddSecured, null,
            GNSProtocol.ACL_TYPE.toString(), accessType.name(),
            GNSProtocol.GUID.toString(), guid,
            GNSProtocol.FIELD.toString(), field,
            GNSProtocol.ACCESSER.toString(), accesserGuid);
  }


  public void aclRemove(AclAccessType accessType, GuidEntry guid,
          String field, String accesserGuid) throws Exception  {
    aclRemove(accessType.name(), guid, field, accesserGuid);
  }


  public void aclRemoveSecure(AclAccessType accessType,
          String guid, String field, String accesserGuid)
          throws ClientException, IOException {
    getResponse(CommandType.AclRemoveSecured, null,
            GNSProtocol.ACL_TYPE.toString(), accessType.name(),
            GNSProtocol.GUID.toString(), guid,
            GNSProtocol.FIELD.toString(), field,
            GNSProtocol.ACCESSER.toString(), accesserGuid);
  }


  public JSONArray aclGet(AclAccessType accessType, GuidEntry guid,
          String field, String readerGuid) throws ClientException, IOException  {
    return aclGet(accessType.name(), guid, field, readerGuid);
  }


  public JSONArray aclGetSecure(AclAccessType accessType, String guid, String field)
          throws ClientException, IOException {
    try {
      return new JSONArray(getResponse(CommandType.AclRetrieveSecured, null,
              GNSProtocol.ACL_TYPE.toString(), accessType.name(),
              GNSProtocol.GUID.toString(), guid,
              GNSProtocol.FIELD.toString(), field));
    } catch (JSONException e) {
      throw new ClientException("Invalid ACL list", e);
    }
  }


  public void fieldCreateAcl(AclAccessType accessType, GuidEntry guid, String field,
          String writerGuid) throws Exception {
    getResponse(CommandType.FieldCreateAcl, guid, GNSProtocol.ACL_TYPE.toString(), accessType.name(), GNSProtocol.GUID.toString(),
            guid.getGuid(), GNSProtocol.FIELD.toString(), field, GNSProtocol.WRITER.toString(), writerGuid);
  }


  public void fieldCreateAcl(AclAccessType accessType, GuidEntry guid, String field) throws Exception {
    fieldCreateAcl(accessType, guid, field, guid.getGuid());
  }


  public void fieldDeleteAcl(AclAccessType accessType, GuidEntry guid, String field,
          String writerGuid) throws Exception {
    getResponse(CommandType.FieldDeleteAcl, guid, GNSProtocol.ACL_TYPE.toString(), accessType.name(),
            GNSProtocol.GUID.toString(), guid.getGuid(), GNSProtocol.FIELD.toString(), field, GNSProtocol.WRITER.toString(), writerGuid);
  }


  public void fieldDeleteAcl(AclAccessType accessType, GuidEntry guid, String field) throws Exception {
    fieldDeleteAcl(accessType, guid, field, guid.getGuid());
  }


  public boolean fieldAclExists(AclAccessType accessType, GuidEntry guid, String field,
          String readerGuid) throws Exception {
    return Boolean.valueOf(getResponse(CommandType.FieldAclExists, guid, GNSProtocol.ACL_TYPE.toString(), accessType.name(),
            GNSProtocol.GUID.toString(), guid.getGuid(), GNSProtocol.FIELD.toString(), field, GNSProtocol.READER.toString(), readerGuid));
  }


  public boolean fieldAclExists(AclAccessType accessType, GuidEntry guid, String field) throws Exception {
    return fieldAclExists(accessType, guid, field, guid.getGuid());
  }

  // ALIASES

  public void addAlias(GuidEntry guid, String name) throws Exception {
    getResponse(CommandType.AddAlias, guid, GNSProtocol.GUID.toString(), guid.getGuid(), GNSProtocol.NAME.toString(),
            name);
  }


  public void removeAlias(GuidEntry guid, String name) throws Exception {
    getResponse(CommandType.RemoveAlias, guid, GNSProtocol.GUID.toString(), guid.getGuid(), GNSProtocol.NAME.toString(),
            name);
  }


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

  private String guidCreateHelper(GuidEntry accountGuid, String name,
          PublicKey publicKey) throws ClientException, IOException  {
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
    GuidEntry entry = GUIDUtilsGNSClient.lookupGuidEntryFromDatabase(this, alias);

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
          String accesserGuid) throws ClientException, IOException  {
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
          String readerGuid) throws ClientException, IOException  {
    try {
      return new JSONArray(getResponse(CommandType.AclRetrieve, guid,
              GNSProtocol.ACL_TYPE.toString(), accessType, GNSProtocol.GUID.toString(), guid.getGuid(), GNSProtocol.FIELD.toString(), field,
              GNSProtocol.READER.toString(), readerGuid == null ? GNSProtocol.ALL_GUIDS.toString() : readerGuid));
    } catch (JSONException e) {
      throw new ClientException("Invalid ACL list", e);
    }
  }

  // Extended commands

  public void fieldCreateList(String targetGuid, String field,
          JSONArray value, GuidEntry writer) throws IOException,
          ClientException {
    getResponse(CommandType.CreateList, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(),
            field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(), writer.getGuid());
  }


  public void fieldAppendOrCreateList(String targetGuid, String field,
          JSONArray value, GuidEntry writer) throws IOException,
          ClientException {
    getResponse(CommandType.AppendOrCreateList, writer, GNSProtocol.GUID.toString(), targetGuid,
            GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(), writer.getGuid());
  }


  public void fieldReplaceOrCreateList(String targetGuid, String field,
          JSONArray value, GuidEntry writer) throws IOException,
          ClientException {
    getResponse(CommandType.ReplaceOrCreateList, writer, GNSProtocol.GUID.toString(), targetGuid,
            GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(), writer.getGuid());
  }


  public void fieldAppend(String targetGuid, String field, JSONArray value,
          GuidEntry writer) throws IOException, ClientException {
    getResponse(CommandType.AppendListWithDuplication, writer, GNSProtocol.GUID.toString(),
            targetGuid, GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(),
            writer.getGuid());
  }


  public void fieldReplaceList(String targetGuid, String field,
          JSONArray value, GuidEntry writer) throws IOException,
          ClientException {
    getResponse(CommandType.ReplaceList, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(),
            field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(), writer.getGuid());
  }


  public void fieldClear(String targetGuid, String field, JSONArray value,
          GuidEntry writer) throws IOException, ClientException {
    getResponse(CommandType.RemoveList, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(),
            field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(), writer.getGuid());
  }


  public void fieldClear(String targetGuid, String field, GuidEntry writer)
          throws IOException, ClientException {
    getResponse(CommandType.Clear, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(), field,
            GNSProtocol.WRITER.toString(), writer.getGuid());
  }


  public JSONArray fieldReadArray(String guid, String field, GuidEntry reader)
          throws Exception {
    return commandResponseToJSONArray(field,
            (getResponse(reader != null ? CommandType.ReadArray
                    : CommandType.ReadArrayUnsigned, reader, GNSProtocol.GUID.toString(), guid, GNSProtocol.FIELD.toString(),
                    field, GNSProtocol.READER.toString(), reader != null ? reader.getGuid() : null)));
  }


  public void fieldSetElement(String targetGuid, String field,
          String newValue, int index, GuidEntry writer) throws IOException,
          ClientException {
    getResponse(CommandType.Set, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(), field,
            GNSProtocol.VALUE.toString(), newValue, GNSProtocol.N.toString(), Integer.toString(index), GNSProtocol.WRITER.toString(),
            writer.getGuid());
  }


  public void fieldSetNull(String targetGuid, String field, GuidEntry writer)
          throws IOException, ClientException {
    getResponse(CommandType.SetFieldNull, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(),
            field, GNSProtocol.WRITER.toString(), writer.getGuid());
  }

  //
  // SELECT
  //

  public JSONArray select(String field, String value) throws Exception {
    return new JSONArray(getResponse(CommandType.Select,
            GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value));
  }


  public JSONArray selectWithin(String field, JSONArray value)
          throws Exception {
    return new JSONArray(getResponse(CommandType.SelectWithin, GNSProtocol.FIELD.toString(), field, GNSProtocol.WITHIN.toString(),
            value));
  }


  public JSONArray selectNear(String field, JSONArray value,
          Double maxDistance) throws Exception {
    return new JSONArray(getResponse(CommandType.SelectNear, GNSProtocol.FIELD.toString(), field, GNSProtocol.NEAR.toString(), value,
            GNSProtocol.MAX_DISTANCE.toString(), Double.toString(maxDistance)));
  }


  public void setLocation(String targetGuid, double longitude,
          double latitude, GuidEntry writer) throws Exception {
    fieldReplaceOrCreateList(targetGuid, GNSProtocol.LOCATION_FIELD_NAME.toString(),
            new JSONArray(Arrays.asList(longitude, latitude)), writer);
  }


  public void setLocation(GuidEntry guid, double longitude, double latitude)
          throws Exception {
    setLocation(guid.getGuid(), longitude, latitude, guid);
  }


  public JSONArray getLocation(String targetGuid, GuidEntry readerGuid)
          throws Exception {
    return fieldReadArray(targetGuid, GNSProtocol.LOCATION_FIELD_NAME.toString(), readerGuid);
  }


  public JSONArray getLocation(GuidEntry guid) throws Exception {
    return fieldReadArray(guid.getGuid(), GNSProtocol.LOCATION_FIELD_NAME.toString(), guid);
  }


  // Active Code
  public void activeCodeClear(String guid, String action, GuidEntry writerGuid)
          throws ClientException, IOException {
    getResponse(CommandType.ClearCode, writerGuid, GNSProtocol.GUID.toString(), guid,
            GNSProtocol.AC_ACTION.toString(), action, GNSProtocol.WRITER.toString(), writerGuid.getGuid());
  }


  public void activeCodeSet(String guid, String action, String code,
          GuidEntry writerGuid) throws ClientException, IOException {
    getResponse(CommandType.SetCode, writerGuid, GNSProtocol.GUID.toString(), guid,
            GNSProtocol.AC_ACTION.toString(), action, GNSProtocol.AC_CODE.toString(), code,
            GNSProtocol.WRITER.toString(), writerGuid.getGuid());
  }


  public String activeCodeGet(String guid, String action, GuidEntry readerGuid)
          throws Exception {
    String code = getResponse(CommandType.GetCode,
            readerGuid, GNSProtocol.GUID.toString(), guid, 
            GNSProtocol.AC_ACTION.toString(), action, 
            GNSProtocol.READER.toString(),
            readerGuid.getGuid());
    return code;
  }

  // Extended commands

  public void fieldCreateList(GuidEntry target, String field, JSONArray value)
          throws IOException, ClientException {
    fieldCreateList(target.getGuid(), field, value, target);
  }


  public void fieldCreateOneElementList(String targetGuid, String field,
          String value, GuidEntry writer) throws IOException, ClientException {
    getResponse(CommandType.Create, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(),
            field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(), writer.getGuid());
  }


  public void fieldCreateOneElementList(GuidEntry target, String field,
          String value) throws IOException, ClientException {
    fieldCreateOneElementList(target.getGuid(), field, value, target);
  }


  public void fieldAppendOrCreate(String targetGuid, String field,
          String value, GuidEntry writer) throws IOException, ClientException {
    getResponse(CommandType.AppendOrCreate, writer, GNSProtocol.GUID.toString(), targetGuid,
            GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(), writer.getGuid());
  }


  public void fieldReplaceOrCreate(String targetGuid, String field,
          String value, GuidEntry writer) throws IOException, ClientException {
    getResponse(CommandType.ReplaceOrCreate, writer, GNSProtocol.GUID.toString(), targetGuid,
            GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(), writer.getGuid());
  }


  public void fieldReplaceOrCreateList(GuidEntry targetGuid, String field,
          JSONArray value) throws IOException, ClientException {
    fieldReplaceOrCreateList(targetGuid.getGuid(), field, value, targetGuid);
  }


  public void fieldReplaceOrCreateList(GuidEntry target, String field,
          String value) throws IOException, ClientException {
    fieldReplaceOrCreate(target.getGuid(), field, value, target);
  }


  public void fieldReplace(String targetGuid, String field, String value,
          GuidEntry writer) throws IOException, ClientException {
    getResponse(CommandType.Replace, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(),
            field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(), writer.getGuid());
  }


  public void fieldReplace(GuidEntry target, String field, String value)
          throws IOException, ClientException {
    fieldReplace(target.getGuid(), field, value, target);
  }


  public void fieldReplace(GuidEntry target, String field, JSONArray value)
          throws IOException, ClientException {
    fieldReplaceList(target.getGuid(), field, value, target);
  }


  public void fieldAppend(String targetGuid, String field, String value,
          GuidEntry writer) throws IOException, ClientException {
    getResponse(CommandType.AppendWithDuplication, writer, GNSProtocol.GUID.toString(),
            targetGuid, GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(),
            writer.getGuid());
  }


  public void fieldAppend(GuidEntry target, String field, String value)
          throws IOException, ClientException {
    fieldAppend(target.getGuid(), field, value, target);
  }


  public void fieldAppendWithSetSemantics(String targetGuid, String field,
          JSONArray value, GuidEntry writer) throws IOException,
          ClientException {
    getResponse(CommandType.AppendList, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(),
            field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(), writer.getGuid());
  }


  public void fieldAppendWithSetSemantics(GuidEntry target, String field,
          JSONArray value) throws IOException, ClientException {
    fieldAppendWithSetSemantics(target.getGuid(), field, value, target);
  }


  public void fieldAppendWithSetSemantics(String targetGuid, String field,
          String value, GuidEntry writer) throws IOException, ClientException {
    getResponse(CommandType.Append, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(), field,
            GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(), writer.getGuid());
  }


  public void fieldAppendWithSetSemantics(GuidEntry target, String field,
          String value) throws IOException, ClientException {
    fieldAppendWithSetSemantics(target.getGuid(), field, value, target);
  }


  //@Deprecated
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


  @Deprecated
  public void fieldReplaceFirstElement(GuidEntry target, String field,
          String value) throws IOException, ClientException {
    fieldReplaceFirstElement(target.getGuid(), field, value, target);
  }


  public void fieldSubstitute(String targetGuid, String field,
          String newValue, String oldValue, GuidEntry writer)
          throws IOException, ClientException {
    getResponse(CommandType.Substitute, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(),
            field, GNSProtocol.VALUE.toString(), newValue, GNSProtocol.OLD_VALUE.toString(), oldValue, GNSProtocol.WRITER.toString(),
            writer.getGuid());
  }


  public void fieldSubstitute(GuidEntry target, String field,
          String newValue, String oldValue) throws IOException,
          ClientException {
    fieldSubstitute(target.getGuid(), field, newValue, oldValue, target);
  }


  public void fieldSubstitute(String targetGuid, String field,
          JSONArray newValue, JSONArray oldValue, GuidEntry writer)
          throws IOException, ClientException {
    getResponse(CommandType.SubstituteList, writer, GNSProtocol.GUID.toString(), targetGuid,
            GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), newValue, GNSProtocol.OLD_VALUE.toString(),
            oldValue, GNSProtocol.WRITER.toString(), writer.getGuid());
  }


  public void fieldSubstitute(GuidEntry target, String field,
          JSONArray newValue, JSONArray oldValue) throws IOException,
          ClientException {
    fieldSubstitute(target.getGuid(), field, newValue, oldValue, target);
  }


  //@Deprecated
  public String fieldReadArrayFirstElement(String guid, String field,
          GuidEntry reader) throws Exception {
    return getResponse(reader != null ? CommandType.ReadArrayOne
            : CommandType.ReadArrayOneUnsigned, reader, GNSProtocol.GUID.toString(), guid, GNSProtocol.FIELD.toString(),
            field, GNSProtocol.READER.toString(), reader != null ? reader.getGuid() : null);
  }


  @Deprecated
  public String fieldReadArrayFirstElement(GuidEntry guid, String field)
          throws Exception {
    return fieldReadArrayFirstElement(guid.getGuid(), field, guid);
  }


  public void fieldRemove(GuidEntry guid, String field) throws IOException, ClientException {
    fieldRemove(guid.getGuid(), field, guid);
  }


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
