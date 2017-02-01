
package edu.umass.cs.gnsclient.client.http;

import edu.umass.cs.gnsclient.client.CommandUtils;
import edu.umass.cs.gnsclient.client.CryptoUtils;
import edu.umass.cs.gnsclient.client.GNSClientConfig;
import edu.umass.cs.gnsclient.client.http.android.DownloadTask;
import edu.umass.cs.gnsclient.client.util.GUIDUtilsHTTPClient;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.client.util.Password;
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.SharedGuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.client.EncryptionException;
import edu.umass.cs.gnscommon.exceptions.client.FieldNotFoundException;
import edu.umass.cs.gnscommon.exceptions.client.InvalidGuidException;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnscommon.utils.CanonicalJSON;
import edu.umass.cs.gnscommon.utils.URIEncoderDecoder;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.utils.Config;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
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
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;


public class HttpClient {

  private static final java.util.logging.Logger LOGGER = GNSConfig.getLogger();

  public static final boolean IS_ANDROID = System.getProperty("java.vm.name").equalsIgnoreCase("Dalvik");

  private final static String QUERYPREFIX = "?";
  private final static String VALSEP = "=";
  private final static String KEYSEP = "&";

  private static final String GNS_KEY = "GNS";

  private final String host;

  private final int port;

  private int readTimeout = 10000;

  private int readRetries = 1;

  private static final boolean includeTimestamp = false;


  public HttpClient(String host, int port) {

    this.host = host;
    this.port = port;
  }


  public String getGNSProvider() {
    return System.getProperty(GNS_KEY) != null ? System.getProperty(GNS_KEY)
            : host + ":" + port;
  }


  public int getReadTimeout() {
    return readTimeout;
  }


  public void setReadTimeout(int readTimeout) {
    this.readTimeout = readTimeout;
  }


  public int getReadRetries() {
    return readRetries;
  }


  public void setReadRetries(int readRetries) {
    this.readRetries = readRetries;
  }


  public String getHelp() throws IOException {
    return sendGetCommand("help");
  }


  public String lookupGuid(String alias) throws UnsupportedEncodingException, IOException, ClientException {
    return getResponse(CommandType.LookupGuid,
            GNSProtocol.NAME.toString(), alias);
  }


  public String lookupPrimaryGuid(String guid) throws UnsupportedEncodingException, IOException, ClientException {
    return getResponse(CommandType.LookupPrimaryGuid, GNSProtocol.GUID.toString(), guid);
  }


  public JSONObject lookupGuidRecord(String guid) throws IOException, ClientException {
    try {
      return new JSONObject(getResponse(CommandType.LookupGuidRecord, GNSProtocol.GUID.toString(), guid));
    } catch (JSONException e) {
      throw new ClientException("Failed to parse LOOKUP_GUID_RECORD response", e);
    }
  }


  public JSONObject lookupAccountRecord(String gaccountGuid) throws IOException, ClientException {
    try {
      return new JSONObject(getResponse(CommandType.LookupAccountRecord, GNSProtocol.GUID.toString(), gaccountGuid));
    } catch (JSONException e) {
      throw new ClientException("Failed to parse LOOKUP_ACCOUNT_RECORD response", e);
    }
  }


  public PublicKey publicKeyLookupFromAlias(String alias) throws InvalidGuidException, ClientException, IOException {
    String guid = lookupGuid(alias);
    return publicKeyLookupFromGuid(guid);
  }


  public PublicKey publicKeyLookupFromGuid(String guid) throws InvalidGuidException, ClientException, IOException {
    JSONObject guidInfo = lookupGuidRecord(guid);
    try {
      String key = guidInfo.getString(GNSProtocol.GUID_RECORD_PUBLICKEY.toString());
      byte[] encodedPublicKey = Base64.decode(key);
      KeyFactory keyFactory = KeyFactory.getInstance(GNSProtocol.RSA_ALGORITHM.toString());
      X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(encodedPublicKey);
      return keyFactory.generatePublic(publicKeySpec);
    } catch (JSONException e) {
      throw new ClientException("Failed to parse LOOKUP_USER response", e);
    } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
      throw new EncryptionException("Public key encryption failed", e);
    }

  }


  public GuidEntry accountGuidCreate(String alias, String password)
          throws IOException, ClientException, NoSuchAlgorithmException {

    GuidEntry entry = GUIDUtilsHTTPClient.lookupGuidEntryFromDatabase(this, alias);
    // Don't recreate pair if one already exists. Otherwise you can
    // not get out of the funk where the account creation timed out but
    // wasn't rolled back fully at the server. Re-using
    // the same GNSProtocol.GUID.toString() will at least pass verification as opposed to 
    // incurring an GNSProtocol.ACTIVE_REPLICA_EXCEPTION.toString() for a new (non-existent) GNSProtocol.GUID.toString().
    if (entry == null) {
      KeyPair keyPair = KeyPairGenerator.getInstance(GNSProtocol.RSA_ALGORITHM.toString())
              .generateKeyPair();
      String guid = SharedGuidUtils.createGuidStringFromPublicKey(keyPair
              .getPublic().getEncoded());
      // Squirrel this away now just in case the call below times out.
      KeyPairUtils.saveKeyPair(getGNSProvider(), alias, guid, keyPair);
      entry = new GuidEntry(alias, guid, keyPair.getPublic(),
              keyPair.getPrivate());
    }
    assert (entry != null);
    String returnedGuid = accountGuidCreate(alias, entry, password);

    // Anything else we want to do here?
    if (!returnedGuid.equals(entry.guid)) {
      GNSClientConfig
              .getLogger()
              .log(Level.WARNING,
                      "Returned guid {0} doesn't match locally created guid {1}",
                      new Object[]{returnedGuid, entry.guid});
    }
    assert returnedGuid.equals(entry.guid);
    return entry;
  }


  public String accountGuidVerify(GuidEntry guid, String code) throws IOException, ClientException {
    return getResponse(CommandType.VerifyAccount, guid, GNSProtocol.GUID.toString(), guid.getGuid(),
            GNSProtocol.CODE.toString(), code);
  }


  public void accountGuidRemove(GuidEntry guid) throws IOException, ClientException {
    getResponse(CommandType.RemoveAccount, guid,
            GNSProtocol.GUID.toString(), guid.getGuid(),
            GNSProtocol.NAME.toString(), guid.getEntityName());
  }


  public GuidEntry guidCreate(GuidEntry accountGuid, String alias)
          throws IOException, ClientException, NoSuchAlgorithmException {

    KeyPair keyPair = KeyPairGenerator.getInstance(GNSProtocol.RSA_ALGORITHM.toString()).generateKeyPair();
    String newGuid = guidCreate(accountGuid, alias, keyPair.getPublic());

    KeyPairUtils.saveKeyPair(host + ":" + port, alias, newGuid, keyPair);

    GuidEntry entry = new GuidEntry(alias, newGuid, keyPair.getPublic(), keyPair.getPrivate());

    return entry;
  }


  public void guidRemove(GuidEntry guid) throws IOException, ClientException {
    getResponse(CommandType.RemoveGuid, guid,
            GNSProtocol.GUID.toString(), guid.getGuid());
  }


  public void guidRemove(GuidEntry accountGuid, String guidToRemove) throws IOException, ClientException {
    getResponse(CommandType.RemoveGuid, accountGuid,
            GNSProtocol.ACCOUNT_GUID.toString(), accountGuid.getGuid(),
            GNSProtocol.GUID.toString(), guidToRemove);
  }

  //
  // BASIC READS AND WRITES
  //

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
          GuidEntry writer) throws IOException, ClientException,
          JSONException {
    getResponse(CommandType.ReplaceUserJSON, writer, GNSProtocol.GUID.toString(), targetGuid,
            GNSProtocol.USER_JSON.toString(), new JSONObject().put(field, value), GNSProtocol.WRITER.toString(), writer.getGuid());
  }


  public void fieldCreateIndex(GuidEntry guid, String field, String index)
          throws IOException, ClientException, JSONException {
    getResponse(CommandType.CreateIndex, guid, GNSProtocol.GUID.toString(), guid.getGuid(), GNSProtocol.FIELD.toString(),
            field, GNSProtocol.VALUE.toString(), index, GNSProtocol.WRITER.toString(), guid.getGuid());
  }


  public void fieldUpdate(GuidEntry targetGuid, String field, Object value)
          throws IOException, ClientException, JSONException {
    fieldUpdate(targetGuid.getGuid(), field, value, targetGuid);
  }


  public JSONObject read(String targetGuid, GuidEntry reader)
          throws IOException, ClientException, JSONException {
    return new JSONObject(getResponse(reader != null ? CommandType.ReadArray
            : CommandType.ReadArrayUnsigned, reader, GNSProtocol.GUID.toString(),
            targetGuid, GNSProtocol.FIELD.toString(), GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.READER.toString(),
            reader != null ? reader.getGuid() : null));
  }


  public JSONObject read(GuidEntry guid) throws IOException, ClientException, JSONException {
    return read(guid.getGuid(), guid);
  }


  public boolean fieldExists(String targetGuid, String field, GuidEntry reader)
          throws IOException, ClientException {
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
          throws IOException, ClientException {
    return fieldExists(targetGuid.getGuid(), field, targetGuid);
  }


  public String fieldRead(String targetGuid, String field, GuidEntry reader)
          throws IOException, ClientException {
    if (reader != null) {
      return CommandUtils.specialCaseSingleField(getResponse(CommandType.Read, reader,
              GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(), field, GNSProtocol.READER.toString(), reader.getGuid()));
    } else {
      return CommandUtils.specialCaseSingleField(getResponse(CommandType.ReadUnsigned, reader,
              GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(), field, GNSProtocol.READER.toString(), null));
    }
  }


  public String fieldRead(GuidEntry targetGuid, String field)
          throws IOException, ClientException {
    return fieldRead(targetGuid.getGuid(), field, targetGuid);
  }


  public String fieldRead(String targetGuid, ArrayList<String> fields,
          GuidEntry reader) throws IOException, ClientException {
    return getResponse(reader != null ? CommandType.ReadMultiField
            : CommandType.ReadMultiFieldUnsigned, reader, GNSProtocol.GUID.toString(), targetGuid,
            GNSProtocol.FIELDS.toString(), fields, GNSProtocol.READER.toString(), reader != null ? reader.getGuid()
                    : null);
  }


  public String fieldRead(GuidEntry targetGuid, ArrayList<String> fields)
          throws IOException, ClientException {
    return fieldRead(targetGuid.getGuid(), fields, targetGuid);
  }


  public void fieldCreateList(String targetGuid, String field, JSONArray value, GuidEntry writer) throws IOException,
          ClientException {
    getResponse(CommandType.CreateList, writer, GNSProtocol.GUID.toString(), targetGuid,
            GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value,
            GNSProtocol.WRITER.toString(), writer.getGuid());
  }


  public void fieldRemove(String targetGuid, String field, GuidEntry writer) throws IOException, InvalidKeyException,
          NoSuchAlgorithmException, SignatureException, ClientException {
    getResponse(CommandType.RemoveField, writer, GNSProtocol.GUID.toString(), targetGuid,
            GNSProtocol.FIELD.toString(), field, GNSProtocol.WRITER.toString(), writer.getGuid());
  }


  public void fieldAppendOrCreateList(String targetGuid, String field, JSONArray value, GuidEntry writer)
          throws IOException, ClientException {
    getResponse(CommandType.AppendOrCreateList, writer, GNSProtocol.GUID.toString(), targetGuid,
            GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value,
            GNSProtocol.WRITER.toString(), writer.getGuid());
  }


  public void fieldReplaceOrCreateList(String targetGuid, String field, JSONArray value, GuidEntry writer)
          throws IOException, ClientException {
    getResponse(CommandType.ReplaceOrCreateList, writer, GNSProtocol.GUID.toString(), targetGuid,
            GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value,
            GNSProtocol.WRITER.toString(), writer.getGuid());
  }


  public void fieldAppendList(String targetGuid, String field, JSONArray value, GuidEntry writer) throws IOException,
          ClientException {
    getResponse(CommandType.AppendListWithDuplication, writer, GNSProtocol.GUID.toString(), targetGuid,
            GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value,
            GNSProtocol.WRITER.toString(), writer.getGuid());
  }


  public void fieldReplaceList(String targetGuid, String field, JSONArray value, GuidEntry writer) throws IOException,
          ClientException {
    getResponse(CommandType.ReplaceList, writer, GNSProtocol.GUID.toString(), targetGuid,
            GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value,
            GNSProtocol.WRITER.toString(), writer.getGuid());
  }


  public void fieldClear(String targetGuid, String field, JSONArray value, GuidEntry writer) throws IOException,
          ClientException {
    getResponse(CommandType.RemoveList, writer, GNSProtocol.GUID.toString(), targetGuid,
            GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value,
            GNSProtocol.WRITER.toString(), writer.getGuid());
  }


  public void fieldClear(String targetGuid, String field, GuidEntry writer) throws IOException, ClientException {
    getResponse(CommandType.Clear, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(),
            field, GNSProtocol.WRITER.toString(), writer.getGuid());
  }


  public JSONArray fieldReadArray(String guid, String field, GuidEntry reader)
          throws IOException, ClientException, JSONException {
    String response;
    if (reader == null) {
      response = getResponse(CommandType.ReadArrayUnsigned, GNSProtocol.GUID.toString(),
              guid, GNSProtocol.FIELD.toString(), field);
    } else {
      response = getResponse(CommandType.ReadArray, reader, GNSProtocol.GUID.toString(),
              guid, GNSProtocol.FIELD.toString(), field,
              GNSProtocol.READER.toString(), reader.getGuid());
    }
    return CommandUtils.commandResponseToJSONArray(field, response);
  }


  public void fieldSetElement(String targetGuid, String field, String newValue, int index, GuidEntry writer)
          throws IOException, ClientException {
    getResponse(CommandType.Set, writer, GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(),
            field, GNSProtocol.VALUE.toString(), newValue, GNSProtocol.N.toString(), Integer.toString(index),
            GNSProtocol.WRITER.toString(), writer.getGuid());
  }


  public void fieldSetNull(String targetGuid, String field, GuidEntry writer) throws IOException,
          InvalidKeyException, NoSuchAlgorithmException, SignatureException,
          ClientException {
    getResponse(CommandType.SetFieldNull, writer,
            GNSProtocol.GUID.toString(), targetGuid, GNSProtocol.FIELD.toString(), field,
            GNSProtocol.WRITER.toString(), writer.getGuid());
  }

  /// GROUPS AND ACLS

  public JSONArray guidGetGroups(String groupGuid, GuidEntry reader) throws IOException, ClientException,
          InvalidGuidException {
    try {
      return new JSONArray(getResponse(CommandType.GetGroups, reader, GNSProtocol.GUID.toString(), groupGuid,
              GNSProtocol.READER.toString(), reader.getGuid()));
    } catch (JSONException e) {
      throw new ClientException("Invalid member list", e);
    }
  }


  public void groupAddGuid(String groupGuid, String guidToAdd, GuidEntry writer) throws IOException,
          InvalidGuidException, ClientException {
    getResponse(CommandType.AddToGroup, writer, GNSProtocol.GUID.toString(), groupGuid,
            GNSProtocol.MEMBER.toString(), guidToAdd, GNSProtocol.WRITER.toString(), writer.getGuid());
  }


  public void groupAddGuids(String groupGuid, JSONArray members, GuidEntry writer) throws IOException,
          InvalidGuidException, ClientException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    getResponse(CommandType.AddToGroup, writer,
            GNSProtocol.GUID.toString(), groupGuid,
            GNSProtocol.MEMBERS.toString(), members,
            GNSProtocol.WRITER.toString(), writer.getGuid());
  }


  public void groupRemoveGuid(String guid, String guidToRemove, GuidEntry writer) throws IOException,
          InvalidGuidException, ClientException {
    getResponse(CommandType.RemoveFromGroup, writer, GNSProtocol.GUID.toString(), guid,
            GNSProtocol.MEMBER.toString(), guidToRemove, GNSProtocol.WRITER.toString(), writer.getGuid());

  }


  public void groupRemoveGuids(String guid, JSONArray members, GuidEntry writer) throws IOException,
          InvalidGuidException, ClientException, InvalidKeyException, NoSuchAlgorithmException, SignatureException {
    getResponse(CommandType.RemoveFromGroup, writer, GNSProtocol.GUID.toString(), guid,
            GNSProtocol.MEMBERS.toString(), members,
            GNSProtocol.WRITER.toString(), writer.getGuid());

  }


  public JSONArray groupGetMembers(String groupGuid, GuidEntry reader) throws IOException, ClientException,
          InvalidGuidException {
    try {
      return new JSONArray(getResponse(CommandType.GetGroupMembers, reader, GNSProtocol.GUID.toString(), groupGuid,
              GNSProtocol.READER.toString(), reader.getGuid()));
    } catch (JSONException e) {
      throw new ClientException("Invalid member list", e);
    }
  }


  public void groupAddMembershipUpdatePermission(GuidEntry groupGuid, String guidToAuthorize) throws IOException, ClientException {
    aclAdd(AclAccessType.WRITE_WHITELIST, groupGuid, GNSProtocol.GROUP_ACL.toString(), guidToAuthorize);
  }


  public void groupRemoveMembershipUpdatePermission(GuidEntry groupGuid, String guidToUnauthorize) throws IOException, ClientException {
    aclRemove(AclAccessType.WRITE_WHITELIST, groupGuid, GNSProtocol.GROUP_ACL.toString(), guidToUnauthorize);
  }


  public void groupAddMembershipReadPermission(GuidEntry groupGuid, String guidToAuthorize) throws IOException, ClientException {
    aclAdd(AclAccessType.READ_WHITELIST, groupGuid, GNSProtocol.GROUP_ACL.toString(), guidToAuthorize);
  }


  public void groupRemoveMembershipReadPermission(GuidEntry groupGuid, String guidToUnauthorize) throws IOException, ClientException {
    aclRemove(AclAccessType.READ_WHITELIST, groupGuid, GNSProtocol.GROUP_ACL.toString(), guidToUnauthorize);
  }


  public void aclAdd(AclAccessType accessType, GuidEntry targetGuid, String field, String accesserGuid)
          throws IOException, ClientException {
    aclAdd(accessType.name(), targetGuid, field, accesserGuid);
  }


  public void aclRemove(AclAccessType accessType, GuidEntry guid, String field, String accesserGuid)
          throws IOException, ClientException {
    aclRemove(accessType.name(), guid, field, accesserGuid);
  }


  public JSONArray aclGet(AclAccessType accessType, GuidEntry guid, String field, String accesserGuid)
          throws IOException, ClientException {
    return aclGet(accessType.name(), guid, field, accesserGuid);
  }

  //
  // SELECT
  //

  public JSONArray select(String field, String value) throws IOException, ClientException, JSONException {
    return new JSONArray(getResponse(CommandType.Select, GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value));
  }


  public JSONArray selectWithin(String field, JSONArray value)
          throws IOException, ClientException, JSONException {
    return new JSONArray(getResponse(CommandType.SelectWithin, GNSProtocol.FIELD.toString(), field,
            GNSProtocol.WITHIN.toString(), value));
  }


  public JSONArray selectNear(String field, JSONArray value, Double maxDistance)
          throws IOException, ClientException, JSONException {
    return new JSONArray(getResponse(CommandType.SelectNear, GNSProtocol.FIELD.toString(), field,
            GNSProtocol.NEAR.toString(), value,
            GNSProtocol.MAX_DISTANCE.toString(), Double.toString(maxDistance)));
  }


  public JSONArray selectQuery(String query) throws IOException, ClientException, JSONException {
    return new JSONArray(getResponse(CommandType.SelectQuery, GNSProtocol.QUERY.toString(), query));
  }


  public JSONArray selectSetupGroupQuery(String guid, String query)
          throws IOException, ClientException, JSONException {
    return new JSONArray(getResponse(CommandType.SelectGroupSetupQuery, GNSProtocol.GUID.toString(), guid,
            GNSProtocol.QUERY.toString(), query));
  }


  public JSONArray selectLookupGroupQuery(String guid) throws IOException, ClientException, JSONException {
    return new JSONArray(getResponse(CommandType.SelectGroupLookupQuery, GNSProtocol.GUID.toString(), guid));
  }


  public void setLocation(double longitude, double latitude, GuidEntry guid) throws IOException, ClientException {
    JSONArray array = new JSONArray(Arrays.asList(longitude, latitude));
    fieldReplaceOrCreateList(guid.getGuid(), GNSProtocol.LOCATION_FIELD_NAME.toString(), array, guid);
  }


  public JSONArray getLocation(GuidEntry readerGuid, String targetGuid)
          throws IOException, ClientException, JSONException {
    return fieldReadArray(targetGuid, GNSProtocol.LOCATION_FIELD_NAME.toString(), readerGuid);
  }


  public void addAlias(GuidEntry guid, String name) throws IOException, ClientException {
    getResponse(CommandType.AddAlias, guid,
            GNSProtocol.GUID.toString(), guid.getGuid(), GNSProtocol.NAME.toString(), name);
  }


  public void removeAlias(GuidEntry guid, String name) throws IOException, ClientException {
    getResponse(CommandType.RemoveAlias, guid,
            GNSProtocol.GUID.toString(), guid.getGuid(), GNSProtocol.NAME.toString(), name);
  }


  public JSONArray getAliases(GuidEntry guid) throws IOException, ClientException {
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

  private String guidCreate(GuidEntry accountGuid, String name, PublicKey publicKey) throws IOException, ClientException {
    byte[] publicKeyBytes = publicKey.getEncoded();
    String publicKeyString = Base64.encodeToString(publicKeyBytes, false);
    return getResponse(CommandType.AddGuid, accountGuid,
            GNSProtocol.GUID.toString(), accountGuid.getGuid(),
            GNSProtocol.NAME.toString(), name,
            GNSProtocol.PUBLIC_KEY.toString(), publicKeyString);
  }


  private String accountGuidCreate(String alias, GuidEntry guidEntry, String password) throws UnsupportedEncodingException, IOException,
          ClientException, InvalidGuidException, NoSuchAlgorithmException {
    byte[] publicKeyBytes = guidEntry.getPublicKey().getEncoded();
    String publicKeyString = Base64.encodeToString(publicKeyBytes, false);
    return getResponse(CommandType.RegisterAccount, guidEntry,
            GNSProtocol.NAME.toString(), alias,
            GNSProtocol.PUBLIC_KEY.toString(), publicKeyString,
            GNSProtocol.PASSWORD.toString(),
            password != null
                    ? Password.encryptAndEncodePassword(password, alias)
                    : "");
  }


  private void aclAdd(String accessType, GuidEntry guid, String field, String accesserGuid) throws IOException, ClientException {
    getResponse(accesserGuid == null ? CommandType.AclAdd : CommandType.AclAddSelf, guid,
            GNSProtocol.ACL_TYPE.toString(), accessType, GNSProtocol.GUID.toString(),
            guid.getGuid(), GNSProtocol.FIELD.toString(), field, GNSProtocol.ACCESSER.toString(), accesserGuid == null
                    ? GNSProtocol.ALL_GUIDS.toString()
                    : accesserGuid);
  }


  private void aclRemove(String accessType, GuidEntry guid, String field, String accesserGuid) throws IOException, ClientException {
    getResponse(accesserGuid == null ? CommandType.AclRemove : CommandType.AclRemoveSelf, guid,
            GNSProtocol.ACL_TYPE.toString(), accessType,
            GNSProtocol.GUID.toString(), guid.getGuid(), GNSProtocol.FIELD.toString(), field,
            GNSProtocol.ACCESSER.toString(), accesserGuid == null
                    ? GNSProtocol.ALL_GUIDS.toString()
                    : accesserGuid);
  }


  private JSONArray aclGet(String accessType, GuidEntry guid, String field, String accesserGuid) throws IOException, ClientException {
    try {
      return new JSONArray(getResponse(accesserGuid == null ? CommandType.AclRetrieve : CommandType.AclRetrieveSelf, guid,
              GNSProtocol.ACL_TYPE.toString(), accessType,
              GNSProtocol.GUID.toString(), guid.getGuid(), GNSProtocol.FIELD.toString(), field,
              GNSProtocol.ACCESSER.toString(), accesserGuid == null
                      ? GNSProtocol.ALL_GUIDS.toString()
                      : accesserGuid));
    } catch (JSONException e) {
      throw new ClientException("Invalid ACL list", e);
    }
  }

  //
  // Extended Methods
  //

  public void fieldCreateSingleElementList(String targetGuid, String field, String value, GuidEntry writer) throws IOException,
          ClientException {
    getResponse(CommandType.Create, writer, GNSProtocol.GUID.toString(), targetGuid,
            GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value,
            GNSProtocol.WRITER.toString(), writer.getGuid());
  }


  public void fieldCreateSingleElementList(GuidEntry targetGuid, String field, String value) throws IOException,
          ClientException {
    HttpClient.this.fieldCreateSingleElementList(targetGuid.getGuid(), field, value, targetGuid);
  }


  public void fieldAppendOrCreate(String targetGuid, String field, String value, GuidEntry writer)
          throws IOException, ClientException {
    getResponse(CommandType.AppendOrCreateList, writer, GNSProtocol.GUID.toString(), targetGuid,
            GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value,
            GNSProtocol.WRITER.toString(), writer.getGuid());
  }


  public void fieldReplaceOrCreate(String targetGuid, String field, String value, GuidEntry writer)
          throws IOException, ClientException {
    getResponse(CommandType.ReplaceOrCreateList, writer, GNSProtocol.GUID.toString(), targetGuid,
            GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value,
            GNSProtocol.WRITER.toString(), writer.getGuid());
  }


  public void fieldAppend(String targetGuid, String field, String value, GuidEntry writer) throws IOException,
          ClientException {
    getResponse(CommandType.AppendListWithDuplication, writer, GNSProtocol.GUID.toString(), targetGuid,
            GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value,
            GNSProtocol.WRITER.toString(), writer.getGuid());
  }


  public void fieldAppendWithSetSemantics(String targetGuid, String field, JSONArray value, GuidEntry writer)
          throws IOException, ClientException {
    getResponse(CommandType.AppendList, writer, GNSProtocol.GUID.toString(), targetGuid,
            GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value,
            GNSProtocol.WRITER.toString(), writer.getGuid());
  }


  public void fieldAppendWithSetSemantics(String targetGuid, String field, String value, GuidEntry writer)
          throws IOException, ClientException {
    getResponse(CommandType.Append, writer, GNSProtocol.GUID.toString(), targetGuid,
            GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value,
            GNSProtocol.WRITER.toString(), writer.getGuid());
  }


  public void fieldReplaceFirstElement(String targetGuid, String field, String value, GuidEntry writer)
          throws IOException, ClientException {
    getResponse(CommandType.ReplaceList, writer, GNSProtocol.GUID.toString(), targetGuid,
            GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value,
            GNSProtocol.WRITER.toString(), writer.getGuid());
  }


  public void fieldSubstitute(String targetGuid, String field, String newValue,
          String oldValue, GuidEntry writer) throws IOException, ClientException {
    getResponse(CommandType.SubstituteList, writer,
            GNSProtocol.GUID.toString(), targetGuid,
            GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), newValue,
            GNSProtocol.OLD_VALUE.toString(), oldValue);
  }


  public void fieldSubstitute(String targetGuid, String field,
          JSONArray newValue, JSONArray oldValue, GuidEntry writer) throws IOException, ClientException {
    getResponse(CommandType.SubstituteList, writer, GNSProtocol.GUID.toString(),
            targetGuid, GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), newValue,
            GNSProtocol.OLD_VALUE.toString(), oldValue);
  }


  public String fieldReadFirstElement(String guid, String field, GuidEntry reader) throws IOException, ClientException {
    if (reader == null) {
      return getResponse(CommandType.ReadArrayOneUnsigned,
              GNSProtocol.GUID.toString(), guid, GNSProtocol.FIELD.toString(), field);
    } else {
      return getResponse(CommandType.ReadArrayOne, reader,
              GNSProtocol.GUID.toString(), guid, GNSProtocol.FIELD.toString(), field,
              GNSProtocol.READER.toString(), reader.getGuid());
    }
  }

  private String getResponse(CommandType commandType, Object... keysAndValues) throws ClientException, IOException {
    return getResponse(commandType, null, keysAndValues);
  }

  private String getResponse(CommandType commandType, GuidEntry querier,
          Object... keysAndValues) throws ClientException, IOException {
    String command;
    if (querier != null) {
      command = createAndSignQuery(commandType, querier, keysAndValues);
    } else {
      command = createQuery(commandType, keysAndValues);
    }
    LOGGER.log(Level.FINE, "sending: " + command);
    String response = sendGetCommand(command);
    LOGGER.log(Level.FINE, "getResponse for " + commandType + " : " + response);
    return checkResponse(command, response);
  }


  private String checkResponse(String command, String response) throws ClientException {
    return CommandUtils.checkResponseOldSchool(response);
  }


  private String createQuery(CommandType commandType, Object... keysAndValues) throws IOException {
    String key;
    String value;
    StringBuilder result = new StringBuilder(commandType.name() + QUERYPREFIX);

    for (int i = 0; i < keysAndValues.length; i += 2) {
      key = keysAndValues[i].toString();
      value = keysAndValues[i + 1].toString();
      result.append(URIEncoderDecoder.quoteIllegal(key))
              .append(VALSEP).append(URIEncoderDecoder.quoteIllegal(value))
              .append(i + 2 < keysAndValues.length ? KEYSEP : "");
    }
    return result.toString();
  }


  // This code is similar to what is in CommandUtils.createAndSignCommand but has a little
  // more hair because of need for URLs and also JSON. Maybe just send JSON?
  // The big difference is that it creates a URI String to send to the server, but it also
  // creates a canonical JSON form that it needs for the signature.
  private String createAndSignQuery(CommandType commandType, GuidEntry guid, Object... keysAndValues)
          throws ClientException {
    // First we create the URI string
    String key;
    String value;
    StringBuilder encodedString = new StringBuilder(commandType.name() + QUERYPREFIX);
    try {
      // map over the leys and values to produce the query
      for (int i = 0; i < keysAndValues.length; i += 2) {
        key = keysAndValues[i].toString();
        value = keysAndValues[i + 1].toString();
        encodedString.append(URIEncoderDecoder.quoteIllegal(key))
                .append(VALSEP).append(URIEncoderDecoder.quoteIllegal(value))
                .append(i + 2 < keysAndValues.length ? KEYSEP : "");
      }
      // Now we create the JSON version that we can use to sign the command with
      // Do this first so we can pull out the timestamp and nonce to use in the URI
      JSONObject jsonVersionOfCommand = CryptoUtils.createCommandWithTimestampAndNonce(
              commandType, includeTimestamp, keysAndValues);

      // Also add the Timestamp and Nonce to the URI
      if (includeTimestamp) {
        encodedString.append(KEYSEP)
                .append(GNSProtocol.TIMESTAMP.toString())
                .append(VALSEP)
                .append(URIEncoderDecoder.quoteIllegal(jsonVersionOfCommand.getString(GNSProtocol.TIMESTAMP.toString())));
      }
      encodedString.append(KEYSEP)
              .append(GNSProtocol.NONCE.toString())
              .append(VALSEP)
              .append(URIEncoderDecoder.quoteIllegal(jsonVersionOfCommand.getString(GNSProtocol.NONCE.toString())));

      // Signature handling part
      // And make a canonical version of the JSON
      String canonicalJSON = CanonicalJSON.getCanonicalForm(jsonVersionOfCommand);
      LOGGER.log(Level.FINE, "Canonical JSON: {0}", canonicalJSON);

      // Now grab the keypair for signing the canonicalJSON string
      KeyPair keypair;
      keypair = new KeyPair(guid.getPublicKey(), guid.getPrivateKey());

      PrivateKey privateKey = keypair.getPrivate();
      PublicKey publicKey = keypair.getPublic();
      String signatureString;
      if (Config.getGlobalBoolean(GNSClientConfig.GNSCC.ENABLE_SECRET_KEY)) {
        signatureString = CryptoUtils.signDigestOfMessage(privateKey, publicKey, canonicalJSON);
      } else {
        signatureString = CryptoUtils.signDigestOfMessage(privateKey, canonicalJSON);
      }
      String signaturePart = KEYSEP + GNSProtocol.SIGNATURE.toString()
              // Base64 encode the signature first since it's guaranteed to be a lot of non-ASCII characters
              // and this will limit the percent encoding to just /,+,= in the base64 string.
              + VALSEP + 
              URIEncoderDecoder.quoteIllegal(
                      Base64.encodeToString(
                              signatureString.getBytes(GNSProtocol.CHARSET.toString()), false));
      // This is a debugging aid so we can auto check the message part on the other side. 
      String debuggingPart = "";
      // Currently not being used.
      if (false) {
        debuggingPart = KEYSEP + "originalMessageBase64" + VALSEP
                + URIEncoderDecoder.quoteIllegal(
                        Base64.encodeToString(canonicalJSON.getBytes(GNSProtocol.CHARSET.toString()), false));
      }
      // Finally return everything
      return encodedString.toString() + signaturePart + debuggingPart;
    } catch (JSONException | UnsupportedEncodingException | NoSuchAlgorithmException | InvalidKeyException | SignatureException | IllegalBlockSizeException |
            BadPaddingException | NoSuchPaddingException e) {
      throw new ClientException("Error encoding message", e);
    }
  }

  // /////////////////////////////////////////
  // // PLATFORM DEPENDENT METHODS BELOW /////
  // /////////////////////////////////////////

  public void checkConnectivity() throws IOException {
    if (IS_ANDROID) {
      String urlString = "http://" + host + ":" + port + "/";
      final AndroidHttpGet httpGet = new AndroidHttpGet();
      httpGet.execute(urlString);
      try {
        Object httpGetResponse = httpGet.get();
        if (httpGetResponse instanceof IOException) {
          throw (IOException) httpGetResponse;
        }
      } catch (InterruptedException | ExecutionException | IOException e) {
        throw new IOException(e);
      }
    } else // Desktop version
    {
      sendGetCommand(null);
    }
  }


  private String sendGetCommand(String queryString) throws IOException {
    if (IS_ANDROID) {
      return androidSendGetCommand(queryString);
    } else {
      return desktopSendGetCommmand(queryString);
    }
  }


  private String desktopSendGetCommmand(String queryString) throws IOException {
    HttpURLConnection connection = null;
    try {

      String urlString = "http://" + host + ":" + port;
      if (queryString != null) {
        urlString += "/GNS/" + queryString;
      }
      GNSClientConfig.getLogger().log(Level.FINE, "Sending: {0}", urlString);
      URL serverURL = new URL(urlString);
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
          response = inputStream.readLine(); // we only expect one line to be sent
          break;
        } catch (java.net.SocketTimeoutException e) {
          GNSClientConfig.getLogger().log(Level.INFO,
                  "Get Response timed out. Trying {0} more times. Query is {1}", new Object[]{cnt, queryString});
        }
      } while (cnt-- > 0);
      try {
        // in theory this close should allow the keepalive mechanism to operate correctly
        // http://docs.oracle.com/javase/6/docs/technotes/guides/net/http-keepalive.html
        inputStream.close();
      } catch (IOException e) {
        GNSClientConfig.getLogger().warning("Problem closing the HttpURLConnection's stream.");
      }
      GNSClientConfig.getLogger().log(Level.FINE, "Received: {0}", response);
      if (response != null) {
        return response;
      } else {
        throw new IOException("No response to command: " + queryString);
      }
    } finally {
      // close the connection, set all objects to null
      if (connection != null) {
        connection.disconnect();
      }
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
    } catch (InterruptedException | ExecutionException | IOException e) {
      throw new IOException(e);
    }
  }


  public void close() {
    // nothing to stop
  }

  private class AndroidHttpGet extends DownloadTask {


    public AndroidHttpGet() {
      super();
    }

    // onPostExecute displays the results of the AsyncTask.
    @Override
    protected void onPostExecute(Object result) {
    }

  }

}
