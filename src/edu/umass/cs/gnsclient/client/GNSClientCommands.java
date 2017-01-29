
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


public class GNSClientCommands extends GNSClient {


  public GNSClientCommands() throws IOException {
    super((InetSocketAddress) null);
  }


  public GNSClientCommands(InetSocketAddress anyReconfigurator)
          throws IOException {
    super(anyReconfigurator);
  }

  // READ AND WRITE COMMANDS

  public void update(String targetGuid, JSONObject json, GuidEntry writer)
          throws IOException, ClientException {
    execute(GNSCommand.update(targetGuid, json, writer));
  }


  public void update(GuidEntry guid, JSONObject json) throws IOException,
          ClientException {
    update(guid.getGuid(), json, guid);
  }


  public void fieldUpdate(String targetGuid, String field, Object value,
          GuidEntry writer) throws IOException, ClientException {
    execute(GNSCommand.fieldUpdate(targetGuid, field, value, writer));
  }


  public void fieldUpdate(GuidEntry targetGuid, String field, Object value)
          throws IOException, ClientException {
    fieldUpdate(targetGuid.getGuid(), field, value, targetGuid);
  }


  public void fieldCreateIndex(GuidEntry guid, String field, String index)
          throws IOException, ClientException {
    execute(GNSCommand.fieldCreateIndex(guid, field, index));
  }


  public JSONObject read(String targetGuid, GuidEntry reader)
          throws ClientException, IOException {
    return execute(GNSCommand.read(targetGuid, reader)).getResultJSONObject();
  }


  public JSONObject readSecure(String targetGuid)
          throws ClientException, IOException {
    return execute(GNSCommand.readSecure(targetGuid)).getResultJSONObject();
  }


  public JSONObject read(GuidEntry guid) throws ClientException, IOException {
    return read(guid.getGuid(), guid);
  }


  public boolean fieldExists(String targetGuid, String field, GuidEntry reader)
          throws ClientException, IOException {
    try {
      execute(GNSCommand.fieldExists(targetGuid, field, reader));
      return true;
    } catch (ClientException | IOException e) {
      return false;
    }
  }


  public boolean fieldExists(GuidEntry targetGuid, String field)
          throws ClientException, IOException {
    return fieldExists(targetGuid.getGuid(), field, targetGuid);
  }


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
  }


  public String fieldRead(GuidEntry targetGuid, String field)
          throws ClientException, IOException {
    return fieldRead(targetGuid.getGuid(), field, targetGuid);
  }


  public String fieldRead(String targetGuid, ArrayList<String> fields,
          GuidEntry reader) throws ClientException, IOException {
    return execute(GNSCommand.fieldRead(targetGuid, fields, reader)).getResultString();
  }


  public String fieldRead(GuidEntry targetGuid, ArrayList<String> fields)
          throws ClientException, IOException {
    return fieldRead(targetGuid.getGuid(), fields, targetGuid);
  }


  public void fieldRemove(String targetGuid, String field, GuidEntry writer)
          throws IOException, ClientException {
    execute(GNSCommand.fieldRemove(targetGuid, field, writer));
  }

  // SELECT COMMANDS

  public JSONArray selectQuery(String query) throws ClientException, IOException {
    return execute(GNSCommand.selectQuery(query)).getResultJSONArray();
  }


  public JSONArray selectSetupGroupQuery(GuidEntry accountGuid,
          String publicKey, String query, int interval) throws ClientException, IOException {
    return execute(GNSCommand.selectSetupGroupQuery(accountGuid, publicKey, query, interval)).getResultJSONArray();
  }


  public JSONArray selectLookupGroupQuery(String guid) throws ClientException, IOException {
    return execute(GNSCommand.selectLookupGroupQuery(guid)).getResultJSONArray();
  }

  // ACCOUNT COMMANDS

  public String lookupGuid(String alias) throws IOException, ClientException {
    return execute(GNSCommand.lookupGUID(alias)).getResultString();
  }


  public String lookupPrimaryGuid(String guid)
          throws IOException, ClientException {
    return execute(GNSCommand.lookupPrimaryGUID(guid)).getResultString();
  }


  public JSONObject lookupGuidRecord(String guid) throws IOException,
          ClientException {
    return execute(GNSCommand.lookupGUIDRecord(guid)).getResultJSONObject();
  }


  public JSONObject lookupAccountRecord(String accountGuid)
          throws IOException, ClientException {
    return execute(GNSCommand.lookupAccountRecord(accountGuid)).getResultJSONObject();
  }


  public PublicKey publicKeyLookupFromAlias(String alias)
          throws ClientException, IOException {
    return publicKeyLookupFromGuid(lookupGuid(alias));
  }


  // Note: publicKeyLookupFromGUID is implemented incorrectly in GNSCommand
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


  public GuidEntry accountGuidCreate(String alias, String password) throws ClientException, IOException {
    try {
      execute(GNSCommand.createAccount(alias, password));
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
  }


  public GuidEntry accountGuidCreateSecure(String alias, String password) throws ClientException, IOException {
    try {
      execute(GNSCommand.createAccountSecure(alias, password));
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
  }


  public String accountGuidVerify(GuidEntry guid, String code) throws ClientException, IOException {
    return execute(GNSCommand.accountGuidVerify(guid, code)).getResultString();
  }


  public String accountResendAuthenticationEmail(GuidEntry guid) throws ClientException, IOException {
    return execute(GNSCommand.accountResendAuthenticationEmail(guid)).getResultString();
  }


  public void accountGuidRemove(GuidEntry guid) throws ClientException, IOException {
    execute(GNSCommand.accountGuidRemove(guid)).getResultString();
  }


  public void accountGuidRemoveSecure(String name)
          throws ClientException, IOException {
    execute(GNSCommand.accountGuidRemoveSecure(name));
  }


  public void accountGuidRemoveWithPassword(String name, String password)
          throws ClientException, IOException {
    execute(GNSCommand.accountGuidRemoveWithPassword(name, password));
  }


  public GuidEntry guidCreate(GuidEntry accountGuid, String alias)
          throws ClientException, IOException {

    execute(GNSCommand.createGUID(accountGuid, alias));
    GuidEntry guidEntry = GuidUtils.lookupGuidEntryFromDatabase(this, alias);
    // If something went wrong an exception should be thrown above, but we're checking
    // here anyway just to be safe.
    if (guidEntry == null) {
      throw new ClientException("Failed to create guid for " + alias);
    }
    return guidEntry;
  }


  public void guidBatchCreate(GuidEntry accountGuid, Set<String> aliases)
          throws ClientException, IOException {
    execute(GNSCommand.batchCreateGUIDs(accountGuid, aliases));
  }


  public void guidBatchCreate(GuidEntry accountGuid, Set<String> aliases, long timeout)
          throws ClientException, IOException {
    execute(GNSCommand.batchCreateGUIDs(accountGuid, aliases), timeout);
  }


  public void guidRemove(GuidEntry guid) throws ClientException, IOException {
    execute(GNSCommand.removeGUID(guid));
  }


  public void guidRemove(GuidEntry accountGuid, String guidToRemove)
          throws ClientException, IOException {
    execute(GNSCommand.removeGUID(accountGuid, guidToRemove));
  }

  // GROUP COMMANDS

  public JSONArray groupGetMembers(String groupGuid, GuidEntry reader)
          throws ClientException, IOException {
    return execute(GNSCommand.groupGetMembers(groupGuid, reader)).getResultJSONArray();
  }


  public JSONArray guidGetGroups(String guid, GuidEntry reader)
          throws IOException, ClientException {
    return execute(GNSCommand.guidGetGroups(guid, reader)).getResultJSONArray();
  }


  public void groupAddGuid(String groupGuid, String guidToAdd,
          GuidEntry writer) throws IOException, ClientException {
    execute(GNSCommand.groupAddGuid(groupGuid, guidToAdd, writer));
  }


  public void groupAddGuids(String groupGuid, JSONArray members,
          GuidEntry writer) throws IOException, ClientException {
    execute(GNSCommand.groupAddGUIDs(groupGuid, members, writer));
  }


  public void groupRemoveGuid(String groupGuid, String guidToRemove,
          GuidEntry writer) throws ClientException, IOException {
    execute(GNSCommand.groupRemoveGuid(groupGuid, guidToRemove, writer));
  }


  public void groupRemoveGuids(String groupGuid, JSONArray members,
          GuidEntry writer) throws ClientException, IOException {
    execute(GNSCommand.groupRemoveGuids(groupGuid, members, writer));
  }


  public void groupAddMembershipUpdatePermission(GuidEntry groupGuid,
          String guidToAuthorize) throws ClientException, IOException {
    execute(GNSCommand.groupAddMembershipUpdatePermission(groupGuid, guidToAuthorize));
  }


  public void groupRemoveMembershipUpdatePermission(GuidEntry groupGuid,
          String guidToUnauthorize) throws ClientException, IOException {
    execute(GNSCommand.groupRemoveMembershipUpdatePermission(groupGuid, guidToUnauthorize));
  }


  public void groupAddMembershipReadPermission(GuidEntry groupGuid,
          String guidToAuthorize) throws ClientException, IOException {
    execute(GNSCommand.groupAddMembershipReadPermission(groupGuid, guidToAuthorize));
  }


  public void groupRemoveMembershipReadPermission(GuidEntry groupGuid,
          String guidToUnauthorize) throws ClientException, IOException {
    execute(GNSCommand.groupRemoveMembershipReadPermission(groupGuid, guidToUnauthorize));
  }

  // ACL COMMANDS

  public void aclAdd(AclAccessType accessType, GuidEntry targetGuid,
          String field, String accesserGuid) throws ClientException, IOException {
    execute(GNSCommand.aclAdd(accessType, targetGuid, field, accesserGuid));
  }


  public void aclRemove(AclAccessType accessType, GuidEntry targetGuid,
          String field, String accesserGuid) throws ClientException, IOException {
    execute(GNSCommand.aclRemove(accessType, targetGuid, field, accesserGuid));
  }


  public JSONArray aclGet(AclAccessType accessType, GuidEntry targetGuid,
          String field, String readerGuid) throws ClientException, IOException {
    return execute(GNSCommand.aclGet(accessType, targetGuid, field, readerGuid)).getResultJSONArray();
  }


  public void aclAddSecure(AclAccessType accessType, String targetGuid,
          String field, String accesserGuid) throws ClientException, IOException {
    execute(GNSCommand.aclAddSecure(accessType, targetGuid, field, accesserGuid));
  }


  public void aclRemoveSecure(AclAccessType accessType, String targetGuid,
          String field, String accesserGuid) throws ClientException, IOException {
    execute(GNSCommand.aclRemoveSecure(accessType, targetGuid, field, accesserGuid));
  }


  public JSONArray aclGetSecure(AclAccessType accessType, String targetGuid,
          String field) throws ClientException, IOException {
    return execute(GNSCommand.aclGetSecure(accessType, targetGuid, field)).getResultJSONArray();
  }


  public void fieldCreateAcl(AclAccessType accessType, GuidEntry guid, String field,
          String writerGuid) throws ClientException, IOException {
    execute(GNSCommand.fieldCreateAcl(accessType, guid, field, writerGuid));
  }


  public void fieldCreateAcl(AclAccessType accessType, GuidEntry guid, String field)
          throws ClientException, IOException {
    fieldCreateAcl(accessType, guid, field, guid.getGuid());
  }


  public void fieldDeleteAcl(AclAccessType accessType, GuidEntry guid, String field,
          String writerGuid)
          throws ClientException, IOException {
    execute(GNSCommand.fieldDeleteAcl(accessType, guid, field, writerGuid));
  }


  public void fieldDeleteAcl(AclAccessType accessType, GuidEntry guid, String field)
          throws ClientException, IOException {
    fieldDeleteAcl(accessType, guid, field, guid.getGuid());
  }


  public boolean fieldAclExists(AclAccessType accessType, GuidEntry guid, String field,
          String readerGuid) throws ClientException, IOException {
    return execute(GNSCommand.fieldAclExists(accessType, guid, field, readerGuid)).getResultBoolean();
  }


  public boolean fieldAclExists(AclAccessType accessType, GuidEntry guid, String field)
          throws ClientException, IOException {
    return fieldAclExists(accessType, guid, field, guid.getGuid());
  }

  // ALIASES

  public void addAlias(GuidEntry guid, String name) throws ClientException, IOException {
    execute(GNSCommand.addAlias(guid, name));
  }


  public void removeAlias(GuidEntry guid, String name) throws ClientException, IOException {
    execute(GNSCommand.removeAlias(guid, name));
  }


  public JSONArray getAliases(GuidEntry guid) throws ClientException, IOException {
    return execute(GNSCommand.getAliases(guid)).getResultJSONArray();
  }

  // Extended commands

  public void fieldCreateList(String targetGuid, String field,
          JSONArray value, GuidEntry writer) throws IOException, ClientException {
    execute(GNSCommand.fieldCreateList(targetGuid, field, value, writer));
  }


  public void fieldAppendOrCreateList(String targetGuid, String field,
          JSONArray value, GuidEntry writer) throws IOException, ClientException {
    execute(GNSCommand.fieldAppendOrCreateList(targetGuid, field, value, writer));
  }


  public void fieldReplaceOrCreateList(String targetGuid, String field,
          JSONArray value, GuidEntry writer) throws IOException, ClientException {
    execute(GNSCommand.fieldReplaceOrCreateList(targetGuid, field, value, writer));
  }


  public void fieldAppend(String targetGuid, String field, JSONArray value,
          GuidEntry writer) throws IOException, ClientException {
    execute(GNSCommand.fieldAppend(targetGuid, field, value, writer));
  }


  public void fieldReplaceList(String targetGuid, String field,
          JSONArray value, GuidEntry writer) throws IOException,
          ClientException {
    execute(GNSCommand.fieldReplaceList(targetGuid, field, value, writer));
  }


  public void fieldClear(String targetGuid, String field, JSONArray value,
          GuidEntry writer) throws IOException, ClientException {
    execute(GNSCommand.fieldClear(targetGuid, field, value, writer));
  }


  public void fieldClear(String targetGuid, String field, GuidEntry writer)
          throws IOException, ClientException {
    execute(GNSCommand.fieldClear(targetGuid, field, writer));
  }


  public JSONArray fieldReadArray(String guid, String field, GuidEntry reader)
          throws ClientException, IOException {
    String response = execute(GNSCommand.fieldReadArray(guid, field, reader)).getResultString();
    try {
      return CommandUtils.commandResponseToJSONArray(field, response);
    } catch (JSONException e) {
      throw new ClientException(e);
    }
  }


  public void fieldSetElement(String targetGuid, String field,
          String newValue, int index, GuidEntry writer) throws IOException,
          ClientException {
    execute(GNSCommand.fieldSetElement(targetGuid, field, newValue, index, writer));
  }


  public void fieldSetNull(String targetGuid, String field, GuidEntry writer)
          throws IOException, ClientException {
    execute(GNSCommand.fieldSetNull(targetGuid, field, writer));
  }

  //
  // SELECT
  //

  public JSONArray select(String field, String value) throws ClientException, IOException {
    return execute(GNSCommand.select(field, value)).getResultJSONArray();
  }


  public JSONArray selectWithin(String field, JSONArray value)
          throws ClientException, IOException {
    return execute(GNSCommand.selectWithin(field, value)).getResultJSONArray();
  }


  public JSONArray selectNear(String field, JSONArray value,
          Double maxDistance) throws ClientException, IOException {
    return execute(GNSCommand.selectNear(field, value, maxDistance)).getResultJSONArray();
  }


  public void setLocation(String targetGuid, double longitude,
          double latitude, GuidEntry writer) throws ClientException, IOException {
    fieldReplaceOrCreateList(targetGuid, GNSProtocol.LOCATION_FIELD_NAME.toString(),
            new JSONArray(Arrays.asList(longitude, latitude)), writer);
  }


  public void setLocation(GuidEntry guid, double longitude, double latitude)
          throws ClientException, IOException {
    setLocation(guid.getGuid(), longitude, latitude, guid);
  }


  public JSONArray getLocation(String targetGuid, GuidEntry readerGuid)
          throws ClientException, IOException {
    try {
      JSONObject json = execute(GNSCommand.getLocation(targetGuid, readerGuid)).getResultJSONObject();
      return json.getJSONArray(GNSProtocol.LOCATION_FIELD_NAME.toString());
    } catch (JSONException e) {
      throw new ClientException(e);
    }
  }


  public JSONArray getLocation(GuidEntry guid) throws ClientException, IOException {
    return getLocation(guid.getGuid(), guid);
  }


  // Active Code
  public void activeCodeClear(String guid, String action, GuidEntry writerGuid)
          throws ClientException, IOException {
    execute(GNSCommand.activeCodeClear(guid, action, writerGuid));
  }


  public void activeCodeSet(String guid, String action, byte[] code,
          GuidEntry writerGuid) throws ClientException, IOException {
    // The GNSCommand method expects bytes which it Base64 encodes.
    execute(GNSCommand.activeCodeSet(guid, action, code, writerGuid));
  }


  public String activeCodeGet(String guid, String action, GuidEntry readerGuid)
          throws ClientException, IOException {
    return execute(GNSCommand.activeCodeGet(guid, action, readerGuid)).getResultString();
  }

  // Extended commands

  public void fieldCreateList(GuidEntry target, String field, JSONArray value)
          throws IOException, ClientException {
    execute(GNSCommand.fieldCreateList(field, field, value, target));
  }


  public void fieldCreateOneElementList(String targetGuid, String field,
          String value, GuidEntry writer) throws IOException, ClientException {
    execute(GNSCommand.fieldCreateOneElementList(targetGuid, field, value, writer));
  }


  public void fieldCreateOneElementList(GuidEntry target, String field,
          String value) throws IOException, ClientException {
    fieldCreateOneElementList(target.getGuid(), field, value, target);
  }


  public void fieldAppendOrCreate(String targetGuid, String field,
          String value, GuidEntry writer) throws IOException, ClientException {
    execute(GNSCommand.fieldAppendOrCreate(targetGuid, field, value, writer));
  }


  public void fieldReplaceOrCreate(String targetGuid, String field,
          String value, GuidEntry writer) throws IOException, ClientException {
    execute(GNSCommand.fieldReplaceOrCreate(targetGuid, field, value, writer));
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
    execute(GNSCommand.fieldReplace(writer, field, value));
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
    execute(GNSCommand.fieldAppend(targetGuid, field, value, writer));
  }


  public void fieldAppend(GuidEntry target, String field, String value)
          throws IOException, ClientException {
    fieldAppend(target.getGuid(), field, value, target);
  }


  public void fieldAppendWithSetSemantics(String targetGuid, String field,
          JSONArray value, GuidEntry writer) throws IOException,
          ClientException {
    execute(GNSCommand.fieldAppendWithSetSemantics(targetGuid, field, value, writer));
  }


  public void fieldAppendWithSetSemantics(GuidEntry target, String field,
          JSONArray value) throws IOException, ClientException {
    fieldAppendWithSetSemantics(target.getGuid(), field, value, target);
  }


  public void fieldAppendWithSetSemantics(String targetGuid, String field,
          String value, GuidEntry writer) throws IOException, ClientException {
    execute(GNSCommand.fieldAppendWithSetSemantics(targetGuid, field, value, writer));
  }


  public void fieldAppendWithSetSemantics(GuidEntry target, String field,
          String value) throws IOException, ClientException {
    fieldAppendWithSetSemantics(target.getGuid(), field, value, target);
  }


  @Deprecated
  public void fieldReplaceFirstElement(String targetGuid, String field,
          String value, GuidEntry writer) throws IOException, ClientException {
    execute(GNSCommand.fieldReplaceFirstElement(targetGuid, field, value, writer));
  }


  @Deprecated
  public void fieldReplaceFirstElement(GuidEntry target, String field,
          String value) throws IOException, ClientException {
    fieldReplaceFirstElement(target.getGuid(), field, value, target);
  }


  public void fieldSubstitute(String targetGuid, String field,
          String newValue, String oldValue, GuidEntry writer)
          throws IOException, ClientException {
    execute(GNSCommand.fieldSubstitute(writer, field, newValue, oldValue));
  }


  public void fieldSubstitute(GuidEntry target, String field,
          String newValue, String oldValue) throws IOException,
          ClientException {
    fieldSubstitute(target.getGuid(), field, newValue, oldValue, target);
  }


  public void fieldSubstitute(String targetGuid, String field,
          JSONArray newValue, JSONArray oldValue, GuidEntry writer)
          throws IOException, ClientException {
    execute(GNSCommand.fieldSubstitute(writer, field, newValue, oldValue));
  }


  public void fieldSubstitute(GuidEntry target, String field,
          JSONArray newValue, JSONArray oldValue) throws IOException,
          ClientException {
    fieldSubstitute(target.getGuid(), field, newValue, oldValue, target);
  }


  //FIXME: This should probably be deprecated and removed.
  public String fieldReadArrayFirstElement(String guid, String field,
          GuidEntry reader) throws ClientException, IOException {
    return execute(GNSCommand.fieldReadArrayFirstElement(guid, field, reader)).getResultString();
  }


  //FIXME: This should probably be deprecated and removed.
  public String fieldReadArrayFirstElement(GuidEntry guid, String field)
          throws ClientException, IOException {
    return fieldReadArrayFirstElement(guid.getGuid(), field, guid);
  }


  public void fieldRemove(GuidEntry guid, String field)
          throws IOException, ClientException {
    execute(GNSCommand.fieldRemove(field, field, guid));
  }


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
