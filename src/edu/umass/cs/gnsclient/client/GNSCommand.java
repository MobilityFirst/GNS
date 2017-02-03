
package edu.umass.cs.gnsclient.client;

import edu.umass.cs.gnsclient.client.util.GUIDUtilsGNSClient;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.IOSKeyPairUtils;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.client.util.Password;
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.SharedGuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.client.EncryptionException;
import edu.umass.cs.gnscommon.exceptions.client.InvalidGuidException;
import edu.umass.cs.gnscommon.packets.AdminCommandPacket;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.utils.Base64;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;



public class GNSCommand extends CommandPacket {



  protected GNSCommand(JSONObject command) {
    this(

            randomLong(), command);
  }


  protected GNSCommand(long id, JSONObject command) {
    super(id, command);
  }


  public static CommandPacket getCommand(CommandType type, GuidEntry querier,
          Object... keysAndValues) throws ClientException {
    JSONObject command = CryptoUtils.createAndSignCommand(type, querier,
            keysAndValues);
    if (CommandPacket.getJSONCommandType(command).isMutualAuth()) {
      return new AdminCommandPacket(randomLong(), command);
    }
    return new GNSCommand(command);
  }


  public static CommandPacket getCommand(CommandType type,
          Object... keysAndValues) throws ClientException {
    return getCommand(type, null, keysAndValues);
  }


  private static long randomLong() {
    return (long) (Math.random() * Long.MAX_VALUE);
  }



  public static final CommandPacket update(String targetGUID,
          JSONObject json, GuidEntry querierGUID) throws ClientException {
    return getCommand(CommandType.ReplaceUserJSON, querierGUID, GNSProtocol.GUID.toString(),
            targetGUID, GNSProtocol.USER_JSON.toString(), json.toString(), GNSProtocol.WRITER.toString(),
            querierGUID.getGuid());
  }


  public static final CommandPacket update(GuidEntry targetGUID,
          JSONObject json) throws ClientException {
    return update(targetGUID.getGuid(), json, targetGUID);
  }


  public static final CommandPacket fieldUpdate(String targetGuid,
          String field, Object value, GuidEntry querierGUID)
          throws ClientException {
    return getCommand(CommandType.ReplaceUserJSON, querierGUID, GNSProtocol.GUID.toString(),
            targetGuid, GNSProtocol.USER_JSON.toString(), getJSONObject(field, value).toString(),
            GNSProtocol.WRITER.toString(), querierGUID.getGuid());
  }

  // converts JSONException to ClientException

  protected static JSONObject makeJSON(String field, Object value)
          throws JSONException {
    return new JSONObject().put(field, value);
  }

  // converts JSONException to ClientException

  protected static JSONObject getJSONObject(String field, Object value)
          throws ClientException {
    try {
      return makeJSON(field, value);
    } catch (JSONException e) {
      throw new ClientException(e);
    }
  }


  protected static final CommandPacket fieldCreateIndex(GuidEntry GUID,
          String field, String index) throws ClientException {
    return getCommand(CommandType.CreateIndex, GUID,
            GNSProtocol.GUID.toString(), GUID.getGuid(),
            GNSProtocol.FIELD.toString(), field,
            GNSProtocol.VALUE.toString(), index,
            GNSProtocol.WRITER.toString(), GUID.getGuid());
  }


  public static final CommandPacket fieldUpdate(GuidEntry targetGUID,
          String field, Object value) throws ClientException {
    return fieldUpdate(targetGUID.getGuid(), field, value, targetGUID);
  }


  public static final CommandPacket read(String targetGUID,
          GuidEntry querierGUID) throws ClientException {
    return getCommand(querierGUID != null ? CommandType.ReadArray
            : CommandType.ReadArrayUnsigned, querierGUID, GNSProtocol.GUID.toString(), targetGUID,
            GNSProtocol.FIELD.toString(), GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.READER.toString(),
            querierGUID != null ? querierGUID.getGuid() : null);
  }
  

  public static final CommandPacket readSecure(String targetGUID) throws ClientException {
    return getCommand(CommandType.ReadSecured, 
            GNSProtocol.GUID.toString(), targetGUID,
            GNSProtocol.FIELD.toString(), GNSProtocol.ENTIRE_RECORD.toString());
  }


  public static final CommandPacket read(GuidEntry targetGUID)
          throws ClientException {
    return read(targetGUID.getGuid(), targetGUID);
  }


  public static final CommandPacket fieldExists(String targetGUID,
          String field, GuidEntry querierGUID) throws ClientException {
    return getCommand(querierGUID != null ? CommandType.Read
            : CommandType.ReadUnsigned, querierGUID, GNSProtocol.GUID.toString(), targetGUID,
            GNSProtocol.FIELD.toString(), field, GNSProtocol.READER.toString(),
            querierGUID != null ? querierGUID.getGuid() : null);
  }


  public static final CommandPacket fieldExists(GuidEntry targetGUID,
          String field) throws ClientException {
    return fieldExists(targetGUID.getGuid(), field, targetGUID);
  }


  public static final CommandPacket fieldRead(String targetGUID,
          String field, GuidEntry querierGUID) throws ClientException {
    return getCommand(querierGUID != null ? CommandType.Read
            : CommandType.ReadUnsigned, querierGUID, GNSProtocol.GUID.toString(), targetGUID,
            GNSProtocol.FIELD.toString(), field, GNSProtocol.READER.toString(),
            querierGUID != null ? querierGUID.getGuid() : null);
  }


  public static final CommandPacket fieldRead(GuidEntry targetGUID,
          String field) throws ClientException {
    return fieldRead(targetGUID.getGuid(), field, targetGUID);
  }


  public static final CommandPacket fieldRead(String targetGUID,
          ArrayList<String> fields, GuidEntry querierGUID)
          throws ClientException {
    return getCommand(querierGUID != null ? CommandType.ReadMultiField
            : CommandType.ReadMultiFieldUnsigned, querierGUID, GNSProtocol.GUID.toString(),
            targetGUID, GNSProtocol.FIELDS.toString(), fields, GNSProtocol.READER.toString(),
            querierGUID != null ? querierGUID.getGuid() : null);
  }


  public static final CommandPacket fieldRead(GuidEntry targetGUID,
          ArrayList<String> fields) throws ClientException {
    return fieldRead(targetGUID.getGuid(), fields, targetGUID);
  }


  public static final CommandPacket fieldRemove(String targetGUID,
          String field, GuidEntry querierGUID) throws ClientException {
    return getCommand(CommandType.RemoveField, querierGUID, GNSProtocol.GUID.toString(),
            targetGUID, GNSProtocol.FIELD.toString(), field, GNSProtocol.WRITER.toString(), querierGUID.getGuid());
  }

  // ACCOUNT COMMANDS

  public static final CommandPacket lookupGUID(String alias)
          throws ClientException {

    return getCommand(CommandType.LookupGuid, GNSProtocol.NAME.toString(), alias);
  }


  public static final CommandPacket lookupPrimaryGUID(String subGUID)
          throws ClientException {
    return getCommand(CommandType.LookupPrimaryGuid, GNSProtocol.GUID.toString(), subGUID);
  }


  public static final CommandPacket lookupGUIDRecord(String targetGUID)
          throws ClientException {
    return (getCommand(CommandType.LookupGuidRecord, GNSProtocol.GUID.toString(), targetGUID));
  }


  public static final CommandPacket lookupAccountRecord(String accountGUID)
          throws ClientException {
    return getCommand(CommandType.LookupAccountRecord, GNSProtocol.GUID.toString(), accountGUID);
  }


  PublicKey publicKeyLookupFromAlias(String alias)
          throws InvalidGuidException, ClientException, IOException {
    throw new RuntimeException("Unimplementable");
    // String guid = lookupGuid(alias);
    // return publicKeyLookupFromGuid(guid);
  }


  public static final CommandPacket publicKeyLookupFromGUID(String targetGUID)
          throws ClientException {
    // FIXME: This is not correctly implemented
    return lookupGUIDRecord(targetGUID);
  }


  //FIXME: The name this of these violates the NOUNVERB naming convention adopted
  // almost everywhere else in here.
  public static final CommandPacket createAccount(
          String alias, String password) throws ClientException, IOException, NoSuchAlgorithmException {
    @SuppressWarnings("deprecation") // FIXME
	GuidEntry guidEntry = lookupOrCreateGuidEntry(GNSClient.getGNSProvider(), alias);
    return accountGuidCreateInternal(alias, password, CommandType.RegisterAccount, guidEntry);
  }


        //FIXME: The name this of these violates the NOUNVERB naming convention adopted
        // almost everywhere else in here.
	public static final CommandPacket createAccount(
			String alias) throws ClientException, IOException,
			NoSuchAlgorithmException {
		@SuppressWarnings("deprecation") // FIXME
		GuidEntry guidEntry = lookupOrCreateGuidEntry(GNSClient.getGNSProvider(), alias);
		return accountGuidCreateInternal(alias, null,
				CommandType.RegisterAccount, guidEntry);
	}


  public static final CommandPacket createAccountSecure(
          String alias, String password) throws ClientException, IOException, NoSuchAlgorithmException {
    @SuppressWarnings("deprecation")
	GuidEntry guidEntry = lookupOrCreateGuidEntry(GNSClient.getGNSProvider(), alias);
    return accountGuidCreateInternal(alias, password, CommandType.RegisterAccountSecured, guidEntry);
  }


  public static final CommandPacket accountGuidVerify(GuidEntry accountGUID,
          String code) throws ClientException {
    return getCommand(CommandType.VerifyAccount, accountGUID,
            GNSProtocol.GUID.toString(),
            accountGUID.getGuid(),
            GNSProtocol.CODE.toString(), code);
  }


  public static final CommandPacket accountResendAuthenticationEmail(GuidEntry accountGUID)
          throws ClientException {
    return getCommand(CommandType.ResendAuthenticationEmail, accountGUID,
            GNSProtocol.GUID.toString(),
            accountGUID.getGuid());
  }


  public static final CommandPacket accountGuidRemove(GuidEntry accountGUID)
          throws ClientException {
    return getCommand(CommandType.RemoveAccount, accountGUID, GNSProtocol.GUID.toString(),
            accountGUID.getGuid(), GNSProtocol.NAME.toString(), accountGUID.getEntityName());
  }


  public static final CommandPacket accountGuidRemoveSecure(String name)
          throws ClientException {
    return getCommand(CommandType.RemoveAccountSecured, null,
            GNSProtocol.NAME.toString(), name);
  }


  public static final CommandPacket accountGuidRemoveWithPassword(String name, String password)
          throws ClientException {
    String encodedPassword;
    try {
      encodedPassword = Password.encryptAndEncodePassword(password, name);
    } catch (NoSuchAlgorithmException e) {
      throw new ClientException(e);
    }
    return getCommand(CommandType.RemoveAccountWithPassword, GNSProtocol.NAME.toString(),
            name, GNSProtocol.PASSWORD.toString(), encodedPassword);
  }


  @SuppressWarnings("deprecation") // FIXME:
//FIXME: The name this of these violates the NOUNVERB naming convention adopted
// almost everywhere else in here.
public static final CommandPacket createGUID(
          GuidEntry accountGUID, String alias) throws ClientException {
    try {
      return guidCreateHelper(accountGUID, alias, GUIDUtilsGNSClient
              .createAndSaveGuidEntry(alias, GNSClient.getGNSProvider()).getPublicKey());
    } catch (NoSuchAlgorithmException e) {
      throw new ClientException(e);
    }
  }


  @SuppressWarnings("deprecation") // FIXME
public static final CommandPacket batchCreateGUIDs(
          GuidEntry accountGUID, Set<String> aliases) throws ClientException {

    List<String> aliasList = new ArrayList<>(aliases);
    List<String> publicKeys;
    publicKeys = new ArrayList<>();
    for (String alias : aliasList) {
      GuidEntry entry;
      try {
        entry = GUIDUtilsGNSClient.createAndSaveGuidEntry(alias, GNSClient.getGNSProvider());
      } catch (NoSuchAlgorithmException e) {
        // FIXME: Do we need to roll back created keys?
        throw new ClientException(e);
      }
      byte[] publicKeyBytes = entry.getPublicKey().getEncoded();
      String publicKeyString = Base64.encodeToString(publicKeyBytes,
              false);
      publicKeys.add(publicKeyString);
    }

    return getCommand(CommandType.AddMultipleGuids, accountGUID, GNSProtocol.GUID.toString(),
            accountGUID.getGuid(), GNSProtocol.NAMES.toString(), new JSONArray(aliasList),
            GNSProtocol.PUBLIC_KEYS.toString(), new JSONArray(publicKeys));
  }


  public static final CommandPacket removeGUID(GuidEntry targetGUID)
          throws ClientException {
    return getCommand(CommandType.RemoveGuidNoAccount, targetGUID,
            GNSProtocol.GUID.toString(), targetGUID.getGuid());
  }


  //FIXME: The name this of these violates the NOUNVERB naming convention adopted
  // almost everywhere else in here.
  public static final CommandPacket removeGUID(GuidEntry accountGUID,
          String targetGUID) throws ClientException {
    return getCommand(CommandType.RemoveGuid, accountGUID, 
            GNSProtocol.ACCOUNT_GUID.toString(), accountGUID.getGuid(), 
            GNSProtocol.GUID.toString(), targetGUID);
  }

  // GROUP COMMANDS

  public static final CommandPacket groupGetMembers(String groupGuid,
          GuidEntry querierGUID) throws ClientException {
    return getCommand(CommandType.GetGroupMembers, querierGUID, GNSProtocol.GUID.toString(),
            groupGuid, GNSProtocol.READER.toString(), querierGUID.getGuid());
  }


  public static final CommandPacket guidGetGroups(String targetGUID,
          GuidEntry querierGUID) throws ClientException {

    return getCommand(CommandType.GetGroups, querierGUID, GNSProtocol.GUID.toString(), targetGUID,
            GNSProtocol.READER.toString(), querierGUID.getGuid());
  }


  public static final CommandPacket groupAddGuid(String groupGUID,
          String toAddGUID, GuidEntry querierGUID) throws ClientException {
    return getCommand(CommandType.AddToGroup, querierGUID, GNSProtocol.GUID.toString(), groupGUID,
            GNSProtocol.MEMBER.toString(), toAddGUID,
            GNSProtocol.WRITER.toString(), querierGUID.getGuid());
  }


  public static final CommandPacket groupAddGUIDs(String groupGUID,
          JSONArray members, GuidEntry querierGUID) throws ClientException {
    return getCommand(CommandType.AddMembersToGroup, querierGUID, GNSProtocol.GUID.toString(),
            groupGUID, GNSProtocol.MEMBERS.toString(), members.toString(),
            GNSProtocol.WRITER.toString(), querierGUID.getGuid());
  }


  public static final CommandPacket groupAddGUIDs(String groupGUID,
          Set<String> members, GuidEntry querierGUID) throws ClientException {
    return groupAddGUIDs(groupGUID, new JSONArray(members), querierGUID);
  }


  public static final CommandPacket groupRemoveGuid(String groupGUID,
          String toRemoveGUID, GuidEntry querierGUID) throws ClientException {
    return getCommand(CommandType.RemoveFromGroup, querierGUID, GNSProtocol.GUID.toString(),
            groupGUID, GNSProtocol.MEMBER.toString(), toRemoveGUID, GNSProtocol.WRITER.toString(), querierGUID.getGuid());
  }


  public static final CommandPacket groupRemoveGuids(String groupGUID,
          JSONArray members, GuidEntry querierGUID) throws ClientException {
    return getCommand(CommandType.RemoveMembersFromGroup, querierGUID,
            GNSProtocol.GUID.toString(), groupGUID, GNSProtocol.MEMBERS.toString(), members.toString(), GNSProtocol.WRITER.toString(),
            querierGUID.getGuid());
  }


  public static final CommandPacket groupAddMembershipUpdatePermission(
          GuidEntry groupGUID, String toAuthorizeGUID) throws ClientException {
    return aclAdd(AclAccessType.WRITE_WHITELIST, groupGUID, GNSProtocol.GROUP_ACL.toString(),
            toAuthorizeGUID);
  }


  public static final CommandPacket groupRemoveMembershipUpdatePermission(
          GuidEntry groupGuid, String guidToUnauthorize)
          throws ClientException {
    return aclRemove(AclAccessType.WRITE_WHITELIST, groupGuid, GNSProtocol.GROUP_ACL.toString(),
            guidToUnauthorize);
  }


  public static final CommandPacket groupAddMembershipReadPermission(
          GuidEntry groupGUID, String toAuthorizeGUID) throws ClientException {
    return aclAdd(AclAccessType.READ_WHITELIST, groupGUID, GNSProtocol.GROUP_ACL.toString(),
            toAuthorizeGUID);
  }


  public static final CommandPacket groupRemoveMembershipReadPermission(
          GuidEntry groupGUID, String toUnauthorizeGUID)
          throws ClientException {
    return aclRemove(AclAccessType.READ_WHITELIST, groupGUID, GNSProtocol.GROUP_ACL.toString(),
            toUnauthorizeGUID);
  }

  //
  // ACL COMMANDS
  //

  public static final CommandPacket aclAdd(AclAccessType accessType,
          GuidEntry targetGUID, String field, String accesserGUID)
          throws ClientException {
    return aclAdd(accessType.name(), targetGUID, field, accesserGUID);
  }


  public static final CommandPacket aclAddSecure(AclAccessType accessType,
          String guid, String field, String accesserGUID)
          throws ClientException {
    return getCommand(CommandType.AclAddSecured, null,
            GNSProtocol.ACL_TYPE.toString(), accessType.name(),
            GNSProtocol.GUID.toString(), guid,
            GNSProtocol.FIELD.toString(), field,
            GNSProtocol.ACCESSER.toString(), accesserGUID);
  }


  public static final CommandPacket aclRemove(AclAccessType accessType,
          GuidEntry targetGUID, String field, String accesserGUID)
          throws ClientException {
    return aclRemove(accessType.name(), targetGUID, field, accesserGUID);
  }


  public static final CommandPacket aclRemoveSecure(AclAccessType accessType,
          String guid, String field, String accesserGUID)
          throws ClientException {
    return getCommand(CommandType.AclRemoveSecured, null,
            GNSProtocol.ACL_TYPE.toString(), accessType.name(),
            GNSProtocol.GUID.toString(), guid,
            GNSProtocol.FIELD.toString(), field,
            GNSProtocol.ACCESSER.toString(), accesserGUID);
  }


  public static final CommandPacket aclGet(AclAccessType accessType,
          GuidEntry targetGUID, String field, String querierGUID)
          throws ClientException {
    return aclGet(accessType.name(), targetGUID, field, querierGUID);
  }


  public static final CommandPacket aclGetSecure(AclAccessType accessType,
          String guid, String field)
          throws ClientException {
    return getCommand(CommandType.AclRetrieveSecured, null,
            GNSProtocol.ACL_TYPE.toString(), accessType.name(),
            GNSProtocol.GUID.toString(), guid,
            GNSProtocol.FIELD.toString(), field);
  }


  public static final CommandPacket fieldCreateAcl(AclAccessType accessType,
          GuidEntry guid, String field, String writerGuid)
          throws ClientException {
    return getCommand(CommandType.FieldCreateAcl, guid,
            GNSProtocol.ACL_TYPE.toString(), accessType.name(),
            GNSProtocol.GUID.toString(), guid.getGuid(),
            GNSProtocol.FIELD.toString(), field,
            GNSProtocol.WRITER.toString(), writerGuid);
  }


  public static final CommandPacket fieldDeleteAcl(AclAccessType accessType,
          GuidEntry guid, String field, String writerGuid)
          throws ClientException {
    return getCommand(CommandType.FieldDeleteAcl, guid,
            GNSProtocol.ACL_TYPE.toString(), accessType.name(),
            GNSProtocol.GUID.toString(), guid.getGuid(),
            GNSProtocol.FIELD.toString(), field,
            GNSProtocol.WRITER.toString(), writerGuid);
  }


  public static final CommandPacket fieldAclExists(AclAccessType accessType,
          GuidEntry guid, String field, String reader)
          throws ClientException {
    return getCommand(CommandType.FieldAclExists, guid,
            GNSProtocol.ACL_TYPE.toString(), accessType.name(),
            GNSProtocol.GUID.toString(), guid.getGuid(),
            GNSProtocol.FIELD.toString(), field,
            GNSProtocol.READER.toString(), reader);
  }



  public static final CommandPacket addAlias(GuidEntry targetGUID, String name)
          throws ClientException {
    return getCommand(CommandType.AddAlias, targetGUID, GNSProtocol.GUID.toString(),
            targetGUID.getGuid(), GNSProtocol.NAME.toString(), name);
  }


  public static final CommandPacket removeAlias(GuidEntry targetGUID,
          String name) throws ClientException {
    return getCommand(CommandType.RemoveAlias, targetGUID, GNSProtocol.GUID.toString(),
            targetGUID.getGuid(), GNSProtocol.NAME.toString(), name);
  }


  public static final CommandPacket getAliases(GuidEntry guid)
          throws ClientException {
    return getCommand(CommandType.RetrieveAliases, guid, GNSProtocol.GUID.toString(),
            guid.getGuid());
  }

  // ///////////////////////////////
  // // PRIVATE METHODS BELOW /////
  // /////////////////////////////
  private static GuidEntry lookupOrCreateGuidEntry(String gnsInstance,
          String alias) throws NoSuchAlgorithmException, EncryptionException {
    GuidEntry guidEntry = GUIDUtilsGNSClient.lookupGuidEntryFromDatabase(gnsInstance, alias);

    if (guidEntry == null) {
      KeyPair keyPair = KeyPairGenerator.getInstance(GNSProtocol.RSA_ALGORITHM.toString())
              .generateKeyPair();
      String guid = SharedGuidUtils.createGuidStringFromPublicKey(keyPair
              .getPublic().getEncoded());
      // Squirrel this away now just in case the call below times out.
      IOSKeyPairUtils.saveKeyPair(gnsInstance, alias, guid, keyPair);
      guidEntry = new GuidEntry(alias, guid, keyPair.getPublic(),
              keyPair.getPrivate());
    }
    return guidEntry;
  }

  private static CommandPacket accountGuidCreateInternal(String alias, String password,
          CommandType commandType, GuidEntry guidEntry)
          throws ClientException, NoSuchAlgorithmException {
    return getCommand(commandType,
            guidEntry, GNSProtocol.NAME.toString(), alias,
            GNSProtocol.PUBLIC_KEY.toString(),
            Base64.encodeToString(
                    guidEntry.publicKey.getEncoded(), false),
            GNSProtocol.PASSWORD.toString(),
            password != null ? Password.encryptAndEncodePassword(password, alias) : "");
  }

  private static CommandPacket guidCreateHelper(GuidEntry accountGuid,
          String name, PublicKey publicKey) throws ClientException {
    byte[] publicKeyBytes = publicKey.getEncoded();
    String publicKeyString = Base64.encodeToString(publicKeyBytes, false);
    return getCommand(CommandType.AddGuid, accountGuid,
            GNSProtocol.GUID.toString(), accountGuid.getGuid(),
            GNSProtocol.NAME.toString(), name,
            GNSProtocol.PUBLIC_KEY.toString(), publicKeyString);
  }

  private static CommandPacket aclAdd(String accessType,
          GuidEntry guid, String field, String accesserGuid)
          throws ClientException {
    return getCommand(CommandType.AclAddSelf, guid,
            GNSProtocol.ACL_TYPE.toString(), accessType,
            GNSProtocol.GUID.toString(), guid.getGuid(),
            GNSProtocol.FIELD.toString(), field,
            GNSProtocol.ACCESSER.toString(),
            accesserGuid == null ? GNSProtocol.ALL_GUIDS.toString() : accesserGuid);
  }

  private static CommandPacket aclRemove(String accessType,
          GuidEntry guid, String field, String accesserGuid)
          throws ClientException {
    return getCommand(CommandType.AclRemoveSelf, guid, GNSProtocol.ACL_TYPE.toString(),
            accessType, GNSProtocol.GUID.toString(), guid.getGuid(), GNSProtocol.FIELD.toString(), field, GNSProtocol.ACCESSER.toString(),
            accesserGuid == null ? GNSProtocol.ALL_GUIDS.toString() : accesserGuid);
  }

  private static CommandPacket aclGet(String accessType,
          GuidEntry guid, String field, String readerGuid)
          throws ClientException {
    return getCommand(CommandType.AclRetrieve, guid, GNSProtocol.ACL_TYPE.toString(), accessType,
            GNSProtocol.GUID.toString(), guid.getGuid(), GNSProtocol.FIELD.toString(), field, GNSProtocol.READER.toString(),
            readerGuid == null ? GNSProtocol.ALL_GUIDS.toString() : readerGuid);
  }



  public static final CommandPacket fieldCreateList(String targetGUID,
          String field, JSONArray list, GuidEntry querierGUID)
          throws ClientException {
    return getCommand(CommandType.CreateList, querierGUID, GNSProtocol.GUID.toString(),
            targetGUID, GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), list.toString(), GNSProtocol.WRITER.toString(),
            querierGUID.getGuid());
  }


  public static final CommandPacket fieldAppendOrCreateList(
          String targetGUID, String field, JSONArray list,
          GuidEntry querierGUID) throws ClientException {
    return getCommand(CommandType.AppendOrCreateList, querierGUID, GNSProtocol.GUID.toString(),
            targetGUID, GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), list.toString(), GNSProtocol.WRITER.toString(),
            querierGUID.getGuid());
  }


  public static final CommandPacket fieldReplaceOrCreateList(
          String targetGUID, String field, JSONArray list,
          GuidEntry querierGUID) throws ClientException {
    return getCommand(CommandType.ReplaceOrCreateList, querierGUID, GNSProtocol.GUID.toString(),
            targetGUID, GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), list.toString(), GNSProtocol.WRITER.toString(),
            querierGUID.getGuid());
  }


  public static final CommandPacket fieldAppend(String targetGUID,
          String field, JSONArray list, GuidEntry querierGUID)
          throws ClientException {
    return getCommand(CommandType.AppendListWithDuplication, querierGUID,
            GNSProtocol.GUID.toString(), targetGUID, GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), list.toString(), GNSProtocol.WRITER.toString(),
            querierGUID.getGuid());
  }


  public static final CommandPacket fieldReplaceList(String targetGUID,
          String field, JSONArray list, GuidEntry querierGUID)
          throws ClientException {
    return getCommand(CommandType.ReplaceList, querierGUID, GNSProtocol.GUID.toString(),
            targetGUID, GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), list.toString(), GNSProtocol.WRITER.toString(),
            querierGUID.getGuid());
  }


  public static final CommandPacket fieldClear(String targetGUID,
          String field, JSONArray list, GuidEntry querierGUID)
          throws ClientException {
    return getCommand(CommandType.RemoveList, querierGUID, GNSProtocol.GUID.toString(),
            targetGUID, GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), list.toString(), GNSProtocol.WRITER.toString(),
            querierGUID.getGuid());
  }


  public static final CommandPacket fieldClear(String targetGUID,
          String field, GuidEntry querierGUID) throws ClientException {
    return getCommand(CommandType.Clear, querierGUID, GNSProtocol.GUID.toString(), targetGUID,
            GNSProtocol.FIELD.toString(), field, GNSProtocol.WRITER.toString(), querierGUID.getGuid());
  }


  public static final CommandPacket fieldReadArray(String targetGUID,
          String field, GuidEntry querierGUID) throws ClientException {
    return getCommand(querierGUID != null ? CommandType.ReadArray
            : CommandType.ReadArrayUnsigned, querierGUID, GNSProtocol.GUID.toString(), targetGUID,
            GNSProtocol.FIELD.toString(), field, GNSProtocol.READER.toString(),
            querierGUID != null ? querierGUID.getGuid() : null);
  }


  public static final CommandPacket fieldSetElement(String targetGUID,
          String field, String newValue, int index, GuidEntry querierGUID)
          throws ClientException {
    return getCommand(CommandType.Set, querierGUID, GNSProtocol.GUID.toString(), targetGUID,
            GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), newValue, GNSProtocol.N.toString(), Integer.toString(index),
            GNSProtocol.WRITER.toString(), querierGUID.getGuid());
  }


  public static final CommandPacket fieldSetNull(String targetGUID,
          String field, GuidEntry querierGUID) throws ClientException {
    return getCommand(CommandType.SetFieldNull, querierGUID, GNSProtocol.GUID.toString(),
            targetGUID, GNSProtocol.FIELD.toString(), field, GNSProtocol.WRITER.toString(), querierGUID.getGuid());
  }



  public static final CommandPacket selectQuery(String query)
          throws ClientException {
    return getCommand(CommandType.SelectQuery, GNSProtocol.QUERY.toString(), query);
  }


  public static final CommandPacket selectSetupGroupQuery(
          GuidEntry accountGUID, String publicKey, String query, int interval)
          throws ClientException {
    return getCommand(CommandType.SelectGroupSetupQuery, GNSProtocol.GUID.toString(),
            accountGUID.getGuid(), GNSProtocol.PUBLIC_KEY.toString(), publicKey, GNSProtocol.QUERY.toString(), query,
            GNSProtocol.INTERVAL.toString(), interval);
  }


  public static final CommandPacket selectLookupGroupQuery(String groupGUID)
          throws ClientException {
    return getCommand(CommandType.SelectGroupLookupQuery, GNSProtocol.GUID.toString(), groupGUID);
  }


  public static final CommandPacket select(String field, String value)
          throws ClientException {
    return getCommand(CommandType.Select, GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value);
  }


  public static final CommandPacket selectWithin(String field, JSONArray value)
          throws ClientException {
    return getCommand(CommandType.SelectWithin, GNSProtocol.FIELD.toString(), field, GNSProtocol.WITHIN.toString(),
            value.toString());
  }


  public static final CommandPacket selectNear(String field, JSONArray value,
          Double maxDistance) throws ClientException {
    return getCommand(CommandType.SelectNear, GNSProtocol.FIELD.toString(), field, GNSProtocol.NEAR.toString(),
            value.toString(), GNSProtocol.MAX_DISTANCE.toString(), Double.toString(maxDistance));
  }


  public static final CommandPacket setLocation(String targetGUID,
          double longitude, double latitude, GuidEntry querierGUID)
          throws ClientException {
    return fieldReplaceOrCreateList(targetGUID, GNSProtocol.LOCATION_FIELD_NAME.toString(),
            new JSONArray(Arrays.asList(longitude, latitude)), querierGUID);
  }


  public static final CommandPacket setLocation(GuidEntry targetGUID,
          double longitude, double latitude) throws ClientException {
    return setLocation(targetGUID.getGuid(), longitude, latitude,
            targetGUID);
  }


  public static final CommandPacket getLocation(String targetGUID,
          GuidEntry querierGUID) throws ClientException {
    return fieldReadArray(targetGUID, GNSProtocol.LOCATION_FIELD_NAME.toString(), querierGUID);
  }


  public static final CommandPacket getLocation(GuidEntry targetGUID)
          throws ClientException {
    return fieldReadArray(targetGUID.getGuid(), GNSProtocol.LOCATION_FIELD_NAME.toString(),
            targetGUID);
  }



  public static final CommandPacket activeCodeClear(String targetGUID,
          String action, GuidEntry querierGUID) throws ClientException,
          IOException {
    return getCommand(CommandType.ClearCode, querierGUID, GNSProtocol.GUID.toString(),
            targetGUID, GNSProtocol.AC_ACTION.toString(), action, GNSProtocol.WRITER.toString(), querierGUID.getGuid());
  }


  public static final CommandPacket activeCodeSet(String targetGUID,
          String action, byte[] code, GuidEntry querierGUID)
          throws ClientException {
    return getCommand(CommandType.SetCode, querierGUID, GNSProtocol.GUID.toString(),
            targetGUID, GNSProtocol.AC_ACTION.toString(), action, GNSProtocol.AC_CODE.toString(),
            // This doesn't agree with the original method.
            // Is this encoding the byes for the user? Where is it decoded?
            Base64.encodeToString(code, true), GNSProtocol.WRITER.toString(),
            querierGUID.getGuid());
  }


  public static final CommandPacket activeCodeGet(String targetGUID,
          String action, GuidEntry querierGUID) throws ClientException {
    return getCommand(CommandType.GetCode, querierGUID, GNSProtocol.GUID.toString(),
            targetGUID, GNSProtocol.AC_ACTION.toString(), action, GNSProtocol.READER.toString(), querierGUID.getGuid());
  }



  public static final CommandPacket fieldCreateList(GuidEntry targetGUID,
          String field, JSONArray list) throws ClientException {
    return fieldCreateList(targetGUID.getGuid(), field, list, targetGUID);
  }


  public static final CommandPacket fieldCreateOneElementList(
          String targetGUID, String field, String value, GuidEntry querierGUID)
          throws ClientException {
    return getCommand(CommandType.Create, querierGUID, GNSProtocol.GUID.toString(), targetGUID,
            GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(), querierGUID.getGuid());
  }


  public static final CommandPacket fieldCreateOneElementList(
          GuidEntry targetGUID, String field, String value)
          throws ClientException {
    return fieldCreateOneElementList(targetGUID.getGuid(), field, value,
            targetGUID);
  }


  public static final CommandPacket fieldAppendOrCreate(String targetGUID,
          String field, String value, GuidEntry querierGUID)
          throws ClientException {
    return getCommand(CommandType.AppendOrCreate, querierGUID, GNSProtocol.GUID.toString(),
            targetGUID, GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(),
            querierGUID.getGuid());
  }


  public static final CommandPacket fieldReplaceOrCreate(String targetGUID,
          String field, String value, GuidEntry querierGUID)
          throws ClientException {
    return getCommand(CommandType.ReplaceOrCreate, querierGUID, GNSProtocol.GUID.toString(),
            targetGUID, GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(),
            querierGUID.getGuid());
  }


  public static final CommandPacket fieldReplaceOrCreateList(
          GuidEntry targetGUID, String field, JSONArray value)
          throws ClientException {
    return fieldReplaceOrCreateList(targetGUID.getGuid(), field, value,
            targetGUID);
  }


  public static final CommandPacket fieldReplaceOrCreateList(
          GuidEntry targetGUID, String field, String value)
          throws ClientException {
    return fieldReplaceOrCreate(targetGUID.getGuid(), field, value,
            targetGUID);
  }


  public static final CommandPacket fieldReplace(String targetGUID,
          String field, String value, GuidEntry querierGUID)
          throws ClientException {
    return getCommand(CommandType.Replace, querierGUID, GNSProtocol.GUID.toString(), targetGUID,
            GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(), querierGUID.getGuid());
  }


  public static final CommandPacket fieldReplace(GuidEntry targetGUID,
          String field, String value) throws ClientException {
    return fieldReplace(targetGUID.getGuid(), field, value, targetGUID);
  }


  public static final CommandPacket fieldReplace(GuidEntry targetGUID,
          String field, JSONArray value) throws ClientException {
    return fieldReplaceList(targetGUID.getGuid(), field, value, targetGUID);
  }


  public static final CommandPacket fieldAppend(String targetGUID,
          String field, String value, GuidEntry querierGUID)
          throws ClientException {
    return getCommand(CommandType.AppendWithDuplication, querierGUID, GNSProtocol.GUID.toString(),
            targetGUID, GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value, GNSProtocol.WRITER.toString(),
            querierGUID.getGuid());
  }


  public static final CommandPacket fieldAppend(GuidEntry targetGUID,
          String field, String value) throws ClientException {
    return fieldAppend(targetGUID.getGuid(), field, value, targetGUID);
  }


  public static final CommandPacket fieldAppendWithSetSemantics(
          String targetGUID, String field, JSONArray value,
          GuidEntry querierGUID) throws ClientException {
    return getCommand(CommandType.AppendList, querierGUID, GNSProtocol.GUID.toString(),
            targetGUID, GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), value.toString(), GNSProtocol.WRITER.toString(),
            querierGUID.getGuid());
  }


  public static final CommandPacket fieldAppendWithSetSemantics(
          GuidEntry targetGUID, String field, JSONArray value)
          throws ClientException {
    return fieldAppendWithSetSemantics(targetGUID.getGuid(), field, value,
            targetGUID);
  }


  public static final CommandPacket fieldAppendWithSetSemantics(
          String targetGUID, String field, String value, GuidEntry querierGUID)
          throws ClientException {
    return getCommand(CommandType.Append, querierGUID, GNSProtocol.GUID.toString(), targetGUID,
            GNSProtocol.FIELD.toString(), field,
            GNSProtocol.VALUE.toString(), value,
            GNSProtocol.WRITER.toString(), querierGUID.getGuid());
  }


  public static final CommandPacket fieldAppendWithSetSemantics(
          GuidEntry targetGUID, String field, String value)
          throws IOException, ClientException {
    return fieldAppendWithSetSemantics(targetGUID.getGuid(), field, value,
            targetGUID);
  }


  @Deprecated
  public static final CommandPacket fieldReplaceFirstElement(String targetGuid, String field,
          String value, GuidEntry writer)
          throws IOException, ClientException {
    return getCommand(writer != null ? CommandType.Replace : CommandType.ReplaceUnsigned,
            writer,
            GNSProtocol.GUID.toString(), targetGuid,
            GNSProtocol.FIELD.toString(), field,
            GNSProtocol.VALUE.toString(), value,
            GNSProtocol.WRITER.toString(), writer != null ? writer.getGuid() : null);
  }


  @Deprecated
  public CommandPacket fieldReplaceFirstElement(GuidEntry targetGuid, String field,
          String value) throws IOException, ClientException {
    return fieldReplaceFirstElement(targetGuid.getGuid(), field, value, targetGuid);
  }


  public static final CommandPacket fieldSubstitute(String targetGUID,
          String field, String newValue, String oldValue,
          GuidEntry querierGUID) throws ClientException {
    return getCommand(CommandType.Substitute, querierGUID, GNSProtocol.GUID.toString(),
            targetGUID, GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), newValue, GNSProtocol.OLD_VALUE.toString(), oldValue,
            GNSProtocol.WRITER.toString(), querierGUID.getGuid());
  }


  public static final CommandPacket fieldSubstitute(GuidEntry targetGUID,
          String field, String newValue, String oldValue)
          throws ClientException {
    return fieldSubstitute(targetGUID.getGuid(), field, newValue, oldValue,
            targetGUID);
  }


  public static final CommandPacket fieldSubstitute(String targetGUID,
          String field, JSONArray newValue, JSONArray oldValue,
          GuidEntry querierGUID) throws ClientException {
    return getCommand(CommandType.SubstituteList, querierGUID, GNSProtocol.GUID.toString(),
            targetGUID, GNSProtocol.FIELD.toString(), field, GNSProtocol.VALUE.toString(), newValue.toString(),
            GNSProtocol.OLD_VALUE.toString(), oldValue.toString(), GNSProtocol.WRITER.toString(), querierGUID.getGuid());
  }


  public static final CommandPacket fieldSubstitute(GuidEntry targetGUID,
          String field, JSONArray newValue, JSONArray oldValue)
          throws ClientException {
    return fieldSubstitute(targetGUID.getGuid(), field, newValue, oldValue,
            targetGUID);
  }


  @Deprecated
  // FIXME: This should probably go away.
  public static final CommandPacket fieldReadArrayFirstElement(String targetGUID, String field,
          GuidEntry reader) throws ClientException {
    return getCommand(reader != null ? CommandType.ReadArrayOne
            : CommandType.ReadArrayOneUnsigned, reader,
            GNSProtocol.GUID.toString(), targetGUID,
            GNSProtocol.FIELD.toString(), field,
            GNSProtocol.READER.toString(), reader != null ? reader.getGuid() : null);
  }


  @Deprecated
  // FIXME: This should probably go away.
  public static final CommandPacket fieldReadArrayFirstElement(GuidEntry targetGUID, String field)
          throws ClientException {
    return fieldReadArrayFirstElement(targetGUID.getGuid(), field, targetGUID);
  }


  public static final CommandPacket fieldRemove(GuidEntry targetGUID,
          String field) throws ClientException {
    return fieldRemove(targetGUID.getGuid(), field, targetGUID);
  }


  public static final CommandPacket dump()
          throws ClientException, IOException {
    return getCommand(CommandType.Dump, GNSProtocol.NAME.toString(), "Admin");
  }


  public CommandResultType getResultType() {
    return this.getCommandType().getResultType();
  }
}
