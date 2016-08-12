package edu.umass.cs.gnsclient.client;

import static edu.umass.cs.gnscommon.GNSCommandProtocol.ACCESSER;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.ACCOUNT_GUID;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.ACL_TYPE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.AC_ACTION;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.AC_CODE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.ALL_FIELDS;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.ALL_GUIDS;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.CODE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.FIELD;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.FIELDS;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.GROUP_ACL;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.GUID;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.GUID_RECORD_PUBLICKEY;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.INTERVAL;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.LOCATION_FIELD_NAME;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.MAX_DISTANCE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.MEMBER;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.MEMBERS;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.N;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.NAME;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.NAMES;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.NEAR;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.OLD_VALUE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.PASSKEY;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.PASSWORD;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.PUBLIC_KEY;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.PUBLIC_KEYS;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.QUERY;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.READER;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.RSA_ALGORITHM;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.USER_JSON;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.VALUE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.WITHIN;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.WRITER;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.client.util.Password;
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSCommandProtocol;
import edu.umass.cs.gnscommon.SharedGuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.client.EncryptionException;
import edu.umass.cs.gnscommon.exceptions.client.FieldNotFoundException;
import edu.umass.cs.gnscommon.exceptions.client.InvalidGuidException;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnsserver.gnsapp.packet.CommandPacket;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 *
 * A helper class with static methods to help construct GNS commands.
 */
public class GNSCommand extends CommandPacket {

  /**
   * The result types that can be returned by executing {@link CommandPacket}.
   */
  public static enum ResultType {
    /**
     * The default methods {@link CommandPacket#getResultString()} or
     * {@link CommandPacket#getResult()} be used to retrieve the result
     * irrespective of the result type.
     */
    STRING,
    /**
     * The methods {@link CommandPacket#getResultMap} or
     * {@link CommandPacket#getResultJSONObject} can be used if and only if
     * the result type is {@link #MAP};
     */
    MAP,
    /**
     * The methods {@link CommandPacket#getResultList},
     * {@link CommandPacket#getResultJSONArray} can be used if and only if
     * the result type is {@link #LIST}
     */
    LIST,
    /**
     * The method {@link CommandPacket#getResultBoolean} can be used if and
     * only if the result type is {@link #BOOLEAN}.
     */
    BOOLEAN,
    /**
     * The method {@link CommandPacket#getResultLong} can be used if and
     * only if the result type is {@link #LONG}.
     */
    LONG,
    /**
     * The method {@link CommandPacket#getResultInt} can be used if and only
     * if the result type is {@link #INTEGER}.
     */
    INTEGER,
    /**
     * The method {@link CommandPacket#getResultDouble} can be used if and
     * only if the result type is {@link #DOUBLE}.
     */
    DOUBLE,
    /**
     * The result of executing this command is null or does not return a
     * result.
     */
    NULL

  };

  /* GNSCommand constructors must remain private */
  protected GNSCommand(JSONObject command) {
    this(
            /* arun: we just generate a random value here because it is not easy (or
		 * worth trying) to guarantee non-conflicting IDs here. Conflicts will
		 * either result in an IOException further down or the query will be
		 * transformed to carry a different ID if */
            randomLong(), command);
  }
  protected GNSCommand(long id, JSONObject command) {
	    super(id, command);
	  }

  /**
   * Constructs a command of type {@code type} issued by the {@code querier}
   * using the variable length array {@code keysAndValues}. If {@code querier}
   * is non-null, the returned command will be signed by the querier's private
   * key. If {@code querier} is null, the command will succeed only if the
   * operation is open to all, which is normally true only for fields that are
   * readable by anyone.
   *
   * @param type
   * @param querier
   * The GUID issuing this query.
   * @param keysAndValues
   * A variable length array of even size containing a sequence of
   * key and value pairs.
   * @return A {@link CommandPacket} constructed using the supplied arguments.
   * @throws ClientException
   */
  public static GNSCommand getCommand(CommandType type, GuidEntry querier,
          Object... keysAndValues) throws ClientException {
    return new GNSCommand(CommandUtils.createAndSignCommand(type, querier,
            keysAndValues));
  }

  /**
   * @param type
   * @param keysAndValues
   * @return CommandPacket
   * @throws ClientException
   */
  public static GNSCommand getCommand(CommandType type,
          Object... keysAndValues) throws ClientException {
    return getCommand(type, null, keysAndValues);
  }

  /**
   * We just use a random long here as we will either get an IOException if
   * there is a conflicting request ID, i.e., if another unequal request is
   * awaiting a response in the async client unless ENABLE_ID_TRANSFORM is
   * true.
   */
  private static long randomLong() {
    return (long) (Math.random() * Long.MAX_VALUE);
  }

  /* ********** Start of actual command construction methods ********* */
  /**
   * @param targetGUID
   * The GUID being queried.
   * @param json
   * The JSONObject representation of the entire record value
   * excluding the targetGUID itself.
   *
   * @param querierGUID
   * The GUID issuing the query.
   * @return CommandPacket
   *
   * @throws ClientException
   */
  public static final CommandPacket update(String targetGUID,
          JSONObject json, GuidEntry querierGUID) throws ClientException {
    return getCommand(CommandType.ReplaceUserJSON, querierGUID, GUID,
            targetGUID, USER_JSON, json.toString(), WRITER,
            querierGUID.getGuid());
  }

  /**
   * @param targetGUID
   * The GUID being queried.
   * @param json
   * The JSONObject representation of the entire record value
   * excluding the targetGUID itself.
   * @return Refer {@link #update(String, JSONObject, GuidEntry)}
   * @throws ClientException
   */
  public static final CommandPacket update(GuidEntry targetGUID,
          JSONObject json) throws ClientException {
    return update(targetGUID.getGuid(), json, targetGUID);
  }

  /**
   * @param targetGuid
   * The GUID being queried.
   * @param field
   * The field key.
   * @param value
   * The value being assigned to targetGUID.field.
   * @param querierGUID
   * The GUID issuing the query.
   * @return Refer
   * {@link GNSClientCommands#fieldUpdate(String, String, Object, GuidEntry)}
   * .
   * @throws ClientException
   */
  public static final CommandPacket fieldUpdate(String targetGuid,
          String field, Object value, GuidEntry querierGUID)
          throws ClientException {
    return getCommand(CommandType.ReplaceUserJSON, querierGUID, GUID,
            targetGuid, USER_JSON, getJSONObject(field, value).toString(),
            WRITER, querierGUID.getGuid());
  }

  // converts JSONException to ClientException
  private static JSONObject getJSONObject(String field, Object value)
          throws ClientException {
    try {
      return new JSONObject().put(field, value);
    } catch (JSONException e) {
      throw new ClientException(e);
    }
  }

  /**
   * Creates an index for a field. {@code targetGUID} is only used for
   * authentication purposes. This command will succeed only the issuer has
   * the credentials to issue this query.
   *
   * @param GUID
   * The GUID issuing the query.
   * @param field
   * The field key.
   * @param index
   * The name of the index being created upon {@code field}.
   * @return CommandPacket
   * @throws ClientException
   */
  protected static final CommandPacket fieldCreateIndex(GuidEntry GUID,
          String field, String index) throws ClientException {
    return getCommand(CommandType.CreateIndex, GUID, GUID, GUID.getGuid(),
            FIELD, field, VALUE, index, WRITER, GUID.getGuid());
  }

  /**
   * Updates {@code targetGUID}:{@code field} to {@code value}. Signs the
   * query using the private key of {@code targetGUID}.
   *
   * @param targetGUID
   * The GUID being queried.
   * @param field
   * The field key.
   * @param value
   * The value being assigned to {@code field}.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket fieldUpdate(GuidEntry targetGUID,
          String field, Object value) throws ClientException {
    return fieldUpdate(targetGUID.getGuid(), field, value, targetGUID);
  }

  /**
   * Reads the entire record for {@code targetGUID}. {@code reader} is the
   * GUID issuing the query and must be present in the ACL for
   * {@code targetGUID}{@code ALL_FIELDS} for the query to succeed. If
   * {@code reader} is null, {@code targetGUID}{@code ALL_FIELDS} must be
   * globally readable.
   *
   * @param targetGUID
   * The GUID being queried.
   * @param querierGUID
   * The GUID issuing the query.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket read(String targetGUID,
          GuidEntry querierGUID) throws ClientException {
    return getCommand(querierGUID != null ? CommandType.ReadArray
            : CommandType.ReadArrayUnsigned, querierGUID, GUID, targetGUID,
            FIELD, ALL_FIELDS, READER,
            querierGUID != null ? querierGUID.getGuid() : null);
  }

  /**
   * Reads the entire record for {@code targetGUID} implicitly assuming that
   * the querier is also {@code targetGUID}.
   *
   * @param targetGUID
   * The GUID being queried.
   * @return CommandPacket
   * @throws Exception
   */
  public static final CommandPacket read(GuidEntry targetGUID)
          throws Exception {
    return read(targetGUID.getGuid(), targetGUID);
  }

  /**
   * Checks if {@code field} exists in {@code targetGuid}.
   *
   * @param targetGUID
   * The GUID being queried.
   * @param field
   * The field key.
   * @param querierGUID
   * The GUID issuing the query.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket fieldExists(String targetGUID,
          String field, GuidEntry querierGUID) throws ClientException {
    return getCommand(querierGUID != null ? CommandType.Read
            : CommandType.ReadUnsigned, querierGUID, GUID, targetGUID,
            FIELD, field, READER,
            querierGUID != null ? querierGUID.getGuid() : null);
  }

  /**
   * Checks if {@code field} exists in {@code targetGuid}. The querier GUID is
   * assumed to also be {@code targetGUID}.
   *
   * @param targetGUID
   * The GUID being queried.
   * @param field
   * The field key.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket fieldExists(GuidEntry targetGUID,
          String field) throws ClientException {
    return fieldExists(targetGUID.getGuid(), field, targetGUID);
  }

  /**
   * Reads the value of {@code targetGUID}:{@code field}. {@code querierGUID},
   * if not null, must be present in the ACL for {@code targetGUID}:
   * {@code field}; if null, {@code targetGUID}:{@code field} must be globally
   * readable.
   *
   * @param targetGUID
   * The GUID being queried.
   * @param field
   * The field key.
   * @param querierGUID
   * The GUID issuing the query.
   * @return a string containing the values in the field
   * @throws ClientException
   */
  public static final CommandPacket fieldRead(String targetGUID,
          String field, GuidEntry querierGUID) throws ClientException {
    return getCommand(querierGUID != null ? CommandType.Read
            : CommandType.ReadUnsigned, querierGUID, GUID, targetGUID,
            FIELD, field, READER,
            querierGUID != null ? querierGUID.getGuid() : null);
  }

  /**
   * Same as {@link #fieldRead(String, String, GuidEntry)} with
   * {@code querierGUID} set to {@code targetGUID}. The result type of the
   * execution result of this query is {@link GNSCommand.ResultType#MAP}.
   *
   * @param targetGUID
   * The GUID being queried.
   * @param field
   * The field key.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket fieldRead(GuidEntry targetGUID,
          String field) throws ClientException {
    return fieldRead(targetGUID.getGuid(), field, targetGUID);
  }

  /**
   * Reads {@code targetGUID}:{@code field} for each field in {@code fields}.
   * {@code querierGUID} must be present in the read ACL of every field in
   * {@code fields}. The result type of the execution result of this query is
   * {@link GNSCommand.ResultType#LIST}.
   *
   * @param targetGUID
   * The GUID being queried.
   * @param fields
   * The list of field keys being queried.
   * @param querierGUID
   * The GUID issuing the query.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket fieldRead(String targetGUID,
          ArrayList<String> fields, GuidEntry querierGUID)
          throws ClientException {
    return getCommand(querierGUID != null ? CommandType.ReadMultiField
            : CommandType.ReadMultiFieldUnsigned, querierGUID, GUID,
            targetGUID, FIELDS, fields, READER,
            querierGUID != null ? querierGUID.getGuid() : null);
  }

  /**
   * Same as {@link #fieldRead(String, ArrayList, GuidEntry)} with
   * {@code querierGUID} set to {@code targetGUID}.
   *
   * @param targetGUID
   * @param fields
   * The list of field keys.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket fieldRead(GuidEntry targetGUID,
          ArrayList<String> fields) throws ClientException {
    return fieldRead(targetGUID.getGuid(), fields, targetGUID);
  }

  /**
   * Removes {@code targetGUID}:{@code field}. {@code querierGUID} must be
   * present in the write ACL of {@code targetGUID}:{@code field} for the
   * query to succeed.
   *
   * @param targetGUID
   * The GUID being queried.
   * @param field
   * The field key.
   * @param querierGUID
   * The GUID issuing the query.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket fieldRemove(String targetGUID,
          String field, GuidEntry querierGUID) throws ClientException {
    return getCommand(CommandType.RemoveField, querierGUID, GUID,
            targetGUID, FIELD, field, WRITER, querierGUID.getGuid());
  }

  // ACCOUNT COMMANDS
  /**
   * Retrieves the GUID of {@code alias}.
   *
   * @param alias
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket lookupGUID(String alias)
          throws ClientException {

    return getCommand(CommandType.LookupGuid, NAME, alias);
  }

  /**
   * If this is a sub-GUID, this command looks up the corresponding account
   * GUID.
   *
   * @param subGUID
   * @return Account GUID of {@code subGUID}
   * @throws ClientException
   */
  public static final CommandPacket lookupPrimaryGUID(String subGUID)
          throws ClientException {
    return getCommand(CommandType.LookupPrimaryGuid, GUID, subGUID);
  }

  /**
   * Looks up metadata for {@code targetGUID}. The result type of the
   * execution result of this query is {@link GNSCommand.ResultType#MAP}.
   *
   * @param targetGUID
   * The GUID being queried.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket lookupGUIDRecord(String targetGUID)
          throws ClientException {
    return (getCommand(CommandType.LookupGuidRecord, GUID, targetGUID));
  }

  /**
   * Looks up the the metadata for {@code accountGUID}.
   *
   * @param accountGUID
   * The account GUID being queried.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket lookupAccountRecord(String accountGUID)
          throws ClientException {
    return getCommand(CommandType.LookupAccountRecord, GUID, accountGUID);
  }

  /**
   * Get the public key for {@code alias}.
   *
   * @param alias
   * @return CommandPacket
   * @throws ClientException
   */
  PublicKey publicKeyLookupFromAlias(String alias)
          throws InvalidGuidException, ClientException, IOException {
    throw new RuntimeException("Unimplementable");
    // String guid = lookupGuid(alias);
    // return publicKeyLookupFromGuid(guid);
  }

  /**
   * Get the public key for a given GUID.
   *
   * @param targetGUID
   * The GUID being queried.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket publicKeyLookupFromGUID(String targetGUID)
          throws ClientException {
    return lookupGUIDRecord(targetGUID);
  }

  /**
   * Register a new account GUID with the name {@code alias} and a password
   * {@code password}. Executing this query generates a new GUID and a public
   * / private key pair. {@code password} can be used to retrieve account
   * information if the client loses the private key corresponding to the
   * account GUID.
   *
   * @param gnsInstance
   *
   * @param alias
   * Human readable alias for the account GUID being created, e.g.,
   * an email address
   * @param password
   * @return CommandPacket
   * @throws Exception
   */
  public static final CommandPacket accountGuidCreate(String gnsInstance,
          String alias, String password) throws Exception {
	    GuidEntry entry = GuidUtils.lookupGuidEntryFromDatabase(gnsInstance, alias);
	    /* arun: Don't recreate pair if one already exists. Otherwise you can
			 * not get out of the funk where the account creation timed out but
			 * wasn't rolled back fully at the server. Re-using
			 * the same GUID will at least pass verification as opposed to 
			 * incurring an ACTIVE_REPLICA_EXCEPTION for a new (non-existent) GUID.
	     */
		if (entry == null) {
			KeyPair keyPair = KeyPairGenerator.getInstance(RSA_ALGORITHM)
					.generateKeyPair();
			String guid = SharedGuidUtils.createGuidStringFromPublicKey(keyPair
					.getPublic().getEncoded());
			// Squirrel this away now just in case the call below times out.
			KeyPairUtils.saveKeyPair(gnsInstance, alias, guid, keyPair);
			entry = new GuidEntry(alias, guid, keyPair.getPublic(),
					keyPair.getPrivate());
		}
    return accountGuidCreateHelper(alias, entry, password);
  }

  /**
   * Verify an account by sending the verification code back to the server.
   *
   * @param accountGUID
   * The account GUID to verify
   * @param code
   * The verification code
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket accountGuidVerify(GuidEntry accountGUID,
          String code) throws ClientException {
    return getCommand(CommandType.VerifyAccount, accountGUID, GUID,
            accountGUID.getGuid(), CODE, code);
  }

  /**
   * Deletes the account named {@code name}.
   *
   * @param accountGUID
   * GuidEntry for the account being removed.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket accountGuidRemove(GuidEntry accountGUID)
          throws ClientException {
    return getCommand(CommandType.RemoveAccount, accountGUID, GUID,
            accountGUID.getGuid(), NAME, accountGUID.getEntityName());
  }

  /**
   * Creates an new GUID associated with an account on the GNS server.
   *
   * @param gnsInstance
   * The name of the GNS service instance.
   *
   * @param accountGUID
   * The account GUID being created.
   * @param alias
   * The alias for the account GUID.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket createGUID(String gnsInstance,
          GuidEntry accountGUID, String alias) throws ClientException {
    try {
      return guidCreateHelper(accountGUID, alias, GuidUtils
              .createAndSaveGuidEntry(alias, gnsInstance).getPublicKey());
    } catch (NoSuchAlgorithmException e) {
      throw new ClientException(e);
    }
  }

  /**
   * Creates a batch of GUIDs listed in {@code aliases} using gigapaxos' batch
   * creation mechanism.
   *
   * @param gnsInstance
   * The name of the GNS service instance.
   *
   * @param accountGUID
   * @param aliases
   * The batch of names being created.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket batchCreateGUIDs(String gnsInstance,
          GuidEntry accountGUID, Set<String> aliases) throws ClientException {

    List<String> aliasList = new ArrayList<>(aliases);
    List<String> publicKeys = null;
    publicKeys = new ArrayList<>();
    for (String alias : aliasList) {
      GuidEntry entry;
      try {
        entry = GuidUtils.createAndSaveGuidEntry(alias, gnsInstance);
      } catch (NoSuchAlgorithmException e) {
        // FIXME: Do we need to roll back created keys?
        throw new ClientException(e);
      }
      byte[] publicKeyBytes = entry.getPublicKey().getEncoded();
      String publicKeyString = Base64.encodeToString(publicKeyBytes,
              false);
      publicKeys.add(publicKeyString);
    }

    return getCommand(CommandType.AddMultipleGuids, accountGUID, GUID,
            accountGUID.getGuid(), NAMES, new JSONArray(aliasList),
            PUBLIC_KEYS, new JSONArray(publicKeys));
  }

  /**
   * Removes {@code targetGUID} that is not an account GUID.
   *
   * @param targetGUID
   * The GUID being removed.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket removeGUID(GuidEntry targetGUID)
          throws ClientException {
    return getCommand(CommandType.RemoveGuidNoAccount, targetGUID, GUID,
            targetGUID.getGuid());
  }

  /**
   * Removes {@code targetGUID} given the associated {@code accountGUID}.
   *
   * @param accountGUID
   * @param targetGUID
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket removeGUID(GuidEntry accountGUID,
          String targetGUID) throws ClientException {
    return getCommand(CommandType.RemoveGuid, accountGUID, ACCOUNT_GUID,
            accountGUID.getGuid(), GUID, targetGUID);
  }

  // GROUP COMMANDS
  /**
   * Looks up the list of GUIDs that are members of {@code groupGUID}. The
   * result type of the execution result of this query is
   * {@link GNSCommand.ResultType#LIST}.
   *
   * @param groupGuid
   * The group GUID being queried.
   * @param querierGUID
   * The GUID issuing of the query.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket groupGetMembers(String groupGuid,
          GuidEntry querierGUID) throws ClientException {
    return getCommand(CommandType.GetGroupMembers, querierGUID, GUID,
            groupGuid, READER, querierGUID.getGuid());
  }

  /**
   * Looks up the list of groups of which {@code targetGUID} is a member.
   *
   * @param targetGUID
   * The GUID whose groups are being looked up.
   * @param querierGUID
   * The GUID issuing the query.
   * @return the list of groups as a JSONArray
   * @throws ClientException
   * if a protocol error occurs or the list cannot be parsed
   */
  public static final CommandPacket guidGetGroups(String targetGUID,
          GuidEntry querierGUID) throws ClientException {

    return getCommand(CommandType.GetGroups, querierGUID, GUID, targetGUID,
            READER, querierGUID.getGuid());
  }

  /**
   * Add a GUID to {@code groupGUID}. Any GUID can be a group GUID.
   *
   * @param groupGUID
   * The GUID of the group.
   * @param toAddGUID
   * The GUID being added.
   * @param querierGUID
   * The GUID issuing the query.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket groupAddGuid(String groupGUID,
          String toAddGUID, GuidEntry querierGUID) throws ClientException {
    return getCommand(CommandType.AddToGroup, querierGUID, GUID, groupGUID,
            MEMBER, toAddGUID, WRITER, querierGUID.getGuid());
  }

  /**
   * Add multiple members to a group.
   *
   * @param groupGUID
   * The GUID of the group.
   * @param members
   * The member GUIDs to add to the group.
   * @param querierGUID
   * The GUID issuing the query.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket groupAddGUIDs(String groupGUID,
          Set<String> members, GuidEntry querierGUID) throws ClientException {
    return getCommand(CommandType.AddMembersToGroup, querierGUID, GUID,
            groupGUID, MEMBERS, new JSONArray(members).toString(), WRITER,
            querierGUID.getGuid());
  }

  /**
   * Removes a GUID from a group GUID. Any GUID can be a group GUID.
   *
   *
   * @param groupGUID
   * The group GUID
   * @param toRemoveGUID
   * The GUID being removed.
   * @param querierGUID
   * The GUID issuing the query.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket groupRemoveGuid(String groupGUID,
          String toRemoveGUID, GuidEntry querierGUID) throws ClientException {
    return getCommand(CommandType.RemoveFromGroup, querierGUID, GUID,
            groupGUID, MEMBER, toRemoveGUID, WRITER, querierGUID.getGuid());
  }

  /**
   * Remove a list of members from a group
   *
   * @param groupGUID
   * The group GUID
   * @param members
   * The GUIDs to be removed.
   * @param querierGUID
   * The GUID issuing the query.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket groupRemoveGuids(String groupGUID,
          JSONArray members, GuidEntry querierGUID) throws ClientException {
    return getCommand(CommandType.RemoveMembersFromGroup, querierGUID,
            GUID, groupGUID, MEMBERS, members.toString(), WRITER,
            querierGUID.getGuid());
  }

  /**
   * Authorize {@code toAuthorizeGUID} to add/remove members from the group
   * {@code groupGUID}. If {@code toAuthorizeGUID} is null, everyone is
   * authorized to add/remove members to the group. Note that this method can
   * only be called by the group owner (private key required).
   *
   * @param groupGUID
   * the group GUID entry
   * @param toAuthorizeGUID
   * the GUID being authorized to change group membership or null
   * for anyone
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket groupAddMembershipUpdatePermission(
          GuidEntry groupGUID, String toAuthorizeGUID) throws ClientException {
    return aclAdd(AclAccessType.WRITE_WHITELIST, groupGUID, GROUP_ACL,
            toAuthorizeGUID);
  }

  /**
   * Unauthorize guidToUnauthorize to add/remove members from the group
   * groupGuid. If guidToUnauthorize is null, everyone is forbidden to
   * add/remove members to the group. Note that this method can only be called
   * by the group owner (private key required). Signs the query using the
   * private key of the group owner.
   *
   * @param groupGuid
   * the group GUID entry
   * @param guidToUnauthorize
   * the guid to authorize to manipulate group membership or null
   * for anyone
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket groupRemoveMembershipUpdatePermission(
          GuidEntry groupGuid, String guidToUnauthorize)
          throws ClientException {
    return aclRemove(AclAccessType.WRITE_WHITELIST, groupGuid, GROUP_ACL,
            guidToUnauthorize);
  }

  /**
   * Authorize {@code toAuthorizeGUID} to get the membership list from the
   * group {@code groupGUID}. If {@code toAuthorizeGUID} is null, everyone is
   * authorized to list members of the group. Note that this method can only
   * be called by the group owner (private key required).
   *
   * @param groupGUID
   * the group GUID entry
   * @param toAuthorizeGUID
   * the guid to authorize to manipulate group membership or null
   * for anyone
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket groupAddMembershipReadPermission(
          GuidEntry groupGUID, String toAuthorizeGUID) throws ClientException {
    return aclAdd(AclAccessType.READ_WHITELIST, groupGUID, GROUP_ACL,
            toAuthorizeGUID);
  }

  /**
   * Unauthorize {@code toUnauthorizeGUID} to get the membership list from the
   * group {@code groupGUID}. If {@code toUnauthorizeGUID} is null, everyone
   * is forbidden from querying the group membership. Note that this method
   * can only be called by the group owner (private key required).
   *
   * @param groupGUID
   * the group GUID entry
   * @param toUnauthorizeGUID
   * The GUID to unauthorize to change group membership or null for
   * anyone.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket groupRemoveMembershipReadPermission(
          GuidEntry groupGUID, String toUnauthorizeGUID)
          throws ClientException {
    return aclRemove(AclAccessType.READ_WHITELIST, groupGUID, GROUP_ACL,
            toUnauthorizeGUID);
  }

  /* ************* ACL COMMANDS ********************* */
  /**
   * Adds {@code accessorGUID} to the access control list (ACL) of
   * {@code targetGUID}:{@code field}. {@code accessorGUID} can be a GUID of a
   * user or a group GUID or null that means anyone can access the field. The
   * field can be also be +ALL+ which means the {@code accessorGUID} is being
   * added to the ACLs of all fields of {@code targetGUID}.
   *
   * @param accessType
   * a value from GnrsProtocol.AclAccessType
   * @param targetGUID
   * The GUID being queried (updated).
   * @param field
   * The field key.
   * @param accesserGUID
   * GUID to add to the ACL
   * @return CommandPacket
   * @throws ClientException
   * if the query is not accepted by the server.
   */
  public static final CommandPacket aclAdd(AclAccessType accessType,
          GuidEntry targetGUID, String field, String accesserGUID)
          throws ClientException {
    return aclAdd(accessType.name(), targetGUID, field, accesserGUID);
  }

  /**
   * Removes {@code accessorGUID} from the access control list (ACL) of
   * {@code targetGUID}:{@code field}. {@code accessorGUID} can be a GUID of a
   * user or a group GUID or null that means anyone can access the field. The
   * field can be also be +ALL+ which means the {@code accessorGUID} is being
   * added to the ACLs of all fields of {@code targetGUID}.
   *
   * @param accessType
   * @param targetGUID
   * @param field
   * The field key.
   * @param accesserGUID
   * The GUID to remove from the ACL.
   * @return CommandPacket
   * @throws ClientException
   * if the query is not accepted by the server.
   */
  public static final CommandPacket aclRemove(AclAccessType accessType,
          GuidEntry targetGUID, String field, String accesserGUID)
          throws ClientException {
    return aclRemove(accessType.name(), targetGUID, field, accesserGUID);
  }

  /**
   * Get the access control list of {@code targetGUID}:{@code field}.
   * {@code accesserGUID} can be a user or group GUID or null meaning that
   * anyone can access the field. The field can be also be +ALL+ meaning that
   * FIXME: TBD.
   *
   * @param accessType
   * @param targetGUID
   * The GUID being queried.
   * @param field
   * The field key.
   * @param querierGUID
   * The GUID issuing the query.
   * @return CommandPacket
   * @throws ClientException
   * if the query is not accepted by the server.
   */
  public static final CommandPacket aclGet(AclAccessType accessType,
          GuidEntry targetGUID, String field, String querierGUID)
          throws ClientException {
    return aclGet(accessType.name(), targetGUID, field, querierGUID);
  }

  /* ********************* ALIASES ******************** */
  /**
   * Creates an alias for {@code targetGUID}. The alias can be used just like
   * the original GUID.
   *
   * @param targetGUID
   * @param name
   * - the alias
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket addAlias(GuidEntry targetGUID, String name)
          throws ClientException {
    return getCommand(CommandType.AddAlias, targetGUID, GUID,
            targetGUID.getGuid(), NAME, name);
  }

  /**
   * Removes the alias {@code name} for {@code targetGUID}.
   *
   * @param targetGUID
   * The GUID being queried.
   * @param name
   * The alias.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket removeAlias(GuidEntry targetGUID,
          String name) throws ClientException {
    return getCommand(CommandType.RemoveAlias, targetGUID, GUID,
            targetGUID.getGuid(), NAME, name);
  }

  /**
   * Retrieve the aliases associated with the given guid.
   *
   * @param guid
   * @return - a JSONArray containing the aliases
   * @throws Exception
   */
  public static final CommandPacket getAliases(GuidEntry guid)
          throws Exception {
    return getCommand(CommandType.RetrieveAliases, guid, GUID,
            guid.getGuid());
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
  private static final CommandPacket guidCreateHelper(GuidEntry accountGuid,
          String name, PublicKey publicKey) throws ClientException {
    byte[] publicKeyBytes = publicKey.getEncoded();
    String publicKeyString = Base64.encodeToString(publicKeyBytes, false);
    return getCommand(CommandType.AddGuid, accountGuid, GUID,
            accountGuid.getGuid(), NAME, name, PUBLIC_KEY, publicKeyString);
  }

	/**
	 * Register a new account guid with the corresponding alias and the given
	 * public key on the GNS server. Returns a new guid.
	 *
	 * @param alias
	 *            the alias to register (usually an email address)
	 * @param guidEntry
	 * @param password
	 *            the public key associate with the account
	 * @return guid the GUID generated by the GNS
	 * @throws IOException
	 * @throws UnsupportedEncodingException
	 * @throws ClientException
	 * @throws InvalidGuidException
	 *             if the user already exists
	 * @throws NoSuchAlgorithmException
	 */
  public static final CommandPacket accountGuidCreateHelper(String alias,
          GuidEntry guidEntry, String password)
          throws UnsupportedEncodingException, IOException, ClientException,
          InvalidGuidException, NoSuchAlgorithmException {
    return //password != null
            //            ? 
            getCommand(CommandType.RegisterAccount,
                    guidEntry, NAME, alias, PUBLIC_KEY, Base64.encodeToString(
                            guidEntry.publicKey.getEncoded(), false),
                    PASSWORD,
                    password != null ? Base64.encodeToString(Password.encryptPassword(password, alias), false)
                            : "");
//            : getCommand(CommandType.RegisterAccountSansPassword,
//                    guidEntry.getPrivateKey(), guidEntry.publicKey, NAME,
//                    alias, PUBLIC_KEY, Base64.encodeToString(
//                            guidEntry.publicKey.getEncoded(), false));
  }

  private static final CommandPacket aclAdd(String accessType,
          GuidEntry guid, String field, String accesserGuid)
          throws ClientException {
    return getCommand(CommandType.AclAddSelf, guid, ACL_TYPE, accessType,
            GUID, guid.getGuid(), FIELD, field, ACCESSER,
            accesserGuid == null ? ALL_GUIDS : accesserGuid);
  }

  private static final CommandPacket aclRemove(String accessType,
          GuidEntry guid, String field, String accesserGuid)
          throws ClientException {
    return getCommand(CommandType.AclRemoveSelf, guid, ACL_TYPE,
            accessType, GUID, guid.getGuid(), FIELD, field, ACCESSER,
            accesserGuid == null ? ALL_GUIDS : accesserGuid);
  }

  private static final CommandPacket aclGet(String accessType,
          GuidEntry guid, String field, String readerGuid)
          throws ClientException {
    return getCommand(CommandType.AclRetrieve, guid, ACL_TYPE, accessType,
            GUID, guid.getGuid(), FIELD, field, READER,
            readerGuid == null ? ALL_GUIDS : readerGuid);
  }

  /* ******************* Extended commands ******************** */
  /**
   * Creates anew {@code targetGUID}:{@code field} with the value
   * {@code value}.
   *
   * @param targetGUID
   * The GUID being queried.
   * @param field
   * The field key.
   * @param list
   * The list.
   * @param querierGUID
   * The GUID issuing the query.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket fieldCreateList(String targetGUID,
          String field, JSONArray list, GuidEntry querierGUID)
          throws ClientException {
    return getCommand(CommandType.CreateList, querierGUID, GUID,
            targetGUID, FIELD, field, VALUE, list.toString(), WRITER,
            querierGUID.getGuid());
  }

  /**
   * Appends the values in the list {@code value} to the list
   * {@code targetGUID}:{@code field} or creates anew {@code field} with the
   * values in the list {@code value} if it does not exist.
   *
   * @param targetGUID
   * The GUID being queried.
   * @param field
   * The field key.
   * @param list
   * The list value.
   * @param querierGUID
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket fieldAppendOrCreateList(
          String targetGUID, String field, JSONArray list,
          GuidEntry querierGUID) throws ClientException {
    return getCommand(CommandType.AppendOrCreateList, querierGUID, GUID,
            targetGUID, FIELD, field, VALUE, list.toString(), WRITER,
            querierGUID.getGuid());
  }

  /**
   * Replaces the values of the field with the list of values or creates a new
   * field with values in the list if it does not exist.
   *
   * @param targetGUID
   * The GUID being queried (updated).
   * @param field
   * The field key.
   * @param list
   * The list value.
   * @param querierGUID
   * The GUID issuing the query.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket fieldReplaceOrCreateList(
          String targetGUID, String field, JSONArray list,
          GuidEntry querierGUID) throws ClientException {
    return getCommand(CommandType.ReplaceOrCreateList, querierGUID, GUID,
            targetGUID, FIELD, field, VALUE, list.toString(), WRITER,
            querierGUID.getGuid());
  }

  /**
   * Appends {@code list} to the list {@code targetGUID}:{@code field}
   * creating the list if it does not exist.
   *
   * @param targetGUID
   * GUID where the field is stored
   * @param field
   * field name
   * @param list
   * list of values
   * @param querierGUID
   * GUID entry of the writer
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket fieldAppend(String targetGUID,
          String field, JSONArray list, GuidEntry querierGUID)
          throws ClientException {
    return getCommand(CommandType.AppendListWithDuplication, querierGUID,
            GUID, targetGUID, FIELD, field, VALUE, list.toString(), WRITER,
            querierGUID.getGuid());
  }

  /**
   * Replaces the list {@code targetGUID}:{@code field} with {@code list}.
   *
   * @param targetGUID
   * The GUID being queried.
   * @param field
   * The field key
   * @param list
   * The list value.
   * @param querierGUID
   * GUID entry of the writer
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket fieldReplaceList(String targetGUID,
          String field, JSONArray list, GuidEntry querierGUID)
          throws ClientException {
    return getCommand(CommandType.ReplaceList, querierGUID, GUID,
            targetGUID, FIELD, field, VALUE, list.toString(), WRITER,
            querierGUID.getGuid());
  }

  /**
   * Removes all the values in {@code list} from {@code targetGUID}:
   * {@code field}.
   *
   * @param targetGUID
   * The GUID being queried (updated)
   * @param field
   * The field key
   * @param list
   * The list value.
   * @param querierGUID
   * The GUID issuing the query.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket fieldClear(String targetGUID,
          String field, JSONArray list, GuidEntry querierGUID)
          throws ClientException {
    return getCommand(CommandType.RemoveList, querierGUID, GUID,
            targetGUID, FIELD, field, VALUE, list.toString(), WRITER,
            querierGUID.getGuid());
  }

  /**
   * Removes all values from {@code targetGUID}:{@code field}.
   *
   * @param targetGUID
   * The GUID being queried (updated).
   * @param field
   * The field key.
   * @param querierGUID
   * The GUID issuing the update.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket fieldClear(String targetGUID,
          String field, GuidEntry querierGUID) throws ClientException {
    return getCommand(CommandType.Clear, querierGUID, GUID, targetGUID,
            FIELD, field, WRITER, querierGUID.getGuid());
  }

  /**
   * Reads the list field {@code targetGUID}:{@code field}. The result type of
   * the execution result of this query is {@link GNSCommand.ResultType#LIST}.
   *
   * @param targetGUID
   * The GUID being queried.
   * @param field
   * The field key.
   * @param querierGUID
   * The GUID issuing the query.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket fieldReadArray(String targetGUID,
          String field, GuidEntry querierGUID) throws ClientException {
    return getCommand(querierGUID != null ? CommandType.ReadArray
            : CommandType.ReadArrayUnsigned, querierGUID, GUID, targetGUID,
            FIELD, field, READER,
            querierGUID != null ? querierGUID.getGuid() : null);
  }

  /**
   * Sets the nth value (zero-based) indicated by {@code index} in the list
   * {@code targetGUID}:{@code field} to {@code newValue}. Index must be less
   * than the current size of the list.
   *
   * @param targetGUID
   * The GUID being queried (updated).
   * @param field
   * The field key.
   * @param newValue
   * The new value.
   * @param index
   * The index of the array element being updated.
   * @param querierGUID
   * The GUID issuing the query.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket fieldSetElement(String targetGUID,
          String field, String newValue, int index, GuidEntry querierGUID)
          throws ClientException {
    return getCommand(CommandType.Set, querierGUID, GUID, targetGUID,
            FIELD, field, VALUE, newValue, N, Integer.toString(index),
            WRITER, querierGUID.getGuid());
  }

  /**
   * Sets {@code targetGUID}:{@code field} to null. Subsequent reads of the
   * field will return a result type of null.
   *
   * @param targetGUID
   * @param field
   * @param querierGUID
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket fieldSetNull(String targetGUID,
          String field, GuidEntry querierGUID) throws ClientException {
    return getCommand(CommandType.SetFieldNull, querierGUID, GUID,
            targetGUID, FIELD, field, WRITER, querierGUID.getGuid());
  }

  /* *********************** SELECT *********************** */
  /**
   * Selects all GUID records that match {@code query}. The result type of the
   * execution result of this query is {@link GNSCommand.ResultType#LIST}.
   *
   * The query syntax is described here:
   * https://gns.name/wiki/index.php?title=Query_Syntax
   *
   * There are some predefined field names such as
   * {@link GNSCommandProtocol#LOCATION_FIELD_NAME} and
   * {@link GNSCommandProtocol#IPADDRESS_FIELD_NAME} that are indexed by
   * default.
   *
   * There are links in the wiki page above to find the exact syntax for
   * querying spatial coordinates.
   *
   * @param query
   * The select query being issued.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket selectQuery(String query)
          throws ClientException {
    return getCommand(CommandType.SelectQuery, QUERY, query);
  }

  /**
   * Set up a context-aware group GUID corresponding to the query. Requires
   * {@code accountGuid} and {@code publicKey} that are used to set up the new
   * GUID or look it up if it already exists. The result type of the execution
   * result of this query is {@link GNSCommand.ResultType#LIST}.
   *
   * The query syntax is described here:
   * https://gns.name/wiki/index.php?title=Query_Syntax
   *
   * @param accountGUID
   * The GUID issuing the query.
   * @param publicKey
   * @param query
   * The select query.
   * @param interval
   * The refresh interval in seconds (default 60).
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket selectSetupGroupQuery(
          GuidEntry accountGUID, String publicKey, String query, int interval)
          throws ClientException {
    return getCommand(CommandType.SelectGroupSetupQuery, GUID,
            accountGUID.getGuid(), PUBLIC_KEY, publicKey, QUERY, query,
            INTERVAL, interval);
  }

  /**
   * Looks up the membership of a context-aware group GUID created using a
   * query. The results may be stale if the queries that happen more quickly
   * than the refresh interval given during setup. The result type of the
   * execution result of this query is {@link GNSCommand.ResultType#LIST}.
   *
   * @param groupGUID
   * The group GUID being queried.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket selectLookupGroupQuery(String groupGUID)
          throws ClientException {
    return getCommand(CommandType.SelectGroupLookupQuery, GUID, groupGUID);
  }

  /**
   * Searches for all GUIDs whose {@code field} has the value {@code value}.
   *
   * @param field
   * The field key.
   * @param value
   * The value that is being searched.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket select(String field, String value)
          throws ClientException {
    return getCommand(CommandType.Select, FIELD, field, VALUE, value);
  }

  /**
   * If {@code field} is a GeoSpatial field, the query searches for all GUIDs
   * that have fields that are within the bounding box specified by
   * {@code value} as nested JSONArrays of paired tuples: [[LONG_UL,
   * LAT_UL],[LONG_BR, LAT_BR]]. The result type of the execution result of
   * this query is {@link GNSCommand.ResultType#LIST}.
   *
   * @param field
   * The field key.
   * @param value
   * - [[LONG_UL, LAT_UL],[LONG_BR, LAT_BR]]
   * @return CommandPacket
   * @throws Exception
   */
  public static final CommandPacket selectWithin(String field, JSONArray value)
          throws Exception {
    return getCommand(CommandType.SelectWithin, FIELD, field, WITHIN,
            value.toString());
  }

  /**
   * If {@code field} is a GeoSpatial field, the query searches for all GUIDs
   * whose {@code field} is near {@code value} that is a point specified as a
   * two element JSONArray: [LONG, LAT]. {@code maxDistance} is in meters.
   *
   * @param field
   * The field key
   * @param value
   * - [LONG, LAT]
   * @param maxDistance
   * - distance in meters
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket selectNear(String field, JSONArray value,
          Double maxDistance) throws ClientException {
    return getCommand(CommandType.SelectNear, FIELD, field, NEAR,
            value.toString(), MAX_DISTANCE, Double.toString(maxDistance));
  }

  /**
   * Update the location field for {@code targetGUID}.
   *
   * @param targetGUID
   * The GUID being queried.
   * @param longitude
   * the longitude
   * @param latitude
   * the latitude
   * @param querierGUID
   * The GUID issuing the query.
   * @return CommandPacket
   * @throws ClientException
   * if a GNS error occurs
   */
  public static final CommandPacket setLocation(String targetGUID,
          double longitude, double latitude, GuidEntry querierGUID)
          throws ClientException {
    return fieldReplaceOrCreateList(targetGUID, LOCATION_FIELD_NAME,
            new JSONArray(Arrays.asList(longitude, latitude)), querierGUID);
  }

  /**
   * Update the location field for {@code targetGUID}.
   *
   * @param longitude
   * the GUID longitude
   * @param latitude
   * the GUID latitude
   * @param targetGUID
   * the GUID to update
   * @return CommandPacket
   * @throws ClientException
   * if a GNS error occurs
   */
  public static final CommandPacket setLocation(GuidEntry targetGUID,
          double longitude, double latitude) throws ClientException {
    return setLocation(targetGUID.getGuid(), longitude, latitude,
            targetGUID);
  }

  /**
   * Get the location of {@code targetGUID} as a JSONArray: [LONG, LAT]
   *
   * @param querierGUID
   * the GUID issuing the request
   * @param targetGUID
   * the GUID that we want to know the location
   * @return a JSONArray: [LONGITUDE, LATITUDE]
   * @throws ClientException
   * if a GNS error occurs
   */
  public static final CommandPacket getLocation(String targetGUID,
          GuidEntry querierGUID) throws ClientException {
    return fieldReadArray(targetGUID, LOCATION_FIELD_NAME, querierGUID);
  }

  /**
   * Get the location of {@code targetGUID} as a JSONArray: [LONG, LAT]
   *
   * @param targetGUID
   * The GUID being queried.
   * @return a JSONArray: [LONGITUDE, LATITUDE]
   * @throws ClientException
   * if a GNS error occurs
   */
  public static final CommandPacket getLocation(GuidEntry targetGUID)
          throws ClientException {
    return fieldReadArray(targetGUID.getGuid(), LOCATION_FIELD_NAME,
            targetGUID);
  }

  /* ******************* Active Code ********************** */
  /**
   * @param targetGUID
   * The GUID being queried.
   * @param action
   * The action corresponding to the active code.
   * @param querierGUID
   * The GUID issuing the query.
   * @return CommandPacket
   * @throws ClientException
   * @throws IOException
   */
  public static final CommandPacket activeCodeClear(String targetGUID,
          String action, GuidEntry querierGUID) throws ClientException,
          IOException {
    return getCommand(CommandType.ClearCode, querierGUID, GUID,
            targetGUID, AC_ACTION, action, WRITER, querierGUID.getGuid());
  }

  /**
   * @param targetGUID
   * The GUID being queried (updated).
   * @param action
   * The action triggering the active code.
   * @param code
   * The active code being installed.
   * @param querierGUID
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket activeCodeSet(String targetGUID,
          String action, byte[] code, GuidEntry querierGUID)
          throws ClientException {
    return getCommand(CommandType.SetCode, querierGUID, GUID,
            targetGUID, AC_ACTION, action, AC_CODE,
            Base64.encodeToString(code, true), WRITER,
            querierGUID.getGuid());
  }

  /**
   * @param targetGUID
   * The GUID being queried.
   * @param action
   * The action triggering the active code.
   * @param querierGUID
   * The GUID issuing the query.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket activeCodeGet(String targetGUID,
          String action, GuidEntry querierGUID) throws ClientException {
    return getCommand(CommandType.GetCode, querierGUID, GUID,
            targetGUID, AC_ACTION, action, READER, querierGUID.getGuid());
  }

  /* ********************* More extended commands ********************** */
  /**
   * Creates a new field in {@code targetGUID} with the list {@code list}.
   *
   * @param targetGUID
   * The GUID being queried (updated).
   * @param field
   * The field key.
   * @param list
   * The list value.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket fieldCreateList(GuidEntry targetGUID,
          String field, JSONArray list) throws ClientException {
    return fieldCreateList(targetGUID.getGuid(), field, list, targetGUID);
  }

  /**
   * Creates a new one-element field {@code targetGUID}:{@code field} with the
   * string {@code value}.
   *
   * @param targetGUID
   * The GUID being queried.
   * @param field
   * The field key.
   * @param value
   * The single-element value.
   * @param querierGUID
   * The GUID issuing the query.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket fieldCreateOneElementList(
          String targetGUID, String field, String value, GuidEntry querierGUID)
          throws ClientException {
    return getCommand(CommandType.Create, querierGUID, GUID, targetGUID,
            FIELD, field, VALUE, value, WRITER, querierGUID.getGuid());
  }

  /**
   * Same as
   * {@link #fieldCreateOneElementList(String, String, String, GuidEntry)}
   * with {@code querierGUID} same as {@code targetGUID}.
   *
   * @param targetGUID
   * The GUID being queried (updated).
   * @param field
   * The field key.
   * @param value
   * The single-element value.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket fieldCreateOneElementList(
          GuidEntry targetGUID, String field, String value)
          throws ClientException {
    return fieldCreateOneElementList(targetGUID.getGuid(), field, value,
            targetGUID);
  }

  /**
   * Appends the single-element string {@code value} to {@code targetGUID}:
   * {@code field} or creates a new field with the single-value list if it
   * does not exist.
   *
   * @param targetGUID
   * The GUID being queried.
   * @param field
   * The field key.
   * @param value
   * The single-element value.
   * @param querierGUID
   * The GUID issuing the query.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket fieldAppendOrCreate(String targetGUID,
          String field, String value, GuidEntry querierGUID)
          throws ClientException {
    return getCommand(CommandType.AppendOrCreate, querierGUID, GUID,
            targetGUID, FIELD, field, VALUE, value, WRITER,
            querierGUID.getGuid());
  }

  /**
   * Replaces the value(s) of {@code targetGUID}:{@code field} with the single
   * value {@code value} or creates a new field with a single-element list if
   * it does not exist.
   *
   * @param targetGUID
   * The GUID being queried.
   * @param field
   * The field key.
   * @param value
   * The single-element value.
   * @param querierGUID
   * The GUID issuing the query.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket fieldReplaceOrCreate(String targetGUID,
          String field, String value, GuidEntry querierGUID)
          throws ClientException {
    return getCommand(CommandType.ReplaceOrCreate, querierGUID, GUID,
            targetGUID, FIELD, field, VALUE, value, WRITER,
            querierGUID.getGuid());
  }

  /**
   * Replaces the value(s) of {@code targetGUID}:{@code field} with the list
   * {@code value} or creates a new field with the list {@code value} if it
   * does not exist.
   *
   * @param targetGUID
   * The GUID being queried.
   * @param field
   * The field key.
   * @param value
   * The list value.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket fieldReplaceOrCreateList(
          GuidEntry targetGUID, String field, JSONArray value)
          throws ClientException {
    return fieldReplaceOrCreateList(targetGUID.getGuid(), field, value,
            targetGUID);
  }

  /**
   * Replaces the value(s) of {@code targetGUID}:{@code field} with the
   * single-element string {@code value} or creates a new field with a single
   * value list if it does not exist.
   *
   * @param targetGUID
   * @param field
   * The field key.
   * @param value
   * The single-element string value.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket fieldReplaceOrCreateList(
          GuidEntry targetGUID, String field, String value)
          throws ClientException {
    return fieldReplaceOrCreate(targetGUID.getGuid(), field, value,
            targetGUID);
  }

  /**
   * Replaces all the values of field with the single value. If the writer is
   * different use addToACL first to allow other the guid to write this field.
   *
   * @param targetGUID
   * The GUID being queried (updated).
   * @param field
   * The field key.
   * @param value
   * The single-element string value.
   * @param querierGUID
   * The GUID issuing the query.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket fieldReplace(String targetGUID,
          String field, String value, GuidEntry querierGUID)
          throws ClientException {
    return getCommand(CommandType.Replace, querierGUID, GUID, targetGUID,
            FIELD, field, VALUE, value, WRITER, querierGUID.getGuid());
  }

  /**
   * Replaces {@code targetGUID}:{@code field} with the single-element string
   * {@code value}.
   *
   * @param targetGUID
   * The GUID being queried (updated).
   * @param field
   * The field key.
   * @param value
   * The single-element string value.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket fieldReplace(GuidEntry targetGUID,
          String field, String value) throws ClientException {
    return fieldReplace(targetGUID.getGuid(), field, value, targetGUID);
  }

  /**
   * Replaces {@code targetGUID}:{@code field} with the list {@code value}.
   *
   * @param targetGUID
   * @param field
   * The field key.
   * @param value
   * The list value.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket fieldReplace(GuidEntry targetGUID,
          String field, JSONArray value) throws ClientException {
    return fieldReplaceList(targetGUID.getGuid(), field, value, targetGUID);
  }

  /**
   * Appends a single-element string {@code value} to {@code targetGUID}:
   * {@code field}.
   *
   * @param targetGUID
   * The GUID being queried (updated).
   * @param field
   * The field key.
   * @param value
   * The single-element string value.
   * @param querierGUID
   * The GUID issuing the query.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket fieldAppend(String targetGUID,
          String field, String value, GuidEntry querierGUID)
          throws ClientException {
    return getCommand(CommandType.AppendWithDuplication, querierGUID, GUID,
            targetGUID, FIELD, field, VALUE, value, WRITER,
            querierGUID.getGuid());
  }

  /**
   * Appends a single-element string {@code value} to {@code targetGUID}:
   * {@code field}.
   *
   * @param targetGUID
   * The GUID being queried (updated).
   * @param field
   * The field key.
   * @param value
   * The single-element string value.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket fieldAppend(GuidEntry targetGUID,
          String field, String value) throws ClientException {
    return fieldAppend(targetGUID.getGuid(), field, value, targetGUID);
  }

  /**
   * Appends a list {@code value} to {@code targetGUID}{:{@code field} but
   * after first converting the list to a set (removing duplicates).
   *
   *
   * @param targetGUID
   * The GUID being queried (updated).
   * @param field
   * The field key.
   * @param value
   * The list value.
   * @param querierGUID
   * The GUID issuing the query.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket fieldAppendWithSetSemantics(
          String targetGUID, String field, JSONArray value,
          GuidEntry querierGUID) throws ClientException {
    return getCommand(CommandType.AppendList, querierGUID, GUID,
            targetGUID, FIELD, field, VALUE, value.toString(), WRITER,
            querierGUID.getGuid());
  }

  /**
   * Appends a list {@code value} onto a {@code targetGUID}:{@code field} but
   * first converts the list to a set (removing duplicates).
   *
   * @param targetGUID
   * @param field
   * @param value
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket fieldAppendWithSetSemantics(
          GuidEntry targetGUID, String field, JSONArray value)
          throws ClientException {
    return fieldAppendWithSetSemantics(targetGUID.getGuid(), field, value,
            targetGUID);
  }

  /**
   * Appends a single-element string {@code value} to {@code targetGUID}:
   * {@code field} but first converts the list to set (removing duplicates).
   *
   * @param targetGUID
   * The GUID being queried (updated).
   * @param field
   * The field key.
   * @param value
   * The single-element string value.
   * @param querierGUID
   * The GUID issuing the query.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket fieldAppendWithSetSemantics(
          String targetGUID, String field, String value, GuidEntry querierGUID)
          throws ClientException {
    return getCommand(CommandType.Append, querierGUID, GUID, targetGUID,
            FIELD, field, VALUE, value.toString(), WRITER,
            querierGUID.getGuid());
  }

  /**
   * Appends a single-element string value to {@code targetGUID}:{@code field}
   * but first converts the list to a set (removing duplicates).
   *
   * @param targetGUID
   * @param field
   * @param value
   * @return CommandPacket
   * @throws IOException
   * @throws ClientException
   */
  public static final CommandPacket fieldAppendWithSetSemantics(
          GuidEntry targetGUID, String field, String value)
          throws IOException, ClientException {
    return fieldAppendWithSetSemantics(targetGUID.getGuid(), field, value,
            targetGUID);
  }

  /**
   * Substitutes {@code targetGUID}:{@code field}'s value with
   * {@code newValue} if the current value is {@code oldValue}.
   *
   * @param targetGUID
   * The GUID being queried (updated).
   * @param field
   * The field key.
   * @param newValue
   * The new value.
   * @param oldValue
   * The value being substituted.
   * @param querierGUID
   * The GUID issuing the query.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket fieldSubstitute(String targetGUID,
          String field, String newValue, String oldValue,
          GuidEntry querierGUID) throws ClientException {
    return getCommand(CommandType.Substitute, querierGUID, GUID,
            targetGUID, FIELD, field, VALUE, newValue, OLD_VALUE, oldValue,
            WRITER, querierGUID.getGuid());
  }

  /**
   * Same as {@link #fieldSubstitute(GuidEntry, String, String, String)} with
   * {@code querierGUID} same as {@code targetGUID}.
   *
   * @param targetGUID
   * @param field
   * @param newValue
   * @param oldValue
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket fieldSubstitute(GuidEntry targetGUID,
          String field, String newValue, String oldValue)
          throws ClientException {
    return fieldSubstitute(targetGUID.getGuid(), field, newValue, oldValue,
            targetGUID);
  }

  /**
   * Pairwise-substitutes all the values in {@code oldValues} with
   * {@code newValues} in the list {@code targetGUID}:{@code field}.
   *
   *
   * @param targetGUID
   * The GUID being queried (updated).
   * @param field
   * The field key.
   * @param newValue
   * The list of new values.
   * @param oldValue
   * The list of old values being substituted.
   * @param querierGUID
   * The GUID issuing the query.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket fieldSubstitute(String targetGUID,
          String field, JSONArray newValue, JSONArray oldValue,
          GuidEntry querierGUID) throws ClientException {
    return getCommand(CommandType.SubstituteList, querierGUID, GUID,
            targetGUID, FIELD, field, VALUE, newValue.toString(),
            OLD_VALUE, oldValue.toString(), WRITER, querierGUID.getGuid());
  }

  /**
   * Same as
   * {@link #fieldSubstitute(String, String, JSONArray, JSONArray, GuidEntry)}
   * with {@code querierGUID} same as {@code targetGUID}.
   *
   * @param targetGUID
   * The GUID being queried.
   * @param field
   * The field key.
   * @param newValue
   * The list of new values.
   * @param oldValue
   * The list of old values.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket fieldSubstitute(GuidEntry targetGUID,
          String field, JSONArray newValue, JSONArray oldValue)
          throws ClientException {
    return fieldSubstitute(targetGUID.getGuid(), field, newValue, oldValue,
            targetGUID);
  }

  /**
   * Removes the field {@code targetGUID}:{@code field}.
   *
   * @param targetGUID
   * The GUID being queried.
   * @param field
   * The field key.
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket fieldRemove(GuidEntry targetGUID,
          String field) throws ClientException {
    return fieldRemove(targetGUID.getGuid(), field, targetGUID);
  }

  /**
   * @param passkey
   * @return CommandPacket
   * @throws ClientException
   */
  public static final CommandPacket adminEnable(String passkey)
          throws ClientException {
    return getCommand(CommandType.Admin, PASSKEY, passkey);
  }

  /**
   * @return The {@link GNSCommand.ResultType} type of the result obtained by
   * executing this query.
   */
  public ResultType getResultType() {
    return this.getCommandType().getResultType();
  }
}
