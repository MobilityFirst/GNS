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

import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.client.util.Password;
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnscommon.CommandType;
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
 *         A helper class with static methods to help construct GNS commands.
 */
public class GNSCommand {

	/**
	 * Constructs the command.
	 * 
	 * @param type
	 * @param querier
	 * @param keysAndValues
	 * @return Constructed CommandPacket
	 * @throws ClientException
	 */
	public static CommandPacket getCommand(CommandType type, GuidEntry querier,
			Object... keysAndValues) throws ClientException {
		CommandPacket packet = new CommandPacket(
		/* arun: we just generate a random value here because it is not possible
		 * to guarantee non-conflicting IDs here. */
		randomLong(), CommandUtils.createAndSignCommand(type, querier,
				keysAndValues));
		return packet;
	}

	/**
	 * @param type
	 * @param keysAndValues
	 * @return CommandPacket
	 * @throws ClientException
	 */
	public static CommandPacket getCommand(CommandType type,
			Object... keysAndValues) throws ClientException {
		return getCommand(type, null, keysAndValues);
	}

	/* We just use a random long here as we will get an IOException if there is
	 * a conflicting request ID, i.e., if another unequal request is awaiting a
	 * response in the async client. Equal requests are considered as
	 * retransmissions. */
	private static long randomLong() {
		return (long) (Math.random() * Long.MAX_VALUE);
	}

	/**
	 * @param targetGuid
	 * @param json
	 * @param writer
	 * @return Refer {@link #update(String, JSONObject, GuidEntry)}
	 * @throws IOException
	 * @throws ClientException
	 */
	public static final CommandPacket update(String targetGuid,
			JSONObject json, GuidEntry writer) throws IOException,
			ClientException {
		return getCommand(CommandType.ReplaceUserJSON, writer, GUID,
				targetGuid, USER_JSON, json.toString(), WRITER,
				writer.getGuid());
	}

	/**
	 * @param guid
	 * @param json
	 * @return Refer {@link #update(String, JSONObject, GuidEntry)}
	 * @throws IOException
	 * @throws ClientException
	 */
	public static final CommandPacket update(GuidEntry guid, JSONObject json)
			throws IOException, ClientException {
		return update(guid.getGuid(), json, guid);
	}

	/**
	 * @param targetGuid
	 * @param field
	 * @param value
	 * @param writer
	 * @return Refer
	 *         {@link GNSClientCommands#fieldUpdate(String, String, Object, GuidEntry)}
	 *         .
	 * @throws IOException
	 * @throws ClientException
	 * @throws JSONException
	 */
	public static final CommandPacket fieldUpdate(String targetGuid,
			String field, Object value, GuidEntry writer) throws IOException,
			ClientException, JSONException {
		return getCommand(CommandType.ReplaceUserJSON, writer, GUID,
				targetGuid, USER_JSON, new JSONObject().put(field, value)
						.toString(), WRITER, writer.getGuid());
	}

	/**
	 * Creates an index for a field. The guid is only used for authentication
	 * purposes.
	 *
	 * @param guid
	 * @param field
	 * @param index
	 * @return
	 * @throws IOException
	 * @throws ClientException
	 * @throws JSONException
	 */
	public static final CommandPacket fieldCreateIndex(GuidEntry guid,
			String field, String index) throws IOException, ClientException,
			JSONException {
		return getCommand(CommandType.CreateIndex, guid, GUID, guid.getGuid(),
				FIELD, field, VALUE, index, WRITER, guid.getGuid());
	}

	/**
	 * Updates the field in the targetGuid. Signs the query using the private
	 * key of the given guid.
	 *
	 * @param targetGuid
	 * @param field
	 * @param value
	 * @return
	 * @throws IOException
	 * @throws ClientException
	 * @throws JSONException
	 */
	public static final CommandPacket fieldUpdate(GuidEntry targetGuid,
			String field, Object value) throws IOException, ClientException,
			JSONException {
		return fieldUpdate(targetGuid.getGuid(), field, value, targetGuid);
	}

	/**
	 * Reads the JSONObject for the given targetGuid. The reader is the guid of
	 * the user attempting access. Signs the query using the private key of the
	 * user associated with the reader guid (unsigned if reader is null).
	 *
	 * @param targetGuid
	 * @param reader
	 *            if null guid must be all fields readable for all users
	 * @return a JSONObject
	 * @throws Exception
	 */
	public static final CommandPacket read(String targetGuid, GuidEntry reader)
			throws Exception {
		return getCommand(reader != null ? CommandType.ReadArray
				: CommandType.ReadArrayUnsigned, reader, GUID, targetGuid,
				FIELD, ALL_FIELDS, READER, reader != null ? reader.getGuid()
						: null);
	}

	/**
	 * Reads the entire record from the GNS server for the given guid. Signs the
	 * query using the private key of the guid.
	 *
	 * @param guid
	 * @return a JSONArray containing the values in the field
	 * @throws Exception
	 */
	public static final CommandPacket read(GuidEntry guid) throws Exception {
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
	 *            if null the field must be readable for all
	 * @return a boolean indicating if the field exists
	 * @throws Exception
	 */
	public static final CommandPacket fieldExists(String targetGuid,
			String field, GuidEntry reader) throws Exception {
		return getCommand(reader != null ? CommandType.Read
				: CommandType.ReadUnsigned, reader, GUID, targetGuid, FIELD,
				field, READER, reader != null ? reader.getGuid() : null);
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
	public static final CommandPacket fieldExists(GuidEntry targetGuid,
			String field) throws Exception {
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
	 *            if null the field must be readable for all
	 * @return a string containing the values in the field
	 * @throws Exception
	 */
	public static final CommandPacket fieldRead(String targetGuid,
			String field, GuidEntry reader) throws Exception {
		return getCommand(reader != null ? CommandType.Read
				: CommandType.ReadUnsigned, reader, GUID, targetGuid, FIELD,
				field, READER, reader != null ? reader.getGuid() : null);
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
	public static final CommandPacket fieldRead(GuidEntry targetGuid,
			String field) throws Exception {
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
	 *            if null the field must be readable for all
	 * @return a JSONArray containing the values in the fields
	 * @throws Exception
	 */
	public static final CommandPacket fieldRead(String targetGuid,
			ArrayList<String> fields, GuidEntry reader) throws Exception {
		return getCommand(reader != null ? CommandType.ReadMultiField
				: CommandType.ReadMultiFieldUnsigned, reader, GUID, targetGuid,
				FIELDS, fields, READER, reader != null ? reader.getGuid()
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
	public static final CommandPacket fieldRead(GuidEntry targetGuid,
			ArrayList<String> fields) throws Exception {
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
	 * @return
	 * @throws IOException
	 * @throws InvalidKeyException
	 * @throws NoSuchAlgorithmException
	 * @throws SignatureException
	 * @throws ClientException
	 */
	public static final CommandPacket fieldRemove(String targetGuid,
			String field, GuidEntry writer) throws IOException,
			InvalidKeyException, NoSuchAlgorithmException, SignatureException,
			ClientException {
		return getCommand(CommandType.RemoveField, writer, GUID, targetGuid,
				FIELD, field, WRITER, writer.getGuid());
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
	 * in edu.umass.cs.gnsclient.client.GNSCommandProtocol): LOCATION_FIELD_NAME
	 * = "geoLocation"; Defined as a "2d" index in the database.
	 * IPADDRESS_FIELD_NAME = "netAddress";
	 *
	 * There are links in the wiki page abive to find the exact syntax for
	 * querying spacial coordinates.
	 *
	 * @param query
	 *            - the query
	 * @return - a JSONArray of guids
	 * @throws Exception
	 */
	public static final CommandPacket selectQuery(String query)
			throws Exception {

		return getCommand(CommandType.SelectQuery, QUERY, query);
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
	 *            the query
	 * @param interval
	 *            - the refresh interval in seconds - default is 60 - (queries
	 *            that happens quicker than this will get stale results)
	 * @return a JSONArray of guids
	 * @throws Exception
	 */
	public static final CommandPacket selectSetupGroupQuery(
			GuidEntry accountGuid, String publicKey, String query, int interval)
			throws Exception {
		return getCommand(CommandType.SelectGroupSetupQuery, GUID,
				accountGuid.getGuid(), PUBLIC_KEY, publicKey, QUERY, query,
				INTERVAL, interval);
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
	public static final CommandPacket selectLookupGroupQuery(String guid)
			throws Exception {
		return getCommand(CommandType.SelectGroupLookupQuery, GUID, guid);
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
	public static final CommandPacket lookupGuid(String alias)
			throws IOException, ClientException {

		return getCommand(CommandType.LookupGuid, NAME, alias);
	}

	/**
	 * If this is a sub guid returns the account guid it was created under.
	 *
	 * @param guid
	 * @return Account GUID of {@code guid}
	 * @throws UnsupportedEncodingException
	 * @throws IOException
	 * @throws ClientException
	 */
	public static final CommandPacket lookupPrimaryGuid(String guid)
			throws UnsupportedEncodingException, IOException, ClientException {
		return getCommand(CommandType.LookupPrimaryGuid, GUID, guid);
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
	public static final CommandPacket lookupGuidRecord(String guid)
			throws IOException, ClientException {
		return (getCommand(CommandType.LookupGuidRecord, GUID, guid));
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
	public static final CommandPacket lookupAccountRecord(String accountGuid)
			throws IOException, ClientException {
		return getCommand(CommandType.LookupAccountRecord, GUID, accountGuid);
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

		throw new RuntimeException("Unimplementable");
		// String guid = lookupGuid(alias);
		// return publicKeyLookupFromGuid(guid);
	}

	/**
	 * Get the public key for a given GUID.
	 *
	 * @param guid
	 * @return Public key for {@code guid}
	 * @throws InvalidGuidException
	 * @throws ClientException
	 * @throws IOException
	 */
	public static final CommandPacket publicKeyLookupFromGuid(String guid)
			throws InvalidGuidException, ClientException, IOException {

		return lookupGuidRecord(guid);
	}

	/**
	 * Register a new account guid with the corresponding alias on the GNS
	 * server. This generates a new guid and a public / private key pair.
	 * Returns a GuidEntry for the new account which contains all of this
	 * information.
	 * 
	 * @param gnsInstance
	 *
	 * @param alias
	 *            - a human readable alias to the guid - usually an email
	 *            address
	 * @param password
	 * @return GuidEntry for {@code alias}
	 * @throws Exception
	 */
	public static final CommandPacket accountGuidCreate(String gnsInstance,
			String alias, String password) throws Exception {
		KeyPair keyPair = KeyPairGenerator.getInstance(RSA_ALGORITHM)
				.generateKeyPair();
		String guid = SharedGuidUtils.createGuidStringFromPublicKey(keyPair
				.getPublic().getEncoded());
		// Squirrel this away now just in case the call below times out.
		KeyPairUtils.saveKeyPair(gnsInstance, alias, guid, keyPair);
		GuidEntry entry = new GuidEntry(alias, guid, keyPair.getPublic(),
				keyPair.getPrivate());
		return accountGuidCreateHelper(alias, entry, password);
	}

	/**
	 * Verify an account by sending the verification code back to the server.
	 *
	 * @param guid
	 *            the account GUID to verify
	 * @param code
	 *            the verification code
	 * @return ?
	 * @throws Exception
	 */
	public static final CommandPacket accountGuidVerify(GuidEntry guid,
			String code) throws Exception {
		return getCommand(CommandType.VerifyAccount, guid, GUID,
				guid.getGuid(), CODE, code);
	}

	/**
	 * Deletes the account given by name
	 *
	 * @param guid
	 *            GuidEntry
	 * @return
	 * @throws Exception
	 */
	public static final CommandPacket accountGuidRemove(GuidEntry guid)
			throws Exception {
		return getCommand(CommandType.RemoveAccount, guid, GUID,
				guid.getGuid(), NAME, guid.getEntityName());
	}

	/**
	 * Creates an new GUID associated with an account on the GNS server.
	 * 
	 * @param gnsInstance
	 *
	 * @param accountGuid
	 * @param alias
	 *            the alias
	 * @return the newly created GUID entry
	 * @throws Exception
	 */
	public static final CommandPacket guidCreate(String gnsInstance,
			GuidEntry accountGuid, String alias) throws Exception {

		GuidEntry entry = GuidUtils.createAndSaveGuidEntry(alias, gnsInstance);
		return guidCreateHelper(accountGuid, alias, entry.getPublicKey());
	}

	/**
	 * Batch create guids with the given aliases. If createPublicKeys is true,
	 * key pairs will be created and saved by the client for the guids. If not,
	 * bogus public keys will be uses which will make the guids only accessible
	 * using the account guid (which has ACL access to each guid).
	 * 
	 * @param gnsInstance
	 *
	 * @param accountGuid
	 * @param aliases
	 * @return ???
	 * @throws Exception
	 */
	public static final CommandPacket guidBatchCreate(String gnsInstance,
			GuidEntry accountGuid, Set<String> aliases) throws Exception {

		List<String> aliasList = new ArrayList<>(aliases);
		List<String> publicKeys = null;
		long publicKeyStartTime = System.currentTimeMillis();
		publicKeys = new ArrayList<>();
		for (String alias : aliasList) {
			long singleEntrystartTime = System.currentTimeMillis();
			GuidEntry entry = GuidUtils.createAndSaveGuidEntry(alias,
					gnsInstance);
			DelayProfiler.updateDelay("updateOnePreference",
					singleEntrystartTime);
			byte[] publicKeyBytes = entry.getPublicKey().getEncoded();
			String publicKeyString = Base64.encodeToString(publicKeyBytes,
					false);
			publicKeys.add(publicKeyString);
		}
		DelayProfiler.updateDelay("batchCreatePublicKeys", publicKeyStartTime);

		return getCommand(CommandType.AddMultipleGuids, accountGuid, GUID,
				accountGuid.getGuid(), NAMES, new JSONArray(aliasList),
				PUBLIC_KEYS, new JSONArray(publicKeys));
	}

	/**
	 * Removes a guid (not for account Guids - use removeAccountGuid for them).
	 *
	 * @param guid
	 *            the guid to remove
	 * @throws Exception
	 */
	public static final CommandPacket guidRemove(GuidEntry guid)
			throws Exception {
		return getCommand(CommandType.RemoveGuidNoAccount, guid, GUID,
				guid.getGuid());
	}

	/**
	 * Removes a guid given the guid and the associated account guid.
	 *
	 * @param accountGuid
	 * @param guidToRemove
	 * @throws Exception
	 */
	public static final CommandPacket guidRemove(GuidEntry accountGuid,
			String guidToRemove) throws Exception {
		return getCommand(CommandType.RemoveGuid, accountGuid, ACCOUNT_GUID,
				accountGuid.getGuid(), GUID, guidToRemove);
	}

	// GROUP COMMANDS
	/**
	 * Return the list of guids that are members of the group. Signs the query
	 * using the private key of the user associated with the guid.
	 *
	 * @param groupGuid
	 *            the guid of the group to lookup
	 * @param reader
	 *            the guid of the entity doing the lookup
	 * @return the list of guids as a JSONArray
	 * @throws IOException
	 *             if a communication error occurs
	 * @throws ClientException
	 *             if a protocol error occurs or the list cannot be parsed
	 * @throws InvalidGuidException
	 *             if the group guid is invalid
	 */
	public static final CommandPacket groupGetMembers(String groupGuid,
			GuidEntry reader) throws IOException, ClientException,
			InvalidGuidException {
		return getCommand(CommandType.GetGroupMembers, reader, GUID, groupGuid,
				READER, reader.getGuid());
	}

	/**
	 * Return a list of the groups that the guid is a member of. Signs the query
	 * using the private key of the user associated with the guid.
	 *
	 * @param guid
	 *            the guid we are looking for
	 * @param reader
	 *            the guid of the entity doing the lookup
	 * @return the list of groups as a JSONArray
	 * @throws IOException
	 *             if a communication error occurs
	 * @throws ClientException
	 *             if a protocol error occurs or the list cannot be parsed
	 * @throws InvalidGuidException
	 *             if the group guid is invalid
	 */
	public static final CommandPacket guidGetGroups(String guid,
			GuidEntry reader) throws IOException, ClientException,
			InvalidGuidException {

		return getCommand(CommandType.GetGroups, reader, GUID, guid, READER,
				reader.getGuid());
	}

	/**
	 * Add a guid to a group guid. Any guid can be a group guid. Signs the query
	 * using the private key of the user associated with the writer.
	 *
	 * @param groupGuid
	 *            guid of the group
	 * @param guidToAdd
	 *            guid to add to the group
	 * @param writer
	 *            the guid doing the add
	 * @throws IOException
	 * @throws InvalidGuidException
	 *             if the group guid does not exist
	 * @throws ClientException
	 */
	public static final CommandPacket groupAddGuid(String groupGuid,
			String guidToAdd, GuidEntry writer) throws IOException,
			InvalidGuidException, ClientException {
		return getCommand(CommandType.AddToGroup, writer, GUID, groupGuid,
				MEMBER, guidToAdd, WRITER, writer.getGuid());
	}

	/**
	 * Add multiple members to a group
	 *
	 * @param groupGuid
	 *            guid of the group
	 * @param members
	 *            guids of members to add to the group
	 * @param writer
	 *            the guid doing the add
	 * @throws IOException
	 * @throws InvalidGuidException
	 * @throws ClientException
	 * @throws InvalidKeyException
	 * @throws NoSuchAlgorithmException
	 * @throws SignatureException
	 */
	public static final CommandPacket groupAddGuids(String groupGuid,
			JSONArray members, GuidEntry writer) throws IOException,
			InvalidGuidException, ClientException, InvalidKeyException,
			NoSuchAlgorithmException, SignatureException {
		return getCommand(CommandType.AddMembersToGroup, writer, GUID,
				groupGuid, MEMBERS, members.toString(), WRITER,
				writer.getGuid());
	}

	/**
	 * Removes a guid from a group guid. Any guid can be a group guid. Signs the
	 * query using the private key of the user associated with the writer.
	 *
	 * @param guid
	 *            guid of the group
	 * @param guidToRemove
	 *            guid to remove from the group
	 * @param writer
	 *            the guid of the entity doing the remove
	 * @throws IOException
	 * @throws InvalidGuidException
	 *             if the group guid does not exist
	 * @throws ClientException
	 */
	public static final CommandPacket groupRemoveGuid(String guid,
			String guidToRemove, GuidEntry writer) throws IOException,
			InvalidGuidException, ClientException {
		return getCommand(CommandType.RemoveFromGroup, writer, GUID, guid,
				MEMBER, guidToRemove, WRITER, writer.getGuid());
	}

	/**
	 * Remove a list of members from a group
	 *
	 * @param guid
	 *            guid of the group
	 * @param members
	 *            guids to remove from the group
	 * @param writer
	 *            the guid of the entity doing the remove
	 * @throws IOException
	 * @throws InvalidGuidException
	 * @throws ClientException
	 * @throws InvalidKeyException
	 * @throws NoSuchAlgorithmException
	 * @throws SignatureException
	 */
	public static final CommandPacket groupRemoveGuids(String guid,
			JSONArray members, GuidEntry writer) throws IOException,
			InvalidGuidException, ClientException, InvalidKeyException,
			NoSuchAlgorithmException, SignatureException {
		return getCommand(CommandType.RemoveMembersFromGroup, writer, GUID,
				guid, MEMBERS, members.toString(), WRITER, writer.getGuid());
	}

	/**
	 * Authorize guidToAuthorize to add/remove members from the group groupGuid.
	 * If guidToAuthorize is null, everyone is authorized to add/remove members
	 * to the group. Note that this method can only be called by the group owner
	 * (private key required) Signs the query using the private key of the group
	 * owner.
	 *
	 * @param groupGuid
	 *            the group GUID entry
	 * @param guidToAuthorize
	 *            the guid to authorize to manipulate group membership or null
	 *            for anyone
	 * @throws Exception
	 */
	public static final CommandPacket groupAddMembershipUpdatePermission(
			GuidEntry groupGuid, String guidToAuthorize) throws Exception {
		return aclAdd(AclAccessType.WRITE_WHITELIST, groupGuid, GROUP_ACL,
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
	 *            the group GUID entry
	 * @param guidToUnauthorize
	 *            the guid to authorize to manipulate group membership or null
	 *            for anyone
	 * @throws Exception
	 */
	public static final CommandPacket groupRemoveMembershipUpdatePermission(
			GuidEntry groupGuid, String guidToUnauthorize) throws Exception {
		return aclRemove(AclAccessType.WRITE_WHITELIST, groupGuid, GROUP_ACL,
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
	 *            the group GUID entry
	 * @param guidToAuthorize
	 *            the guid to authorize to manipulate group membership or null
	 *            for anyone
	 * @throws Exception
	 */
	public static final CommandPacket groupAddMembershipReadPermission(
			GuidEntry groupGuid, String guidToAuthorize) throws Exception {
		return aclAdd(AclAccessType.READ_WHITELIST, groupGuid, GROUP_ACL,
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
	 *            the group GUID entry
	 * @param guidToUnauthorize
	 *            the guid to authorize to manipulate group membership or null
	 *            for anyone
	 * @throws Exception
	 */
	public static final CommandPacket groupRemoveMembershipReadPermission(
			GuidEntry groupGuid, String guidToUnauthorize) throws Exception {
		return aclRemove(AclAccessType.READ_WHITELIST, groupGuid, GROUP_ACL,
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
	 *            a value from GnrsProtocol.AclAccessType
	 * @param targetGuid
	 *            guid of the field to be modified
	 * @param field
	 *            field name
	 * @param accesserGuid
	 *            guid to add to the ACL
	 * @throws Exception
	 * @throws ClientException
	 *             if the query is not accepted by the server.
	 */
	public static final CommandPacket aclAdd(AclAccessType accessType,
			GuidEntry targetGuid, String field, String accesserGuid)
			throws Exception {
		return aclAdd(accessType.name(), targetGuid, field, accesserGuid);
	}

	/**
	 * Removes a GUID from an access control list of the given user's field on
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
	 *             if the query is not accepted by the server.
	 */
	public static final CommandPacket aclRemove(AclAccessType accessType,
			GuidEntry guid, String field, String accesserGuid) throws Exception {
		return aclRemove(accessType.name(), guid, field, accesserGuid);
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
	 * @param accesserGuid
	 * @return list of GUIDs for that ACL
	 * @throws Exception
	 * @throws ClientException
	 *             if the query is not accepted by the server.
	 */
	public static final CommandPacket aclGet(AclAccessType accessType,
			GuidEntry guid, String field, String accesserGuid) throws Exception {
		return aclGet(accessType.name(), guid, field, accesserGuid);
	}

	// ALIASES
	/**
	 * Creates an alias entity name for the given guid. The alias can be used
	 * just like the original entity name.
	 *
	 * @param guid
	 * @param name
	 *            - the alias
	 * @throws Exception
	 */
	public static final CommandPacket addAlias(GuidEntry guid, String name)
			throws Exception {
		return getCommand(CommandType.AddAlias, guid, GUID, guid.getGuid(),
				NAME, name);
	}

	/**
	 * Removes the alias for the given guid.
	 *
	 * @param guid
	 * @param name
	 *            - the alias
	 * @throws Exception
	 */
	public static final CommandPacket removeAlias(GuidEntry guid, String name)
			throws Exception {
		return getCommand(CommandType.RemoveAlias, guid, GUID, guid.getGuid(),
				NAME, name);
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

	// TAGS
	/**
	 * Creates a tag to the tags of the guid.
	 *
	 * @param guid
	 * @param tag
	 * @throws Exception
	 */
	// @Override
	static final CommandPacket addTag(GuidEntry guid, String tag)
			throws Exception {
		return getCommand(CommandType.AddTag, guid, GUID, guid.getGuid(), NAME,
				tag);
	}

	/**
	 * Removes a tag from the tags of the guid.
	 *
	 * @param guid
	 * @param tag
	 * @throws Exception
	 */
	static final CommandPacket removeTag(GuidEntry guid, String tag)
			throws Exception {
		return getCommand(CommandType.RemoveTag, guid, GUID, guid.getGuid(),
				NAME, tag);
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
			String name, PublicKey publicKey) throws Exception {
		long startTime = System.currentTimeMillis();
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
	 * @param publicKey
	 *            the public key associate with the account
	 * @return guid the GUID generated by the GNS
	 * @throws IOException
	 * @throws UnsupportedEncodingException
	 * @throws ClientException
	 * @throws InvalidGuidException
	 *             if the user already exists
	 */
	private static final CommandPacket accountGuidCreateHelper(String alias,
			GuidEntry guidEntry, String password)
			throws UnsupportedEncodingException, IOException, ClientException,
			InvalidGuidException, NoSuchAlgorithmException {
		return password != null ? getCommand(CommandType.RegisterAccount,
				guidEntry, NAME, alias, PUBLIC_KEY, Base64.encodeToString(
						guidEntry.publicKey.getEncoded(), false), PASSWORD,
				Base64.encodeToString(
						Password.encryptPassword(password, alias), false))
				: getCommand(CommandType.RegisterAccountSansPassword,
						guidEntry.getPrivateKey(), guidEntry.publicKey, NAME,
						alias, PUBLIC_KEY, Base64.encodeToString(
								guidEntry.publicKey.getEncoded(), false));
	}

	private static final CommandPacket aclAdd(String accessType,
			GuidEntry guid, String field, String accesserGuid) throws Exception {
		return getCommand(CommandType.AclAddSelf, guid, ACL_TYPE, accessType,
				GUID, guid.getGuid(), FIELD, field, ACCESSER,
				accesserGuid == null ? ALL_GUIDS : accesserGuid);
	}

	private static final CommandPacket aclRemove(String accessType,
			GuidEntry guid, String field, String accesserGuid) throws Exception {
		return getCommand(CommandType.AclRemoveSelf, guid, ACL_TYPE,
				accessType, GUID, guid.getGuid(), FIELD, field, ACCESSER,
				accesserGuid == null ? ALL_GUIDS : accesserGuid);
	}

	private static final CommandPacket aclGet(String accessType,
			GuidEntry guid, String field, String readerGuid) throws Exception {
		return getCommand(CommandType.AclRetrieve, guid, ACL_TYPE, accessType,
				GUID, guid.getGuid(), FIELD, field, READER,
				readerGuid == null ? ALL_GUIDS : readerGuid);
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
	 * @return
	 * @throws IOException
	 * @throws ClientException
	 */
	public static final CommandPacket fieldCreateList(String targetGuid,
			String field, JSONArray value, GuidEntry writer)
			throws IOException, ClientException {
		return getCommand(CommandType.CreateList, writer, GUID, targetGuid,
				FIELD, field, VALUE, value.toString(), WRITER, writer.getGuid());
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
	public static final CommandPacket fieldAppendOrCreateList(
			String targetGuid, String field, JSONArray value, GuidEntry writer)
			throws IOException, ClientException {
		return getCommand(CommandType.AppendOrCreateList, writer, GUID,
				targetGuid, FIELD, field, VALUE, value.toString(), WRITER,
				writer.getGuid());
	}

	/**
	 * Replaces the values of the field with the list of values or creates a new
	 * field with values in the list if it does not exist.
	 *
	 * @param targetGuid
	 * @param field
	 * @param value
	 * @param writer
	 * @return
	 * @throws IOException
	 * @throws ClientException
	 */
	public static final CommandPacket fieldReplaceOrCreateList(
			String targetGuid, String field, JSONArray value, GuidEntry writer)
			throws IOException, ClientException {
		return getCommand(CommandType.ReplaceOrCreateList, writer, GUID,
				targetGuid, FIELD, field, VALUE, value.toString(), WRITER,
				writer.getGuid());
	}

	/**
	 * Appends a list of values onto a field.
	 *
	 * @param targetGuid
	 *            GUID where the field is stored
	 * @param field
	 *            field name
	 * @param value
	 *            list of values
	 * @param writer
	 *            GUID entry of the writer
	 * @return
	 * @throws IOException
	 * @throws ClientException
	 */
	public static final CommandPacket fieldAppend(String targetGuid,
			String field, JSONArray value, GuidEntry writer)
			throws IOException, ClientException {
		return getCommand(CommandType.AppendListWithDuplication, writer, GUID,
				targetGuid, FIELD, field, VALUE, value.toString(), WRITER,
				writer.getGuid());
	}

	/**
	 * Replaces all the values of field with the list of values.
	 *
	 * @param targetGuid
	 *            GUID where the field is stored
	 * @param field
	 *            field name
	 * @param value
	 *            list of values
	 * @param writer
	 *            GUID entry of the writer
	 * @return
	 * @throws IOException
	 * @throws ClientException
	 */
	public static final CommandPacket fieldReplaceList(String targetGuid,
			String field, JSONArray value, GuidEntry writer)
			throws IOException, ClientException {
		return getCommand(CommandType.ReplaceList, writer, GUID, targetGuid,
				FIELD, field, VALUE, value.toString(), WRITER, writer.getGuid());
	}

	/**
	 * Removes all the values in the list from the field.
	 *
	 * @param targetGuid
	 *            GUID where the field is stored
	 * @param field
	 *            field name
	 * @param value
	 *            list of values
	 * @param writer
	 *            GUID entry of the writer
	 * @return
	 * @throws IOException
	 * @throws ClientException
	 */
	public static final CommandPacket fieldClear(String targetGuid,
			String field, JSONArray value, GuidEntry writer)
			throws IOException, ClientException {
		return getCommand(CommandType.RemoveList, writer, GUID, targetGuid,
				FIELD, field, VALUE, value.toString(), WRITER, writer.getGuid());
	}

	/**
	 * Removes all values from the field.
	 *
	 * @param targetGuid
	 *            GUID where the field is stored
	 * @param field
	 *            field name
	 * @param writer
	 *            GUID entry of the writer
	 * @return
	 * @throws IOException
	 * @throws ClientException
	 */
	public static final CommandPacket fieldClear(String targetGuid,
			String field, GuidEntry writer) throws IOException, ClientException {
		return getCommand(CommandType.Clear, writer, GUID, targetGuid, FIELD,
				field, WRITER, writer.getGuid());
	}

	/**
	 * Reads all the values for a key from the GNS server for the given guid.
	 * The guid of the user attempting access is also needed. Signs the query
	 * using the private key of the user associated with the reader guid
	 * (unsigned if reader is null).
	 *
	 * @param guid
	 * @param field
	 * @param reader
	 *            if null the field must be readable for all
	 * @return a JSONArray containing the values in the field
	 * @throws Exception
	 */
	public static final CommandPacket fieldReadArray(String guid, String field,
			GuidEntry reader) throws Exception {
		return getCommand(reader != null ? CommandType.ReadArray
				: CommandType.ReadArrayUnsigned, reader, GUID, guid, FIELD,
				field, READER, reader != null ? reader.getGuid() : null);
	}

	/**
	 * Sets the nth value (zero-based) indicated by index in the list contained
	 * in field to newValue. Index must be less than the current size of the
	 * list.
	 *
	 * @param targetGuid
	 * @param field
	 * @param newValue
	 * @param index
	 * @param writer
	 * @return
	 * @throws IOException
	 * @throws ClientException
	 */
	public static final CommandPacket fieldSetElement(String targetGuid,
			String field, String newValue, int index, GuidEntry writer)
			throws IOException, ClientException {
		return getCommand(CommandType.Set, writer, GUID, targetGuid, FIELD,
				field, VALUE, newValue, N, Integer.toString(index), WRITER,
				writer.getGuid());
	}

	/**
	 * Sets a field to be null. That is when read field is called a null will be
	 * returned.
	 *
	 * @param targetGuid
	 * @param field
	 * @param writer
	 * @return
	 * @throws IOException
	 * @throws InvalidKeyException
	 * @throws NoSuchAlgorithmException
	 * @throws SignatureException
	 * @throws ClientException
	 */
	public static final CommandPacket fieldSetNull(String targetGuid,
			String field, GuidEntry writer) throws IOException,
			InvalidKeyException, NoSuchAlgorithmException, SignatureException,
			ClientException {
		return getCommand(CommandType.SetFieldNull, writer, GUID, targetGuid,
				FIELD, field, WRITER, writer.getGuid());
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
	public static final CommandPacket select(String field, String value)
			throws Exception {
		return getCommand(CommandType.Select, FIELD, field, VALUE, value);
	}

	/**
	 * If field is a GeoSpatial field queries the GNS server return all the
	 * guids that have fields that are within value which is a bounding box
	 * specified as a nested JSONArrays of paired tuples: [[LONG_UL,
	 * LAT_UL],[LONG_BR, LAT_BR]]
	 *
	 * @param field
	 * @param value
	 *            - [[LONG_UL, LAT_UL],[LONG_BR, LAT_BR]]
	 * @return a JSONArray containing the guids of all the matched records
	 * @throws Exception
	 */
	public static final CommandPacket selectWithin(String field, JSONArray value)
			throws Exception {
		return getCommand(CommandType.SelectWithin, FIELD, field, WITHIN,
				value.toString());
	}

	/**
	 * If field is a GeoSpatial field queries the GNS server and returns all the
	 * guids that have fields that are near value which is a point specified as
	 * a two element JSONArray: [LONG, LAT]. Max Distance is in meters.
	 *
	 * @param field
	 * @param value
	 *            - [LONG, LAT]
	 * @param maxDistance
	 *            - distance in meters
	 * @return a JSONArray containing the guids of all the matched records
	 * @throws Exception
	 */
	public static final CommandPacket selectNear(String field, JSONArray value,
			Double maxDistance) throws Exception {
		return getCommand(CommandType.SelectNear, FIELD, field, NEAR,
				value.toString(), MAX_DISTANCE, Double.toString(maxDistance));
	}

	/**
	 * Update the location field for the given GUID
	 *
	 * @param targetGuid
	 * @param longitude
	 *            the GUID longitude
	 * @param latitude
	 *            the GUID latitude
	 * @param writer
	 * @return
	 * @throws Exception
	 *             if a GNS error occurs
	 */
	public static final CommandPacket setLocation(String targetGuid,
			double longitude, double latitude, GuidEntry writer)
			throws Exception {
		return fieldReplaceOrCreateList(targetGuid, LOCATION_FIELD_NAME,
				new JSONArray(Arrays.asList(longitude, latitude)), writer);
	}

	/**
	 * Update the location field for the given GUID
	 *
	 * @param longitude
	 *            the GUID longitude
	 * @param latitude
	 *            the GUID latitude
	 * @param guid
	 *            the GUID to update
	 * @return
	 * @throws Exception
	 *             if a GNS error occurs
	 */
	public static final CommandPacket setLocation(GuidEntry guid,
			double longitude, double latitude) throws Exception {
		return setLocation(guid.getGuid(), longitude, latitude, guid);
	}

	/**
	 * Get the location of the target GUID as a JSONArray: [LONG, LAT]
	 *
	 * @param readerGuid
	 *            the GUID issuing the request
	 * @param targetGuid
	 *            the GUID that we want to know the location
	 * @return a JSONArray: [LONGITUDE, LATITUDE]
	 * @throws Exception
	 *             if a GNS error occurs
	 */
	public static final CommandPacket getLocation(String targetGuid,
			GuidEntry readerGuid) throws Exception {
		return fieldReadArray(targetGuid, LOCATION_FIELD_NAME, readerGuid);
	}

	/**
	 * Get the location of the target GUID as a JSONArray: [LONG, LAT]
	 *
	 * @param guid
	 * @return a JSONArray: [LONGITUDE, LATITUDE]
	 * @throws Exception
	 *             if a GNS error occurs
	 */
	public static final CommandPacket getLocation(GuidEntry guid)
			throws Exception {
		return fieldReadArray(guid.getGuid(), LOCATION_FIELD_NAME, guid);
	}

	/**
	 * @param guid
	 * @param action
	 * @param writerGuid
	 * @throws ClientException
	 * @throws IOException
	 */
	// Active Code
	public static final CommandPacket activeCodeClear(String guid,
			String action, GuidEntry writerGuid) throws ClientException,
			IOException {
		return getCommand(CommandType.ClearActiveCode, writerGuid, GUID, guid,
				AC_ACTION, action, WRITER, writerGuid.getGuid());
	}

	/**
	 * @param guid
	 * @param action
	 * @param code
	 * @param writerGuid
	 * @return
	 * @throws ClientException
	 * @throws IOException
	 */
	public static final CommandPacket activeCodeSet(String guid, String action,
			byte[] code, GuidEntry writerGuid) throws ClientException,
			IOException {
		return getCommand(CommandType.SetActiveCode, writerGuid, GUID, guid,
				AC_ACTION, action, AC_CODE, Base64.encodeToString(code, true),
				WRITER, writerGuid.getGuid());
	}

	/**
	 * @param guid
	 * @param action
	 * @param readerGuid
	 * @return Active code of {@code guid} as byte[]
	 * @throws Exception
	 */
	public static final CommandPacket activeCodeGet(String guid, String action,
			GuidEntry readerGuid) throws Exception {
		return getCommand(CommandType.GetActiveCode, readerGuid, GUID, guid,
				AC_ACTION, action, READER, readerGuid.getGuid());
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
	public static final CommandPacket fieldCreateList(GuidEntry target,
			String field, JSONArray value) throws IOException, ClientException {
		return fieldCreateList(target.getGuid(), field, value, target);
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
	 * @return
	 * @throws IOException
	 * @throws ClientException
	 */
	public static final CommandPacket fieldCreateOneElementList(
			String targetGuid, String field, String value, GuidEntry writer)
			throws IOException, ClientException {
		return getCommand(CommandType.Create, writer, GUID, targetGuid, FIELD,
				field, VALUE, value, WRITER, writer.getGuid());
	}

	/**
	 * Creates a new one element field in the target guid with single element
	 * value being the string.
	 *
	 * @param target
	 * @param field
	 * @param value
	 * @return
	 * @throws IOException
	 * @throws ClientException
	 */
	public static final CommandPacket fieldCreateOneElementList(
			GuidEntry target, String field, String value) throws IOException,
			ClientException {
		return fieldCreateOneElementList(target.getGuid(), field, value, target);
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
	 * @return
	 * @throws IOException
	 * @throws ClientException
	 */
	public static final CommandPacket fieldAppendOrCreate(String targetGuid,
			String field, String value, GuidEntry writer) throws IOException,
			ClientException {
		return getCommand(CommandType.AppendOrCreate, writer, GUID, targetGuid,
				FIELD, field, VALUE, value, WRITER, writer.getGuid());
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
	 * @return
	 * @throws IOException
	 * @throws ClientException
	 */
	public static final CommandPacket fieldReplaceOrCreate(String targetGuid,
			String field, String value, GuidEntry writer) throws IOException,
			ClientException {
		return getCommand(CommandType.ReplaceOrCreate, writer, GUID,
				targetGuid, FIELD, field, VALUE, value, WRITER,
				writer.getGuid());
	}

	/**
	 * Replaces the values of the field with the list of values or creates a new
	 * field with values in the list if it does not exist.
	 *
	 * @param targetGuid
	 * @param field
	 * @param value
	 * @return
	 * @throws IOException
	 * @throws ClientException
	 */
	public static final CommandPacket fieldReplaceOrCreateList(
			GuidEntry targetGuid, String field, JSONArray value)
			throws IOException, ClientException {
		return fieldReplaceOrCreateList(targetGuid.getGuid(), field, value,
				targetGuid);
	}

	/**
	 * Replaces the values of the field in the target guid with the single value
	 * or creates a new field with a single value list if it does not exist.
	 *
	 * @param target
	 * @param field
	 * @param value
	 * @return
	 * @throws IOException
	 * @throws ClientException
	 */
	public static final CommandPacket fieldReplaceOrCreateList(
			GuidEntry target, String field, String value) throws IOException,
			ClientException {
		return fieldReplaceOrCreate(target.getGuid(), field, value, target);
	}

	/**
	 * Replaces all the values of field with the single value. If the writer is
	 * different use addToACL first to allow other the guid to write this field.
	 *
	 * @param targetGuid
	 *            GUID where the field is stored
	 * @param field
	 *            field name
	 * @param value
	 *            the new value
	 * @param writer
	 *            GUID entry of the writer
	 * @return
	 * @throws IOException
	 * @throws ClientException
	 */
	public static final CommandPacket fieldReplace(String targetGuid,
			String field, String value, GuidEntry writer) throws IOException,
			ClientException {
		return getCommand(CommandType.Replace, writer, GUID, targetGuid, FIELD,
				field, VALUE, value, WRITER, writer.getGuid());
	}

	/**
	 * Replaces all the values of field in target with with the single value.
	 *
	 * @param target
	 * @param field
	 * @param value
	 * @return
	 * @throws IOException
	 * @throws ClientException
	 */
	public static final CommandPacket fieldReplace(GuidEntry target,
			String field, String value) throws IOException, ClientException {
		return fieldReplace(target.getGuid(), field, value, target);
	}

	/**
	 * Replaces all the values of field in target with the list of values.
	 *
	 * @param target
	 * @param field
	 * @param value
	 * @return
	 * @throws IOException
	 * @throws ClientException
	 */
	public static final CommandPacket fieldReplace(GuidEntry target,
			String field, JSONArray value) throws IOException, ClientException {
		return fieldReplaceList(target.getGuid(), field, value, target);
	}

	/**
	 * Appends a single value onto a field. If the writer is different use
	 * addToACL first to allow other the guid to write this field.
	 *
	 * @param targetGuid
	 * @param field
	 * @param value
	 * @param writer
	 * @return
	 * @throws IOException
	 * @throws ClientException
	 */
	public static final CommandPacket fieldAppend(String targetGuid,
			String field, String value, GuidEntry writer) throws IOException,
			ClientException {
		return getCommand(CommandType.AppendWithDuplication, writer, GUID,
				targetGuid, FIELD, field, VALUE, value, WRITER,
				writer.getGuid());
	}

	/**
	 * Appends a single value onto a field in the target guid.
	 *
	 * @param target
	 * @param field
	 * @param value
	 * @return
	 * @throws IOException
	 * @throws ClientException
	 */
	public static final CommandPacket fieldAppend(GuidEntry target,
			String field, String value) throws IOException, ClientException {
		return fieldAppend(target.getGuid(), field, value, target);
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
	 * @return
	 * @throws IOException
	 * @throws ClientException
	 */
	public static final CommandPacket fieldAppendWithSetSemantics(
			String targetGuid, String field, JSONArray value, GuidEntry writer)
			throws IOException, ClientException {
		return getCommand(CommandType.AppendList, writer, GUID, targetGuid,
				FIELD, field, VALUE, value.toString(), WRITER, writer.getGuid());
	}

	/**
	 * Appends a list of values onto a field in target but converts the list to
	 * set removing duplicates.
	 *
	 * @param target
	 * @param field
	 * @param value
	 * @return
	 * @throws IOException
	 * @throws ClientException
	 */
	public static final CommandPacket fieldAppendWithSetSemantics(
			GuidEntry target, String field, JSONArray value)
			throws IOException, ClientException {
		return fieldAppendWithSetSemantics(target.getGuid(), field, value,
				target);
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
	public static final CommandPacket fieldAppendWithSetSemantics(
			String targetGuid, String field, String value, GuidEntry writer)
			throws IOException, ClientException {
		return getCommand(CommandType.Append, writer, GUID, targetGuid, FIELD,
				field, VALUE, value.toString(), WRITER, writer.getGuid());
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
	public static final CommandPacket fieldAppendWithSetSemantics(
			GuidEntry target, String field, String value) throws IOException,
			ClientException {
		return fieldAppendWithSetSemantics(target.getGuid(), field, value,
				target);
	}

	/**
	 * Replaces all the first element of field with the value. If the writer is
	 * different use addToACL first to allow other the guid to write this field.
	 * If writer is null the command is sent unsigned.
	 *
	 * @param targetGuid
	 * @param field
	 * @param value
	 * @param writer
	 * @return
	 * @throws IOException
	 * @throws ClientException
	 */
	public static final CommandPacket fieldReplaceFirstElement(
			String targetGuid, String field, String value, GuidEntry writer)
			throws IOException, ClientException {
		if (writer == null)
			throw new ClientException(
					"Can not perform an update without querier information");
		return getCommand(CommandType.Replace, writer, GUID, targetGuid, FIELD,
				field, VALUE, value, WRITER, writer != null ? writer.getGuid()
						: null);
	}

	/**
	 * For testing only.
	 *
	 * @param targetGuid
	 * @param field
	 * @param value
	 * @param writer
	 * @return
	 * @throws IOException
	 * @throws ClientException
	 */
	public static final CommandPacket fieldReplaceFirstElementTest(
			String targetGuid, String field, String value, GuidEntry writer)
			throws IOException, ClientException {
		return writer == null ? getCommand(CommandType.ReplaceUnsigned, GUID,
				targetGuid, FIELD, field, VALUE, value)
				: fieldReplaceFirstElement(targetGuid, field, value, writer);
	}

	/**
	 * Replaces the first element of field in target with the value.
	 *
	 * @param target
	 * @param field
	 * @param value
	 * @return
	 * @throws IOException
	 * @throws ClientException
	 */
	public static final CommandPacket fieldReplaceFirstElement(
			GuidEntry target, String field, String value) throws IOException,
			ClientException {
		return fieldReplaceFirstElement(target.getGuid(), field, value, target);
	}

	/**
	 * Substitutes the value for oldValue in the list of values of a field. If
	 * the writer is different use addToACL first to allow other the guid to
	 * write this field.
	 *
	 * @param targetGuid
	 *            GUID where the field is stored
	 * @param field
	 *            field name
	 * @param newValue
	 * @param oldValue
	 * @param writer
	 *            GUID entry of the writer
	 * @return
	 * @throws IOException
	 * @throws ClientException
	 */
	public static final CommandPacket fieldSubstitute(String targetGuid,
			String field, String newValue, String oldValue, GuidEntry writer)
			throws IOException, ClientException {
		return getCommand(CommandType.Substitute, writer, GUID, targetGuid,
				FIELD, field, VALUE, newValue, OLD_VALUE, oldValue, WRITER,
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
	 * @return
	 * @throws IOException
	 * @throws ClientException
	 */
	public static final CommandPacket fieldSubstitute(GuidEntry target,
			String field, String newValue, String oldValue) throws IOException,
			ClientException {
		return fieldSubstitute(target.getGuid(), field, newValue, oldValue,
				target);
	}

	/**
	 * Pairwise substitutes all the values for the oldValues in the list of
	 * values of a field. If the writer is different use addToACL first to allow
	 * other the guid to write this field.
	 *
	 *
	 * @param targetGuid
	 *            GUID where the field is stored
	 * @param field
	 * @param newValue
	 *            list of new values
	 * @param oldValue
	 *            list of old values
	 * @param writer
	 *            GUID entry of the writer
	 * @return
	 * @throws IOException
	 * @throws ClientException
	 */
	public static final CommandPacket fieldSubstitute(String targetGuid,
			String field, JSONArray newValue, JSONArray oldValue,
			GuidEntry writer) throws IOException, ClientException {
		return getCommand(CommandType.SubstituteList, writer, GUID, targetGuid,
				FIELD, field, VALUE, newValue.toString(), OLD_VALUE,
				oldValue.toString(), WRITER, writer.getGuid());
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
	public static final CommandPacket fieldSubstitute(GuidEntry target,
			String field, JSONArray newValue, JSONArray oldValue)
			throws IOException, ClientException {
		return fieldSubstitute(target.getGuid(), field, newValue, oldValue,
				target);
	}

	/**
	 * Reads the first value for a key from the GNS server for the given guid.
	 * The guid of the user attempting access is also needed. Signs the query
	 * using the private key of the user associated with the reader guid
	 * (unsigned if reader is null).
	 *
	 * @param guid
	 * @param field
	 * @param reader
	 * @return First value of {@code field} whose value is expected to be an
	 *         array.
	 * @throws Exception
	 */
	public static final CommandPacket fieldReadArrayFirstElement(String guid,
			String field, GuidEntry reader) throws Exception {
		return getCommand(reader != null ? CommandType.ReadArrayOne
				: CommandType.ReadArrayOneUnsigned, reader, GUID, guid, FIELD,
				field, READER, reader != null ? reader.getGuid() : null);
	}

	/**
	 * Reads the first value for a key in the guid.
	 *
	 * @param guid
	 * @param field
	 * @return First value of {@code field} whose value is expected to be an
	 *         array.
	 * @throws Exception
	 */
	public static final CommandPacket fieldReadArrayFirstElement(
			GuidEntry guid, String field) throws Exception {
		return fieldReadArrayFirstElement(guid.getGuid(), field, guid);
	}

	/**
	 * Removes a field in the JSONObject record of the given guid. Signs the
	 * query using the private key of the guid. A convenience method™.
	 *
	 * @param guid
	 * @param field
	 * @return
	 * @throws IOException
	 * @throws InvalidKeyException
	 * @throws NoSuchAlgorithmException
	 * @throws SignatureException
	 * @throws ClientException
	 */
	public static final CommandPacket fieldRemove(GuidEntry guid, String field)
			throws IOException, InvalidKeyException, NoSuchAlgorithmException,
			SignatureException, ClientException {
		return fieldRemove(guid.getGuid(), field, guid);
	}

	/**
	 * @param passkey
	 * @return ???
	 * @throws Exception
	 */
	public static final CommandPacket adminEnable(String passkey)
			throws Exception {
		return getCommand(CommandType.Admin, PASSKEY, passkey);
	}

}
