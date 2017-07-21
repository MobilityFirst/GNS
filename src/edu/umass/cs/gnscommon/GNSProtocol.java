package edu.umass.cs.gnscommon;

import edu.umass.cs.reconfiguration.reconfigurationpackets.ClientReconfigurationPacket;

/**
 * An enum class for all the constants used by GNS wireline protocol.
 * 
 * @author arun
 *
 */
public enum GNSProtocol {
  //
  // Response codes
  //
  /**
   * Indicates that a command that does not return a value has completed successfully.
   * See {@link edu.umass.cs.gnscommon.ResponseCode#NO_ERROR}.
   *///
  // Response codes
  //
  /**
   * Indicates that a command that does not return a value has completed successfully.
   * See {@link edu.umass.cs.gnscommon.ResponseCode#NO_ERROR}.
   */
  OK_RESPONSE("+OK+"),
  /**
   * A prefix used in the command return value to indicate an anomolous condition.
   * Always accompanied by an additional string value which indicates the
   * particular exception or error that occurred.
   *
   */
  BAD_RESPONSE("+NO+"),
  /**
   * Indicates that a command resulted in an error of some no specified type.
   * Should be use sparingly if at all. Uses should replaced with a more specific code
   * if possible.
   * See {@link edu.umass.cs.gnscommon.ResponseCode#UNSPECIFIED_ERROR}.
   */
  UNSPECIFIED_ERROR("+GENERICERROR+"),
  /**
   * Indicates that a the signature supplied with a command did not match the message
   * in the command.
   * See {@link edu.umass.cs.gnscommon.ResponseCode#SIGNATURE_ERROR}.
   */
  BAD_SIGNATURE("+BAD_SIGNATURE+"),
  /**
   * Indicates that the accessor guid provided with the command does not have access
   * to the field being accessed.
   * See {@link edu.umass.cs.gnscommon.ResponseCode#ACCESS_ERROR}.
   */
  ACCESS_DENIED("+ACCESS_DENIED+"),
  /**
   * Indicates that the command is too old to be executed.
   * See {@link edu.umass.cs.gnscommon.ResponseCode#STALE_COMMAND_VALUE}.
   */
  STALE_COMMMAND("+STALE_COMMMAND+"),
  /**
   * Indicates that command could not be executed because it had an unknown name
   * or incorrect arguments.
   * See {@link edu.umass.cs.gnscommon.ResponseCode#OPERATION_NOT_SUPPORTED}.
   */
  OPERATION_NOT_SUPPORTED("+OPERATIONNOTSUPPORTED+"),
  /**
   * Indicates that command could not be executed due to non-json parsing or
   * other interpretation error.
   * An additional string is usually provided explaining the error.
   * See {@link edu.umass.cs.gnscommon.ResponseCode#QUERY_PROCESSING_ERROR}.
   */
  QUERY_PROCESSING_ERROR("+QUERYPROCESSINGERROR+"),
  /**
   * Indicates that command an account verification or password verification error occurred.
   * See {@link edu.umass.cs.gnscommon.ResponseCode#VERIFICATION_ERROR}.
   */
  VERIFICATION_ERROR("+VERIFICATIONERROR+"),
  /**
   * Indicates that an account has already been verified.
   */
  ALREADY_VERIFIED_EXCEPTION("+ALREADYVERIFIED+"),
  /**
   * Indicates that a remote query on the server side failed.
   */
  REMOTE_QUERY_EXCEPTION("+REMOTEQUERY+"),
  /**
   * Indicates that a command is trying to access a field using an
   * accessor guid that does not exist.
   * See {@link edu.umass.cs.gnscommon.ResponseCode#BAD_ACCESSOR_ERROR}.
   */
  BAD_ACCESSOR_GUID("+BADACCESSORGUID+"),
  /**
   * Indicates that a command is trying to use a guid that does not exist.
   * See {@link edu.umass.cs.gnscommon.ResponseCode#BAD_GUID_ERROR}.
   */
  BAD_GUID("+BADGUID+"),
  /**
   * Indicates that a command is trying to use an account guid that does not exist.
   * See {@link edu.umass.cs.gnscommon.ResponseCode#BAD_ACCOUNT_ERROR}.
   */
  BAD_ACCOUNT("+BADACCOUNT+"),
  /**
   * Indicates that a command is trying to use an alias that does not exist.
   * See {@link edu.umass.cs.gnscommon.ResponseCode#BAD_ALIAS_EXCEPTION}.
   *
   */
  BAD_ALIAS("+BADALIAS+"),
  /**
   * Indicates that a command is trying to use an ACL type that does not exist.
   * See {@link edu.umass.cs.gnscommon.ResponseCode#BAD_ACL_TYPE_ERROR}.
   */
  BAD_ACL_TYPE("+BADACLTYPE+"),
  /**
   * Indicates that a command is trying to access a field that does not exist.
   * See {@link edu.umass.cs.gnscommon.ResponseCode#FIELD_NOT_FOUND_EXCEPTION}.
   */
  FIELD_NOT_FOUND("+FIELDNOTFOUND+"),
  /**
   * Indicates that a command is trying to add a guid that already exists.
   *  * See {@link edu.umass.cs.gnscommon.ResponseCode#CONFLICTING_GUID_EXCEPTION}.
   */
  DUPLICATE_GUID("+DUPLICATEGUID+"),
  /**
   * Indicates that a command is trying to add a field that already exists.
   * See {@link edu.umass.cs.gnscommon.ResponseCode#DUPLICATE_FIELD_EXCEPTION}.
   */
  DUPLICATE_FIELD("+DUPLICATEFIELD+"),
  /**
   * Indicates that a command is trying to add a HRN that already exists.
   * See {@link edu.umass.cs.gnscommon.ResponseCode#CONFLICTING_HRN_EXCEPTION}.
   */
  DUPLICATE_NAME("+DUPLICATENAME+"),
  /**
   * Indicates that a command resulted in a error while parsing a JSON string.
   * See {@link edu.umass.cs.gnscommon.ResponseCode#JSON_PARSE_ERROR}.
   */
  JSON_PARSE_ERROR("+JSONPARSEERROR+"),
  /**
   * Indicates that a command attempted to create more alias than is allowed.
   * See {@link edu.umass.cs.gnscommon.ResponseCode#TOO_MANY_ALIASES_EXCEPTION}.
   */
  TOO_MANY_ALIASES("+TOMANYALIASES+"),
  /**
   * Indicates that a command attempted to create more guids than is allowed.
   * See {@link edu.umass.cs.gnscommon.ResponseCode#TOO_MANY_GUIDS_EXCEPTION}.
   */
  TOO_MANY_GUIDS("+TOMANYGUIDS+"),
  /**
   * Indicates that a command resulted in an error while updating a record.
   * See {@link edu.umass.cs.gnscommon.ResponseCode#UPDATE_ERROR}.
   */
  UPDATE_ERROR("+UPDATEERROR+"),
  /**
   * Indicates that a command resulted in an error while updating a record.
   * See {@link edu.umass.cs.gnscommon.ResponseCode#UPDATE_ERROR}.
   */
  DATABASE_OPERATION_ERROR("+DATABASEOPERROR+"),
  /**
   * Indicates that a timeout occurred during the execution of a command.
   * See {@link edu.umass.cs.gnscommon.ResponseCode#TIMEOUT}.
   */
  TIMEOUT("+TIMEOUT+"),
  /**
   * Indicates that a command resulted in an active replica receiving a request
   * for a name that is not replicated there.
   * See {@link edu.umass.cs.gnscommon.ResponseCode#ACTIVE_REPLICA_EXCEPTION}.
   */
  ACTIVE_REPLICA_EXCEPTION(ClientReconfigurationPacket.ResponseCodes.ACTIVE_REPLICA_EXCEPTION.toString()),
  //
  // End of Response codes
  //
  /**
   * Used to allow commands to store and return a null value.
   */
  NULL_RESPONSE("+NULL+"),
  /**
   * The name of the RSA algorithm. Used with {@link java.security.KeyFactory}
   */
  RSA_ALGORITHM("RSA"),
  /**
   * The name of the signature algorithm. Used with {@link java.security.Signature}
   */
  SIGNATURE_ALGORITHM("SHA1withRSA"),
  /**
   * The name of the digest algorithm. Used with {@link java.security.MessageDigest}
   */
  DIGEST_ALGORITHM("SHA1"),
  /**
   * The name of the secret key algorithm. Used with {@link javax.crypto.Cipher}
   */
  SECRET_KEY_ALGORITHM("DESede"),
  /**
   * The character set used by client and server for reading and writing command packets.
   */
  // FIXME: The reason why we use ISO-8859-1 should be more throughly documented here.
  CHARSET("ISO-8859-1"),
  //
  // Argument fields in commands.
  //
  /**
   * The guid field in a command. Usually the guid being acted upon.
   */
  GUID("guid"),
  /**
   * The name field in a command. Usually the HRN being acted upon.
   */
  NAME("name"),
  /**
   * The names field in a command. Usually specifies a list of HRNs.
   */
  NAMES("names"),
  /**
   * The account guid field in a command. Only used in guid remove.
   */
  ACCOUNT_GUID("accountGuid"),
  /**
   * The reader guid in a command. Specifies the guid attempting to access a field for reading.
   */
  READER("reader"),
  /**
   * The writer guid in a command. Specifies the guid attempting to access a field for writing.
   */
  WRITER("writer"),
  /**
   * The accesser guid in a command. Specifies the guid attempting to access a field for reading or writing.
   */
  ACCESSER("accesser"),
  /**
   * The field being accessed in a command.
   */
  FIELD("field"),
  /**
   * The fields being accessed in a command.
   */
  FIELDS("fields"),
  /**
   * The value being updated in a command.
   */
  VALUE("value"),
  /**
   * The old value in a command. Used in substitution commands.
   */
  OLD_VALUE("oldvalue"),
  /**
   * The JSON Object in a command.
   */
  USER_JSON("userjson"),
  /**
   * The generic argument in a command.
   */
  ARGUMENT("argument"),
  /**
   * Used to represent an array element being accessed in a command.
   */
  N("n"),
  /**
   * The member field in a command. The member of a group guid.
   */
  MEMBER("member"),
  /**
   * The member field in a command. The members of a group guid.
   */
  MEMBERS("members"),
  /**
   * The acl type field in a command.
   */
  ACL_TYPE("aclType"),
  /**
   * The public key field in a command.
   */
  PUBLIC_KEY("publickey"),
  /**
   * The public keys list field in a command.
   */
  PUBLIC_KEYS("publickeys"),
  /**
   * The password field in a command.
   */
  PASSWORD("password"),
  /**
   * The code field in a command. Used for account verification.
   */
  CODE("code"),
  /**
   * The signature field in a command.
   */
  SIGNATURE("signature"),
  // select commands
  /**
   * The signature field in a command. Used for select within.
   */
  WITHIN("within"),
  /**
   * The signature field in a command. Used for select near.
   */
  NEAR("near"),
  /**
   * The max distance field in a command. Used for select near.
   */
  MAX_DISTANCE("maxDistance"),
  /**
   * The signature field in a command. Used for select query.
   */
  QUERY("query"),
  /**
   * The interval value in a command. Used for select commands
   * to determine refresh interval.
   */
  INTERVAL("interval"),
  /**
   * Used in commands to represent the use of all fields.
   */
  ENTIRE_RECORD("+ALL+"),
  /**
   * Used in commands to represent the use of all guids.
   */
  ALL_GUIDS("+ALL+"),
  /**
   * Used in commands to represent the use of all guids.
   */
  EVERYONE("+ALL+"),
  // admin commands
  /**
   * The logging level in a command.
   */
  LOG_LEVEL("level"),
  /**
   * Represents the number of guids in a command.
   */
  GUIDCNT("guidCnt"),
  /**
   * Represents the ACL field in GUID info's meta data
   */
  MD("MD"),
  /**
   * Represents the metaData field in user's JSON
   */
  META_DATA_FIELD("nr_valuesMap"),
  //
  // Command packet fields
  //
  /**
   * The timestamp of the command. Currently optional.
   */
  TIMESTAMP("timestamp"),
  /**
   * The sequence number field in a command packet.
   */
  NONCE("seqnum"),
  /**
   * The passkey number field in a command packet.
   */
  PASSKEY("passkey"),
  /**
   * The message that was signed field in a command packet.
   */
  SIGNATUREFULLMESSAGE("_signatureFullMessage_"),
  // Special fields for ACL
  /**
   *
   */
  GROUP_ACL("+GROUP_ACL+"),
  // Field names in guid record JSON Object
  /**
   * The public key field in a guid record.
   */
  GUID_RECORD_PUBLICKEY("publickey"),
  /**
   * The name field in a guid record.
   */
  GUID_RECORD_NAME("name"),
  /**
   * The guid field in a guid record.
   */
  GUID_RECORD_GUID("guid"),
  /**
   * The type field in a guid record.
   */
  GUID_RECORD_TYPE("type"),
  /**
   * The created field in a guid record.
   */
  GUID_RECORD_CREATED("created"),
  /**
   * The updated field in a guid record.
   */
  GUID_RECORD_UPDATED("updated"),
  /**
   * The tags field in a guid record.
   */
  GUID_RECORD_TAGS("tags"),
  // Field names in account record JSON Object
  /**
   * The verified field in a account record.
   */
  ACCOUNT_RECORD_VERIFIED("verified"),
  /**
   * The guids field in a account record.
   */
  ACCOUNT_RECORD_GUIDS("guids"),
  /**
   * The guid field in a account record.
   */
  ACCOUNT_RECORD_GUID("guid"),
  /**
   * The username field in a account record.
   */
  ACCOUNT_RECORD_USERNAME("username"),
  /**
   * The created field in a account record.
   */
  ACCOUNT_RECORD_CREATED("created"),
  /**
   * The updated field in a account record.
   */
  ACCOUNT_RECORD_UPDATED("updated"),
  /**
   * The type field in a account record.
   */
  ACCOUNT_RECORD_TYPE("type"),
  /**
   * The password field in a account record.
   */
  ACCOUNT_RECORD_PASSWORD("password"),
  /**
   * The aliases field in a account record.
   */
  ACCOUNT_RECORD_ALIASES("aliases"),
  // Blessed field names
  /**
   * The standard name of the legacy location field of a record.
   */
  LOCATION_FIELD_NAME("geoLocation"),
  /**
   * The standard name of the location field of a record.
   */
  LOCATION_FIELD_NAME_2D_SPHERE("geoLocationCurrent"),
  /**
   * The standard name of the ipaddress field of a record.
   */
  IPADDRESS_FIELD_NAME("netAddress"),
  /**
   * The preferred way to indicate the command type is a command packet.
   */
  COMMAND_INT("COMMANDINT"),
  /**
   * Can be used by clients to indicate the command type in a command packet.
   */
  COMMANDNAME("COMMANDNAME"),
  /**
   * If this exists in a command it indicates that coordinated reads should be used.
   */
  FORCE_COORDINATE_READS("COORDREAD"),
  /**
   * This member was not documented by it's creator.
   */
  AC_ACTION("acAction"),
  /**
   * This member was not documented by it's creator.
   */
  AC_CODE("acCode"),
  //
  /**
   * The prefix used to hide GNS internal fields.
   */
  INTERNAL_PREFIX("_GNS_"),
  /**
   * The newline character in a string.
   */
  NEWLINE(System.getProperty("line.separator")),
  //
  // Misc
  //
  //
  /**
   * The key for the querier GUID that originated the request chain. This is
   * the GUID to which all resource usage is charged. This GUID is needed to
   * enforce ACL checks but not necessarily signature checks as is the case
   * with internal requests as well as active requests. The request chain here
   * as well as in the following two enums may refer to a chain of requests
   * spawned either by active code or simply in the course of regular requests
   * that transitively spawn other requests.
   */
  ORIGINATING_GUID("OGUID"),
  /**
   * The key for the request ID corresponding to the request that originated
   * the request chain.
   */
  ORIGINATING_QID("OQID"),
  /**
   * The number of hops in the request chain remaining before the request
   * expires.
   */
  REQUEST_TTL("QTTL"),
  
  /**
   * The GUID inducing this query. For active request chains, this is the
   * most recently queried GUID that becomes the querying GUID for the
   * next request in the chain.
   */
  QUERIER_GUID ("QGUID"),

  
  /**
   * Proof that this request is internal. 
   */
  INTERNAL_PROOF ("IPROOF"),
  
  /**
   * Special reader or writer to indicate that the querier is internal.
   * The proof that this request is internal is *NOT* in the secrecy of
   * this string.
   */
  INTERNAL_QUERIER("IQUERIER"),

  /**
   * Long client request ID in every {@link edu.umass.cs.gnscommon.packets.CommandPacket}.
   */
  REQUEST_ID("QID"),
  /**
   * String return value carried in every {@link edu.umass.cs.gnscommon.packets.ResponsePacket}.
   */
  RETURN_VALUE("RVAL"),
  /**
   * The query carried in every {@link edu.umass.cs.gnscommon.packets.CommandPacket}.
   */
  COMMAND_QUERY("QVAL"),
  /**
   * Name or HRN or GUID, whatever is used in {@link edu.umass.cs.gigapaxos.interfaces.Request#getServiceName}.
   */
  SERVICE_NAME("NAME"),
  /**
   * The name carried in requests not directed to any particular GUID.
   */
  UNKNOWN_NAME("unknown"),
  /**
   * Error code carried in {@link edu.umass.cs.gnscommon.packets.ResponsePacket}.
   */
  ERROR_CODE("ECODE"),
  /**
   * Internal request exception message string.
   */
  INTERNAL_REQUEST_EXCEPTION("+INTERNAL_REQUEST_EXCEPTION+"),
  /**
   * Whether an internal request was previously coordinated (at most once).
   */
  COORD1("COORD1"),
  /**
   * Indicates that sanity check failed
   * See {@link edu.umass.cs.gnscommon.ResponseCode#SANITY_CHECK_ERROR}.
   */
  SANITY_CHECK_ERROR("+SANITY_CHECK_ERROR+"),;

  final String label;

  GNSProtocol(String label) {
    this.label = label != null ? label : this.name();
  }

  @Override
  public String toString() {
    return this.label;
  }
  
  private static String generateSwiftConstants() {
    StringBuilder result = new StringBuilder();
    for (GNSProtocol entry : GNSProtocol.values()) {
      result.append("    public static let ");
      result.append(entry.name().toUpperCase());
      result.append("\t\t\t\t = ");
      result.append("\"");
      result.append(entry.toString());
      result.append("\"\n");
    }
    return result.toString();
  }
  
  public static void main(String args[]) {
    System.out.println(generateSwiftConstants());
  }
}
