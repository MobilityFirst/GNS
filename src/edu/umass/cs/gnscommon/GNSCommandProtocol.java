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
package edu.umass.cs.gnscommon;

import edu.umass.cs.reconfiguration.reconfigurationpackets.ClientReconfigurationPacket;

/**
 * This class defines a GNSCommandProtocol. Which is to say that
 * it defines a bunch of constants that define the protocol
 * support for communicating between the client and the local name server.
 *
 * See also CommandType.
 *
 * @author arun, Westy
 * @version 1.18
 */
public class GNSCommandProtocol {

  //
  // Response codes
  //
  /**
   * Indicates that a command that does not return a value has completed successfully.
   * See {@link edu.umass.cs.gnscommon.GNSResponseCode#NO_ERROR}.
   */
  public final static String OK_RESPONSE = "+OK+";
  /**
   * A prefix used in the command return value to indicate an anomolous condition.
   * Always accompanied by an additional string value which indicates the
   * particular exception or error that occurred.
   * 
   */
  public final static String BAD_RESPONSE = "+NO+";
  /**
   * Indicates that a command resulted in an error of some no specified type.
   * Should be use sparingly if at all. Uses should replaced with a more specific code
   * if possible.
   * See {@link edu.umass.cs.gnscommon.GNSResponseCode#UNSPECIFIED_ERROR}.
   */
  public final static String UNSPECIFIED_ERROR = "+GENERICERROR+";
  /**
   * Indicates that a the signature supplied with a command did not match the message
   * in the command.
   * See {@link edu.umass.cs.gnscommon.GNSResponseCode#SIGNATURE_ERROR}.
   */
  public final static String BAD_SIGNATURE = "+BAD_SIGNATURE+";
  /**
   * Indicates that the accessor guid provided with the command does not have access
   * to the field being accessed.
   * See {@link edu.umass.cs.gnscommon.GNSResponseCode#ACCESS_ERROR}.
   */
  public final static String ACCESS_DENIED = "+ACCESS_DENIED+";
  /**
   * Indicates that the command is too old to be executed.
   * See {@link edu.umass.cs.gnscommon.GNSResponseCode#STALE_COMMAND_VALUE}.
   */
  public final static String STALE_COMMMAND = "+STALE_COMMMAND+";
  /**
   * Indicates that command could not be executed because it had an unknown name
   * or incorrect arguments.
   * See {@link edu.umass.cs.gnscommon.GNSResponseCode#OPERATION_NOT_SUPPORTED}.
   */
  public final static String OPERATION_NOT_SUPPORTED = "+OPERATIONNOTSUPPORTED+";
  /**
   * Indicates that command could not be executed due to non-json parsing or
   * other interpretation error.
   * An additional string is usually provided explaining the error.
   * See {@link edu.umass.cs.gnscommon.GNSResponseCode#QUERY_PROCESSING_ERROR}.
   */
  public final static String QUERY_PROCESSING_ERROR = "+QUERYPROCESSINGERROR+";
  /**
   * Indicates that command an account verification or password verification error occurred.
   * See {@link edu.umass.cs.gnscommon.GNSResponseCode#VERIFICATION_ERROR}.
   */
  public final static String VERIFICATION_ERROR = "+VERIFICATIONERROR+";

  /**
   * Indicates that an account has already been verified.
   */
  public final static String ALREADY_VERIFIED_EXCEPTION = "+ALREADYVERIFIED+";

	/**
	 * Indicates that a remote query on the server side failed.
	 */
	public static final String REMOTE_QUERY_EXCEPTION = "+REMOTEQUERY+";

  /**
   * Indicates that a command is trying to access a field using an
   * accessor guid that does not exist.
   * See {@link edu.umass.cs.gnscommon.GNSResponseCode#BAD_ACCESSOR_ERROR}.
   */
  public final static String BAD_ACCESSOR_GUID = "+BADACCESSORGUID+";
  /**
   * Indicates that a command is trying to use a guid that does not exist.
   * See {@link edu.umass.cs.gnscommon.GNSResponseCode#BAD_GUID_ERROR}.
   */
  public final static String BAD_GUID = "+BADGUID+";
  /**
   * Indicates that a command is trying to use an account guid that does not exist.
   * See {@link edu.umass.cs.gnscommon.GNSResponseCode#BAD_ACCOUNT_ERROR}.
   */
  public final static String BAD_ACCOUNT = "+BADACCOUNT+";
  /**
   * Indicates that a command is trying to use an alias that does not exist.
   * See {@link edu.umass.cs.gnscommon.GNSResponseCode#BAD_ALIAS_EXCEPTION}.
   * 
   */
  public final static String BAD_ALIAS = "+BADALIAS+";
  /**
   * Indicates that a command is trying to use an ACL type that does not exist.
   * See {@link edu.umass.cs.gnscommon.GNSResponseCode#BAD_ACL_TYPE_ERROR}.
   */
  public final static String BAD_ACL_TYPE = "+BADACLTYPE+";
  /**
   * Indicates that a command is trying to access a field that does not exist.
   * See {@link edu.umass.cs.gnscommon.GNSResponseCode#FIELD_NOT_FOUND_EXCEPTION}.
   */
  public final static String FIELD_NOT_FOUND = "+FIELDNOTFOUND+";
  /**
   * Indicates that a command is trying to add a guid that already exists.
   *  * See {@link edu.umass.cs.gnscommon.GNSResponseCode#DUPLICATE_GUID_EXCEPTION}.
   */
  public final static String DUPLICATE_GUID = "+DUPLICATEGUID+";
  /**
   * Indicates that a command is trying to add a field that already exists.
   * See {@link edu.umass.cs.gnscommon.GNSResponseCode#DUPLICATE_FIELD_EXCEPTION}.
   */
  public final static String DUPLICATE_FIELD = "+DUPLICATEFIELD+";
  /**
   * Indicates that a command is trying to add a HRN that already exists.
   * See {@link edu.umass.cs.gnscommon.GNSResponseCode#DUPLICATE_NAME_EXCEPTION}.
   */
  public final static String DUPLICATE_NAME = "+DUPLICATENAME+";
  /**
   * Indicates that a command resulted in a error while parsing a JSON string.
   * See {@link edu.umass.cs.gnscommon.GNSResponseCode#JSON_PARSE_ERROR}.
   */
  public final static String JSON_PARSE_ERROR = "+JSONPARSEERROR+";
  /**
   * Indicates that a command attempted to create more alias than is allowed.
   * See {@link edu.umass.cs.gnscommon.GNSResponseCode#TOO_MANY_ALIASES_EXCEPTION}.
   */
  public final static String TOO_MANY_ALIASES = "+TOMANYALIASES+";
  /**
   * Indicates that a command attempted to create more guids than is allowed.
   * See {@link edu.umass.cs.gnscommon.GNSResponseCode#TOO_MANY_GUIDS_EXCEPTION}.
   */
  public final static String TOO_MANY_GUIDS = "+TOMANYGUIDS+";
  /**
   * Indicates that a command resulted in an error while updating a record.
   * See {@link edu.umass.cs.gnscommon.GNSResponseCode#UPDATE_ERROR}.
   */
  public final static String UPDATE_ERROR = "+UPDATEERROR+";
   /**
   * Indicates that a command resulted in an error while updating a record.
   * See {@link edu.umass.cs.gnscommon.GNSResponseCode#UPDATE_ERROR}.
   */
  public final static String DATABASE_OPERATION_ERROR = "+DATABASEOPERROR+";
  /**
   * Indicates that a timeout occurred during the execution of a command.
   * See {@link edu.umass.cs.gnscommon.GNSResponseCode#TIMEOUT}.
   */
  public final static String TIMEOUT = "+TIMEOUT+";
  /**
   * Indicates that a command resulted in an active replica receiving a request
   * for a name that is not replicated there.
   * See {@link edu.umass.cs.gnscommon.GNSResponseCode#ACTIVE_REPLICA_EXCEPTION}.
   */
  public final static String ACTIVE_REPLICA_EXCEPTION
          = ClientReconfigurationPacket.ResponseCodes.ACTIVE_REPLICA_EXCEPTION.toString();
  //
  // End of Response codes
  //
  /**
   * Used to allow commands to store and return a null value.
   */
  public final static String NULL_RESPONSE = "+NULL+";
  /**
   * The name of the RSA algorithm. Used with {@link java.security.KeyFactory}
   */
  public static final String RSA_ALGORITHM = "RSA";
  /**
   * The name of the signature algorithm. Used with {@link java.security.Signature}
   */
  public static final String SIGNATURE_ALGORITHM = "SHA1withRSA";
  /**
   * The name of the digest algorithm. Used with {@link java.security.MessageDigest}
   */
  public static final String DIGEST_ALGORITHM = "SHA1";
  /**
   * The name of the secret key algorithm. Used with {@link javax.crypto.Cipher}
   */
  public static final String SECRET_KEY_ALGORITHM = "DESede";
  /**
   * The character set used by client and server for reading and writing command packets.
   */
  public static final String CHARSET = "ISO-8859-1";

  //
  // Argument fields in commands.
  //
  /**
   * The guid field in a command. Usually the guid being acted upon.
   */
  public final static String GUID = "guid";
  /**
   * The name field in a command. Usually the HRN being acted upon.
   */
  public final static String NAME = "name";
  /**
   * The names field in a command. Usually specifies a list of HRNs.
   */
  public final static String NAMES = "names";
  /**
   * The account guid field in a command. Only used in guid remove.
   */
  public final static String ACCOUNT_GUID = "accountGuid";
  /**
   * The reader guid in a command. Specifies the guid attempting to access a field for reading.
   */
  public final static String READER = "reader";
  /**
   * The writer guid in a command. Specifies the guid attempting to access a field for writing.
   */
  public final static String WRITER = "writer";
  /**
   * The accesser guid in a command. Specifies the guid attempting to access a field for reading or writing.
   */
  public final static String ACCESSER = "accesser";
  /**
   * The field being accessed in a command.
   */
  public final static String FIELD = "field";
  /**
   * The fields being accessed in a command.
   */
  public final static String FIELDS = "fields";
  /**
   * The value being updated in a command.
   */
  public final static String VALUE = "value";
  /**
   * The old value in a command. Used in substitution commands.
   */
  public final static String OLD_VALUE = "oldvalue";
  /**
   * The JSON Object in a command.
   */
  public final static String USER_JSON = "userjson";
  /**
   * The generic argument in a command.
   */
  public final static String ARGUMENT = "argument";
  /**
   * Used to represent an array element being accessed in a command.
   */
  public final static String N = "n";
  /**
   * The member field in a command. The member of a group guid.
   */
  public final static String MEMBER = "member";
  /**
   * The member field in a command. The members of a group guid.
   */
  public final static String MEMBERS = "members";
  /**
   * The acl type field in a command.
   */
  public final static String ACL_TYPE = "aclType";
  /**
   * The public key field in a command.
   */
  public final static String PUBLIC_KEY = "publickey";
  /**
   * The public keys list field in a command.
   */
  public final static String PUBLIC_KEYS = "publickeys";
  /**
   * The password field in a command.
   */
  public final static String PASSWORD = "password";
  /**
   * The code field in a command. Used for account verification.
   */
  public final static String CODE = "code";
  /**
   * The signature field in a command.
   */
  public final static String SIGNATURE = "signature";
  // select commands
  /**
   * The signature field in a command. Used for select within.
   */
  public final static String WITHIN = "within";
  /**
   * The signature field in a command. Used for select near.
   */
  public final static String NEAR = "near";
  /**
   * The max distance field in a command. Used for select near.
   */
  public final static String MAX_DISTANCE = "maxDistance";
  /**
   * The signature field in a command. Used for select query.
   */
  public final static String QUERY = "query";
  /**
   * The interval value in a command. Used for select commands
   * to determine refresh interval.
   */
  public final static String INTERVAL = "interval";
  /**
   * Used in commands to represent the use of all fields.
   */
  public final static String ALL_FIELDS = "+ALL+";
  /**
   * Used in commands to represent the use of all guids.
   */
  public final static String ALL_GUIDS = "+ALL+";
  /**
   * Used in commands to represent the use of all guids.
   */
  public final static String EVERYONE = "+ALL+";

  // admin commands
  /**
   * The logging level in a command.
   */
  public final static String LOG_LEVEL = "level";
  /**
   * Represents the number of guids in a command.
   */
  public final static String GUIDCNT = "guidCnt";

  //
  // Command packet fields
  //
  /**
   * The timestamp field in a command packet.
   */
  public final static String TIMESTAMP = "timestamp";
  /**
   * The sequence number field in a command packet.
   */
  public final static String SEQUENCE_NUMBER = "seqnum";
  /**
   * The passkey number field in a command packet.
   */
  public final static String PASSKEY = "passkey";
  /**
   * The message that was signed field in a command packet.
   */
  public final static String SIGNATUREFULLMESSAGE = "_signatureFullMessage_";

  /* arun: a static final MAGIC_STRING is a security hole. No can do.
  
//  /**
//   * The magic string field in a command packet. The magic string indicates to the
//   * server that this command packet was sent by another server and does not
//   * need to be authenticated.
//   */
//  public final static String MAGIC_STRING = "magic";

  // Special fields for ACL
  public final static String GROUP_ACL = "+GROUP_ACL+";
  // Field names in guid record JSON Object
  /**
   * The public key field in a guid record.
   */
  public static final String GUID_RECORD_PUBLICKEY = "publickey";
  /**
   * The name field in a guid record.
   */
  public static final String GUID_RECORD_NAME = "name";
  /**
   * The guid field in a guid record.
   */
  public static final String GUID_RECORD_GUID = "guid";
  /**
   * The type field in a guid record.
   */
  public static final String GUID_RECORD_TYPE = "type";
  /**
   * The created field in a guid record.
   */
  public static final String GUID_RECORD_CREATED = "created";
  /**
   * The updated field in a guid record.
   */
  public static final String GUID_RECORD_UPDATED = "updated";
  /**
   * The tags field in a guid record.
   */
  public static final String GUID_RECORD_TAGS = "tags";
  // Field names in account record JSON Object
  /**
   * The verified field in a account record.
   */
  public static final String ACCOUNT_RECORD_VERIFIED = "verified";
  /**
   * The guids field in a account record.
   */
  public static final String ACCOUNT_RECORD_GUIDS = "guids";
  /**
   * The guid field in a account record.
   */
  public static final String ACCOUNT_RECORD_GUID = "guid";
  /**
   * The username field in a account record.
   */
  public static final String ACCOUNT_RECORD_USERNAME = "username";
  /**
   * The created field in a account record.
   */
  public static final String ACCOUNT_RECORD_CREATED = "created";
  /**
   * The updated field in a account record.
   */
  public static final String ACCOUNT_RECORD_UPDATED = "updated";
  /**
   * The type field in a account record.
   */
  public static final String ACCOUNT_RECORD_TYPE = "type";
  /**
   * The password field in a account record.
   */
  public static final String ACCOUNT_RECORD_PASSWORD = "password";
  /**
   * The aliases field in a account record.
   */
  public static final String ACCOUNT_RECORD_ALIASES = "aliases";

  // Blessed field names
  /**
   * The standard name of the legacy location field of a record.
   */
  public static final String LOCATION_FIELD_NAME = "geoLocation";
  /**
   * The standard name of the location field of a record.
   */
  public static final String LOCATION_FIELD_NAME_2D_SPHERE = "geoLocationCurrent";
  /**
   * The standard name of the ipaddress field of a record.
   */
  public static final String IPADDRESS_FIELD_NAME = "netAddress";
  /**
   * The preferred way to indicate the command type is a command packet.
   */
  public final static String COMMAND_INT = "COMMANDINT";
  /**
   * Can be used by clients to indicate the command type in a command packet.
   */
  public final static String COMMANDNAME = "COMMANDNAME";

  /**
   * If this exists in a command it indicates that coordinated reads should be used.
   */
  public final static String COORDINATE_READS = "COORDREAD";

  /**
   * This member was not documented by it's creator.
   */
  public final static String AC_ACTION = "acAction";
  /**
   * This member was not documented by it's creator.
   */
  public final static String AC_CODE = "acCode";
  //
  /**
   * The prefix used to hide GNS internal fields.
   */
  public static final String INTERNAL_PREFIX = "_GNS_";

  /**
   * Creates a GNS field that is hidden from the user.
   *
   * @param string
   * @return
   */
  public static String makeInternalField(String string) {
    return INTERNAL_PREFIX + string;
  }

  /**
   * Returns true if field is a GNS field that is hidden from the user.
   *
   * @param key
   * @return
   */
  public static boolean isInternalField(String key) {
    return key.startsWith(INTERNAL_PREFIX);
  }

  /**
   * The newline character in a string.
   */
  public final static String NEWLINE = System.getProperty("line.separator");

}
