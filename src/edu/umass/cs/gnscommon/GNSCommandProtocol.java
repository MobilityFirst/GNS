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
  // Reponse codes
  //
  public final static String OK_RESPONSE = "+OK+";
  public final static String NULL_RESPONSE = "+NULL+";
  public final static String BAD_RESPONSE = "+NO+";
  public final static String BAD_SIGNATURE = "+BAD_SIGNATURE+";
  public final static String ACCESS_DENIED = "+ACCESS_DENIED+";
  public final static String STALE_COMMMAND = "+STALE_COMMMAND+";
  public final static String OPERATION_NOT_SUPPORTED = "+OPERATIONNOTSUPPORTED+";
  public final static String QUERY_PROCESSING_ERROR = "+QUERYPROCESSINGERROR+";
  public final static String VERIFICATION_ERROR = "+VERIFICATIONERROR+";
  public final static String NO_ACTION_FOUND = "+NOACTIONFOUND+";
  public final static String BAD_ACCESSOR_GUID = "+BADACCESSORGUID+";
  public final static String BAD_GUID = "+BADGUID+";
  public final static String BAD_ACCOUNT = "+BADACCOUNT+";
  public final static String BAD_USER = "+BADUSER+";
  public final static String BAD_GROUP = "+BADGROUP+";
  public final static String BAD_FIELD = "+BADFIELD+";
  public final static String BAD_ALIAS = "+BADALIAS+";
  public final static String BAD_ACL_TYPE = "+BADACLTYPE+";
  public final static String FIELD_NOT_FOUND = "+FIELDNOTFOUND+";
  public final static String DUPLICATE_USER = "+DUPLICATEUSER+";
  public final static String DUPLICATE_GUID = "+DUPLICATEGUID+";
  public final static String DUPLICATE_GROUP = "+DUPLICATEGROUP+";
  public final static String DUPLICATE_FIELD = "+DUPLICATEFIELD+";
  public final static String DUPLICATE_NAME = "+DUPLICATENAME+";
  // arun: corresponds to gigapaxos duplicate name creation error
  public final static String DUPLICATE_ERROR = "+"
          + ClientReconfigurationPacket.ResponseCodes.DUPLICATE_ERROR
          .toString() + "+";
  public final static String JSON_PARSE_ERROR = "+JSONPARSEERROR+";
  public final static String TOO_MANY_ALIASES = "+TOMANYALIASES+";
  public final static String TOO_MANY_GUIDS = "+TOMANYGUIDS+";
  public final static String UPDATE_ERROR = "+UPDATEERROR+";
  public final static String UPDATE_TIMEOUT = "+UPDATETIMEOUT+";
  public final static String SELECTERROR = "+SELECTERROR+";
  public final static String GENERIC_ERROR = "+GENERICERROR+";
  public final static String FAIL_ACTIVE_NAMESERVER = "+FAIL_ACTIVE+";
  public final static String INVALID_ACTIVE_NAMESERVER = "+INVALID_ACTIVE+";
  public final static String TIMEOUT = "+TIMEOUT+";

  public final static String ACTIVE_REPLICA_EXCEPTION
          = ClientReconfigurationPacket.ResponseCodes.ACTIVE_REPLICA_EXCEPTION.toString();

  //
  public static final String RSA_ALGORITHM = "RSA";
  public static final String SIGNATURE_ALGORITHM = "SHA1withRSA";
  public static final String DIGEST_ALGORITHM = "SHA1";
  public static final String SECRET_KEY_ALGORITHM = "DESede";
  /**
   * The character set used by client and server for reading and writing command packets.
   */
  public static final String CHARSET = "ISO-8859-1";

  //
  // Argument field in commands.
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
  public final static String READER = "reader";
  public final static String WRITER = "writer";
  public final static String ACCESSER = "accesser";
  public final static String FIELD = "field";
  public final static String FIELDS = "fields";
  public final static String VALUE = "value";
  public final static String OLD_VALUE = "oldvalue";
  public final static String USER_JSON = "userjson";
  public final static String ARGUMENT = "argument";
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
   * The signature field in a command. Used for select commands
   * to determine refresh interval.
   */
  public final static String INTERVAL = "interval";
  
  public final static String ALL_FIELDS = "+ALL+";
  public final static String ALL_USERS = "+ALL+";
  public final static String EVERYONE = "+ALL+";

  // admin commands
  public final static String LEVEL = "level";
  public final static String GUIDCNT = "guidCnt";

  public final static String SK_CERTIFICATE = "sk_certificate";

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
  /**
   * The magic string field in a command packet. The magic string indicates to the
   * server that this command packet was sent by another server and does not
   * need to be authenticated.
   */
  public final static String MAGIC_STRING = "magic";

  // Special fields for ACL
  public final static String GROUP_ACL = "+GROUP_ACL+";
  // Field names in guid record JSON Object
  public static final String GUID_RECORD_PUBLICKEY = "publickey";
  public static final String GUID_RECORD_NAME = "name";
  public static final String GUID_RECORD_GUID = "guid";
  public static final String GUID_RECORD_TYPE = "type";
  public static final String GUID_RECORD_CREATED = "created";
  public static final String GUID_RECORD_UPDATED = "updated";
  public static final String GUID_RECORD_TAGS = "tags";
  // Field names in account record JSON Object
  public static final String ACCOUNT_RECORD_VERIFIED = "verified";
  public static final String ACCOUNT_RECORD_GUIDS = "guids";
  public static final String ACCOUNT_RECORD_GUID = "guid";
  public static final String ACCOUNT_RECORD_USERNAME = "username";
  public static final String ACCOUNT_RECORD_CREATED = "created";
  public static final String ACCOUNT_RECORD_UPDATED = "updated";
  public static final String ACCOUNT_RECORD_TYPE = "type";
  public static final String ACCOUNT_RECORD_PASSWORD = "password";
  public static final String ACCOUNT_RECORD_ALIASED = "aliases";

  // Blessed field names
  public static final String LOCATION_FIELD_NAME = "geoLocation";
  public static final String LOCATION_FIELD_NAME_2D_SPHERE = "geoLocationCurrent";
  public static final String IPADDRESS_FIELD_NAME = "netAddress";
  /**
   * The preferred way to indicate the command type is a command packet.
   */
  public final static String COMMAND_INT = "COMMANDINT";
  /**
   * Can be used by clients to indicate the command type in a command packet.
   */
  public final static String COMMANDNAME = "COMMANDNAME";

  public final static String COORDINATE_READS = "COORDREAD";

  public final static String AC_ACTION = "acAction";
  public final static String AC_CODE = "acCode";
  //

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

  public final static String NEWLINE = System.getProperty("line.separator");

  //public static final boolean USE_SECRET_KEY = false;
}
