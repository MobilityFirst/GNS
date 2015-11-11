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

import java.util.Arrays;
import java.util.List;

/**
 * This class defines a GnsProtocol. Which is to say that
 * it defines a bunch of constants that define the protocol
 * support for communicating between the client and the local name server.
 *
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class GnsProtocol {

  public final static String REGISTER_ACCOUNT = "registerAccount";
  public final static String VERIFY_ACCOUNT = "verifyAccount";
  public final static String REMOVE_ACCOUNT = "removeAccount";
  public final static String LOOKUP_GUID = "lookupGuid";
  public final static String LOOKUP_PRIMARY_GUID = "lookupPrimaryGuid";
  public final static String LOOKUP_GUID_RECORD = "lookupGuidRecord";
  public final static String LOOKUP_ACCOUNT_RECORD = "lookupAccountRecord";
  public final static String RESET_KEY = "resetKey";
  public final static String ADD_ALIAS = "addAlias";
  public final static String REMOVE_ALIAS = "removeAlias";
  public final static String RETRIEVE_ALIASES = "retrieveAliases";
  public final static String ADD_GUID = "addGuid";
  public final static String REMOVE_GUID = "removeGuid";
  public final static String SET_PASSWORD = "setPassword";
  //
  // new
  public final static String CREATE = "create";
  public final static String REMOVE_FIELD = "removeField";
  public final static String APPEND_OR_CREATE = "appendOrCreate";
  public final static String REPLACE = "replace";
  public final static String REPLACE_OR_CREATE = "replaceOrCreate";
  public final static String APPEND_WITH_DUPLICATION = "appendDup";
  public final static String APPEND = "append";
  public final static String REMOVE = "remove";
  public final static String CREATE_LIST = "createList";
  public final static String APPEND_OR_CREATE_LIST = "appendOrCreateList";
  public final static String REPLACE_OR_CREATE_LIST = "replaceOrCreateList";
  public final static String REPLACE_LIST = "replaceList";
  public final static String APPEND_LIST_WITH_DUPLICATION = "appendListDup";
  public final static String APPEND_LIST = "appendList";
  public final static String REMOVE_LIST = "removeList";
  public final static String SUBSTITUTE = "substitute";
  public final static String SUBSTITUTE_LIST = "substituteList";
  public final static String SET = "set";
  public final static String SET_FIELD_NULL = "setFieldNull";
  public final static String CLEAR = "clear";
  public final static String READ = "newRead";
  public final static String NEWREAD = "newRead";
  public final static String READ_ARRAY = "readArray";
  public final static String READ_ARRAY_ONE = "readArrayOne";
  public final static String SELECT = "select";
  public final static String SELECT_GROUP = "selectGroup";
  public final static String WITHIN = "within";
  public final static String NEAR = "near";
  public final static String QUERY = "query";
  public final static String INTERVAL = "interval";
  public final static String REPLACE_USER_JSON = "replaceUserJSON";
  public final static String MAX_DISTANCE = "maxDistance";
  //
  public final static String ACL_ADD = "aclAdd";
  public final static String ACL_REMOVE = "aclRemove";
  public final static String ACL_RETRIEVE = "aclRetrieve";
  // tagss
  public final static String ADD_TAG = "addTag";
  public final static String REMOVE_TAG = "removeTag";
  public final static String CLEAR_TAGGED = "clearTagged";
  public final static String GET_TAGGED = "getTagged";
  public final static String CREATE_GROUP = "createGroup";
  public final static String LOOKUP_GROUP = "lookupGroup";
  public final static String ADD_TO_GROUP = "addToGroup";
  public final static String REMOVE_FROM_GROUP = "removeFromGroup";
  public final static String GET_GROUP_MEMBERS = "getGroupMembers";
  public final static String GET_GROUPS = "getGroups";
  //
  public final static String REQUEST_JOIN_GROUP = "requestJoinGroup";
  public final static String RETRIEVE_GROUP_JOIN_REQUESTS = "retrieveGroupJoinRequests";
  public final static String GRANT_MEMBERSHIP = "grantMembership";
  public final static String REQUEST_LEAVE_GROUP = "requestLeaveGroup";
  public final static String RETRIEVE_GROUP_LEAVE_REQUESTS = "retrieveGroupLeaveRequests";
  public final static String REVOKE_MEMBERSHIP = "revokeMembership";
  //
  public final static String HELP = "help";
  // Admin commands (some not accesible in unless the server is in "admin mode")
  public final static String ADMIN = "admin";
  public final static String DELETE_ALL_RECORDS = "deleteAllRecords";
  public final static String RESET_DATABASE = "resetDatabase";
  public final static String CLEAR_CACHE = "clearCache";
  public final static String DUMPCACHE = "dumpCache";
  public final static String DELETE_ALL_GUID_RECORDS = "deleteAllGuidRecords";
  public final static String DUMP = "dump";
  public final static String CONNECTION_CHECK = "connectionCheck";
  public final static String PING_TABLE = "pingTable";
  public final static String PING_VALUE = "pingValue";
  public final static String CHANGE_LOG_LEVEL = "changeLogLevel";
  public final static String SET_PARAMETER = "setParameter";
  public final static String GET_PARAMETER = "getParameter";
  public final static String LIST_PARAMETERS = "listParameters";
  public final static String BATCH_TEST = "batchTest";
  public final static String RTT_TEST = "rttTest";
  public final static String LEVEL = "level";
  public final static String GUIDCNT = "guidCnt";
  //
  public final static String OK_RESPONSE = "+OK+";
  public final static String NULL_RESPONSE = "+NULL+";
  public final static String BAD_RESPONSE = "+NO+";
  public final static String BAD_SIGNATURE = "+BAD_SIGNATURE+";
  public final static String ACCESS_DENIED = "+ACCESS_DENIED+";
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
  public final static String ALL_FIELDS = "+ALL+";
  public final static String ALL_USERS = "+ALL+";
  public final static String EVERYONE = "+ALL+";
  //
  public static final String RSA_ALGORITHM = "RSA";
  public static final String SIGNATURE_ALGORITHM = "SHA1withRSA";
  //
  public final static String NAME = "name";
  public final static String NAMES = "names";
  public final static String GUID = "guid";
  public final static String GUID_2 = "guid2";
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
  public final static String N2 = "n2";
  public final static String MEMBER = "member";
  public final static String MEMBERS = "members";
  public final static String ACL_TYPE = "aclType";
  public final static String PUBLIC_KEY = "publickey";
  public final static String PUBLIC_KEYS = "publickeys";
  public final static String PASSWORD = "password";
  public final static String CODE = "code";
  public final static String SIGNATURE = "signature";
  public final static String PASSKEY = "passkey";
  public final static String SIGNATUREFULLMESSAGE = "_signatureFullMessage_";
  // Special fields for ACL
  public final static String GUID_ACL = "+GUID_ACL+";
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
  // This one is special, used for the action part of the command
  public final static String COMMANDNAME = "COMMANDNAME"; // aka "action"

   // Active code actions and fields
  public final static String  AC_SET                        = "acSet";
  public final static String  AC_GET                        = "acGet";
  public final static String  AC_CLEAR                      = "acClear";
  public final static String  AC_ACTION                     = "acAction";
  public final static String  AC_CODE                       = "acCode";
 //
  /**
   * This class defines AccessType for ACLs
   *
   * @version 1.0
   */
  public enum AccessType {

    /**
     * Whitelist of GUIDs authorized to read a field
     */
    READ_WHITELIST,
    /**
     * Whitelist of GUIDs authorized to write/update a field
     */
    WRITE_WHITELIST,
    /**
     * Black list of GUIDs not authorized to read a field
     */
    READ_BLACKLIST,
    /**
     * Black list of GUIDs not authorized to write/update a field
     */
    WRITE_BLACKLIST;

    public static String typesToString() {
      StringBuilder result = new StringBuilder();
      String prefix = "";
      for (AccessType type : AccessType.values()) {
        result.append(prefix);
        result.append(type.name());
        prefix = ", ";
      }
      return result.toString();
    }
  };

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
   * The list of command types that are updated commands.
   */
  public final static List<String> UPDATE_COMMANDS
          = Arrays.asList(CREATE, APPEND_OR_CREATE, REPLACE, REPLACE_OR_CREATE, APPEND_WITH_DUPLICATION,
                  APPEND, REMOVE, CREATE_LIST, APPEND_OR_CREATE_LIST, REPLACE_OR_CREATE_LIST, REPLACE_LIST,
                  APPEND_LIST_WITH_DUPLICATION, APPEND_LIST, REMOVE_LIST, SUBSTITUTE, SUBSTITUTE_LIST,
                  SET, SET_FIELD_NULL, CLEAR, REMOVE_FIELD, REPLACE_USER_JSON,
                  //
                  //REGISTERACCOUNT, REMOVEACCOUNT, ADDGUID, REMOVEGUID, ADDALIAS, REMOVEALIAS, 
                  VERIFY_ACCOUNT, SET_PASSWORD,
                  //
                  ACL_ADD, ACL_REMOVE,
                  //ADDTOGROUP, REMOVEFROMGROUP,
                  //
                  ADD_TAG, REMOVE_TAG
          );
  // Currently unused

  /**
   * The list of command types that are read commands.
   */
  public final static List<String> READ_COMMANDS
          = Arrays.asList(READ_ARRAY, NEWREAD, READ_ARRAY_ONE);
  //
  
  public final static String NEWLINE = System.getProperty("line.separator");
}
