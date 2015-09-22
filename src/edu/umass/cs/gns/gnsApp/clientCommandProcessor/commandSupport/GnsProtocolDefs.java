/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport;

import java.util.Arrays;
import java.util.List;

/**
 *
 * @author westy
 */
public class GnsProtocolDefs {

  /**
   *
   */
  public final static String REGISTERACCOUNT = "registerAccount";

  /**
   *
   */
  public final static String VERIFYACCOUNT = "verifyAccount";

  /**
   *
   */
  public final static String ADDALIAS = "addAlias";

  /**
   *
   */
  public final static String REMOVEALIAS = "removeAlias";

  /**
   *
   */
  public final static String RETRIEVEALIASES = "retrieveAliases";

  /**
   *
   */
  public final static String ADDGUID = "addGuid";

  /**
   *
   */
  public final static String REMOVEGUID = "removeGuid";

  /**
   *
   */
  public final static String SETPASSWORD = "setPassword";

  /**
   *
   */
  public final static String REMOVEACCOUNT = "removeAccount";

  /**
   *
   */
  public final static String LOOKUPGUID = "lookupGuid";

  /**
   *
   */
  public final static String LOOKUPGUIDRECORD = "lookupGuidRecord";

  /**
   *
   */
  public final static String LOOKUPACCOUNTRECORD = "lookupAccountRecord";

  /**
   *
   */
  public final static String LOOKUPPRIMARYGUID = "lookupPrimaryGuid";

  /**
   *
   */
  public final static String RESETKEY = "resetKey";
  // update operations

  /**
   *
   */
  public final static String CREATE = "create";

  /**
   *
   */
  public final static String APPENDORCREATE = "appendOrCreate";

  /**
   *
   */
  public final static String REPLACE = "replace";

  /**
   *
   */
  public final static String REPLACEORCREATE = "replaceOrCreate";

  /**
   *
   */
  public final static String APPENDWITHDUPLICATION = "appendDup";

  /**
   *
   */
  public final static String APPEND = "append";

  /**
   *
   */
  public final static String REMOVE = "remove";

  /**
   *
   */
  public final static String CREATELIST = "createList";

  /**
   *
   */
  public final static String APPENDORCREATELIST = "appendOrCreateList";

  /**
   *
   */
  public final static String REPLACEORCREATELIST = "replaceOrCreateList";

  /**
   *
   */
  public final static String REPLACELIST = "replaceList";

  /**
   *
   */
  public final static String APPENDLISTWITHDUPLICATION = "appendListDup";

  /**
   *
   */
  public final static String APPENDLIST = "appendList";

  /**
   *
   */
  public final static String REMOVELIST = "removeList";

  /**
   *
   */
  public final static String SUBSTITUTE = "substitute";

  /**
   *
   */
  public final static String SUBSTITUTELIST = "substituteList";

  /**
   *
   */
  public final static String SET = "set";

  /**
   *
   */
  public final static String SETFIELDNULL = "setFieldNull";

  /**
   *
   */
  public final static String CLEAR = "clear";

  /**
   *
   */
  public final static String REMOVEFIELD = "removeField";

  /**
   *
   */
  public final static String REPLACEUSERJSON = "replaceUserJSON";

  //
  /**
   *
   */
  public final static String READARRAY = "readArray";

  /**
   *
   */
  public final static String NEWREAD = "newRead";

  /**
   *
   */
  public final static String READARRAYONE = "readArrayOne";
  //

  /**
   *
   */
  public final static String SELECT = "select";

  /**
   *
   */
  public final static String SELECTGROUP = "selectGroup";

  /**
   *
   */
  public final static String WITHIN = "within";

  /**
   *
   */
  public final static String NEAR = "near";

  /**
   *
   */
  public final static String QUERY = "query";
  // Arguments

  /**
   *
   */
  public final static String INTERVAL = "interval";

  /**
   *
   */
  public final static String MAXDISTANCE = "maxDistance";
  //

  /**
   *
   */
  public final static String ACLADD = "aclAdd";

  /**
   *
   */
  public final static String ACLREMOVE = "aclRemove";

  /**
   *
   */
  public final static String ACLRETRIEVE = "aclRetrieve";

  /**
   *
   */
  public final static String ADDTOGROUP = "addToGroup";

  /**
   *
   */
  public final static String REMOVEFROMGROUP = "removeFromGroup";

  /**
   *
   */
  public final static String GETGROUPMEMBERS = "getGroupMembers";

  /**
   *
   */
  public final static String GETGROUPS = "getGroups";
  //

  /**
   *
   */
  public final static String REQUESTJOINGROUP = "requestJoinGroup";

  /**
   *
   */
  public final static String RETRIEVEGROUPJOINREQUESTS = "retrieveGroupJoinRequests";

  /**
   *
   */
  public final static String GRANTMEMBERSHIP = "grantMembership";

  /**
   *
   */
  public final static String REQUESTLEAVEGROUP = "requestLeaveGroup";

  /**
   *
   */
  public final static String RETRIEVEGROUPLEAVEREQUESTS = "retrieveGroupLeaveRequests";

  /**
   *
   */
  public final static String REVOKEMEMBERSHIP = "revokeMembership";
  //

  /**
   *
   */
  public final static String HELP = "help";
  // admin commands 

  /**
   *
   */
  public final static String ADMIN = "admin";

  /**
   *
   */
  public final static String DELETEALLRECORDS = "deleteAllRecords";

  /**
   *
   */
  public final static String RESETDATABASE = "resetDatabase";

  /**
   *
   */
  public final static String CLEARCACHE = "clearCache";

  /**
   *
   */
  public final static String DUMPCACHE = "dumpCache";

  /**
   *
   */
  public final static String PINGTABLE = "pingTable";

  /**
   *
   */
  public final static String PINGVALUE = "pingValue";

  /**
   *
   */
  public final static String CHANGELOGLEVEL = "changeLogLevel";

  /**
   *
   */
  public final static String DUMP = "dump";

  /**
   *
   */
  public final static String CONNECTIONCHECK = "connectionCheck";

  /**
   *
   */
  public final static String ADDTAG = "addTag";

  /**
   *
   */
  public final static String REMOVETAG = "removeTag";

  /**
   *
   */
  public final static String GETTAGGED = "getTagged";

  /**
   *
   */
  public final static String CLEARTAGGED = "clearTagged";

  /**
   *
   */
  public final static String RTTTEST = "rttTest";
  // Arguments for admin commands

  /**
   *
   */
  public final static String BATCH = "batch";
  // Arguments for admin commands

  /**
   *
   */
  public final static String LEVEL = "level";

  /**
   *
   */
  public final static String SETPARAMETER = "setParameter";

  /**
   *
   */
  public final static String GETPARAMETER = "getParameter";

  /**
   *
   */
  public final static String LISTPARAMETERS = "listParameters";

  /**
   *
   */
  public final static String N = "n";

  /**
   *
   */
  public final static String N2 = "n2";

  /**
   *
   */
  public final static String GUIDCNT = "guidCnt";
  // Responses

  /**
   *
   */
  public final static String OKRESPONSE = "+OK+";

  /**
   *
   */
  public final static String NULLRESPONSE = "+NULL+";

  /**
   *
   */
  public final static String BADRESPONSE = "+NO+";

  /**
   *
   */
  public final static String BADSIGNATURE = "+BADSIGNATURE+";

  /**
   *
   */
  public final static String ACCESSDENIED = "+ACCESSDENIED+";

  /**
   *
   */
  public final static String OPERATIONNOTSUPPORTED = "+OPERATIONNOTSUPPORTED+";

  /**
   *
   */
  public final static String QUERYPROCESSINGERROR = "+QUERYPROCESSINGERROR+";

  /**
   *
   */
  public final static String NOACTIONFOUND = "+NOACTIONFOUND+";

  /**
   *
   */
  public final static String BADACCESSORGUID = "+BADACCESSORGUID+";

  /**
   *
   */
  public final static String BADGUID = "+BADGUID+";

  /**
   *
   */
  public final static String BADALIAS = "+BADALIAS+";

  /**
   *
   */
  public final static String BADACCOUNT = "+BADACCOUNT+";

  /**
   *
   */
  public final static String BADGROUP = "+BADGROUP+";

  /**
   *
   */
  public final static String BADFIELD = "+BADFIELD+";

  /**
   *
   */
  public final static String BADACLTYPE = "+BADACLTYPE+";

  /**
   *
   */
  public final static String FIELDNOTFOUND = "+FIELDNOTFOUND+";

  /**
   *
   */
  public final static String DUPLICATENAME = "+DUPLICATENAME+";

  /**
   *
   */
  public final static String DUPLICATEGUID = "+DUPLICATEGUID+";

  /**
   *
   */
  public final static String DUPLICATEGROUP = "+DUPLICATEGROUP+";

  /**
   *
   */
  public final static String DUPLICATEFIELD = "+DUPLICATEFIELD+";

  /**
   *
   */
  public final static String JSONPARSEERROR = "+JSONPARSEERROR+";

  /**
   *
   */
  public final static String VERIFICATIONERROR = "+VERIFICATIONERROR+";

  /**
   *
   */
  public final static String TOMANYALIASES = "+TOMANYALIASES+";

  /**
   *
   */
  public final static String TOMANYGUIDS = "+TOMANYGUIDS+";

  /**
   *
   */
  public final static String UPDATEERROR = "+UPDATEERROR+";

  /**
   *
   */
  public final static String UPDATETIMEOUT = "+UPDATETIMEOUT+";

  /**
   *
   */
  public final static String SELECTERROR = "+SELECTERROR+";

  /**
   *
   */
  public final static String GENERICERROR = "+GENERICERROR+";

  /**
   *
   */
  public final static String FAIL_ACTIVE_NAMESERVER = "+FAIL_ACTIVE+";

  /**
   *
   */
  public final static String INVALID_ACTIVE_NAMESERVER = "+INVALID_ACTIVE+";
  // Special values

  /**
   *
   */
  public final static String ALLFIELDS = "+ALL+";

  /**
   *
   */
  public final static String EVERYONE = "+ALL+";

  /**
   *
   */
  public final static String EMPTY = "+EMPTY+";
  //

  /**
   *
   */
  public static final String RSAALGORITHM = "RSA";

  /**
   *
   */
  public static final String SIGNATUREALGORITHM = "SHA1withRSA";

  /**
   *
   */
  public final static String NEWLINE = System.getProperty("line.separator");
  // This one is special, used for the action part of the command

  /**
   *
   */
  public final static String COMMANDNAME = "COMMANDNAME"; // aka "action"
  // Arguments 

  /**
   *
   */
  public final static String NAME = "name";

  /**
   *
   */
  public final static String NAMES = "names";

  /**
   *
   */
  public final static String GUID = "guid";

  /**
   *
   */
  public final static String GUID2 = "guid2";

  /**
   *
   */
  public final static String ACCOUNT_GUID = "accountGuid";

  /**
   *
   */
  public final static String READER = "reader";

  /**
   *
   */
  public final static String WRITER = "writer";

  /**
   *
   */
  public final static String APPGUID = "appGuid";

  /**
   *
   */
  public final static String ACCESSER = "accesser";

  /**
   *
   */
  public final static String FIELD = "field";

  /**
   *
   */
  public final static String FIELDS = "fields";

  /**
   *
   */
  public final static String VALUE = "value";

  /**
   *
   */
  public final static String OLDVALUE = "oldvalue";

  /**
   *
   */
  public final static String USERJSON = "userjson";

  /**
   *
   */
  public final static String ARGUMENT = "argument";

  /**
   *
   */
  public final static String MEMBER = "member";

  /**
   *
   */
  public final static String MEMBERS = "members";
  // Fields for HTTP get queries

  /**
   *
   */
  public final static String ACLTYPE = "aclType";
  // Special fields for ACL 

  /**
   *
   */
  public final static String GUID_ACL = "+GUID_ACL+";

  /**
   *
   */
  public final static String GROUP_ACL = "+GROUP_ACL+";
  //public final static String JSONSTRING = "jsonstring";

  /**
   *
   */
  public final static String GROUP = "group";

  /**
   *
   */
  public final static String PUBLIC_KEY = "publickey";
  
   /**
   *
   */
  public final static String PUBLIC_KEYS = "publickeys";

  /**
   *
   */
  public final static String PASSWORD = "password";

  /**
   *
   */
  public final static String CODE = "code";

  /**
   *
   */
  public final static String SIGNATURE = "signature";

  /**
   *
   */
  public final static String PASSKEY = "passkey";
  // Internal use for signature

  /**
   *
   */
  public final static String SIGNATUREFULLMESSAGE = "_signatureFullMessage_";
  // Blessed field names

  /**
   *
   */
  public static final String LOCATION_FIELD_NAME = "geoLocation";

  /**
   *
   */
  public static final String LOCATION_FIELD_NAME_2D_SPHERE = "geoLocationCurrent";

  /**
   *
   */
  public static final String IPADDRESS_FIELD_NAME = "netAddress";

  /**
   *
   */
  public static final String ACL_FIELD_NAME = "ACL";
  // Used by CommandPacket to control the needsCoordination method
  // Note: That any command that does updates to multiple records is handled
  // by explicitly sending those updates to ARs

  /**
   * The list of command types that are updated commands.
   */
  public final static List<String> UPDATE_COMMANDS
          = Arrays.asList(CREATE, APPENDORCREATE, REPLACE, REPLACEORCREATE, APPENDWITHDUPLICATION,
                  APPEND, REMOVE, CREATELIST, APPENDORCREATELIST, REPLACEORCREATELIST, REPLACELIST,
                  APPENDLISTWITHDUPLICATION, APPENDLIST, REMOVELIST, SUBSTITUTE, SUBSTITUTELIST,
                  SET, SETFIELDNULL, CLEAR, REMOVEFIELD, REPLACEUSERJSON,
                  //
                  //REGISTERACCOUNT, REMOVEACCOUNT, ADDGUID, REMOVEGUID, ADDALIAS, REMOVEALIAS, 
                  VERIFYACCOUNT, SETPASSWORD,
                  //
                  ACLADD, ACLREMOVE,
                  //ADDTOGROUP, REMOVEFROMGROUP,
                  //
                  ADDTAG, REMOVETAG
          );
  // Currently unused

  /**
   * The list of command types that are read commands.
   */
  public final static List<String> READ_COMMANDS
          = Arrays.asList(READARRAY, NEWREAD, READARRAYONE);
  //

}
