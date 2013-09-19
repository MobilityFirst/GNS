package edu.umass.cs.gns.httpserver;

import edu.umass.cs.gns.client.AclAccess;
import edu.umass.cs.gns.client.AclAccess.AccessType;
import edu.umass.cs.gns.client.GroupAccess;
import edu.umass.cs.gns.client.Intercessor;
import edu.umass.cs.gns.client.RecordAccess;
import edu.umass.cs.gns.client.AccountAccess;
import edu.umass.cs.gns.client.AccountInfo;
import edu.umass.cs.gns.client.Admintercessor;
import edu.umass.cs.gns.client.GuidInfo;
import static edu.umass.cs.gns.httpserver.Defs.*;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.client.UpdateOperation;
import edu.umass.cs.gns.database.ResultValue;
import edu.umass.cs.gns.util.Base64;
import edu.umass.cs.gns.util.ByteUtils;
import edu.umass.cs.gns.util.JSONUtils;
import edu.umass.cs.gns.util.Util;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONException;

/**
 * Implements the GNS server protocol
 *
 * @author westy
 */
public class Protocol {

  public final static String REGISTERACCOUNT = "registerAccount";
  public final static String ADDALIAS = "addAlias";
  public final static String REMOVEALIAS = "removeAlias";
  public final static String RETRIEVEALIASES = "retrieveAliases";
  public final static String ADDGUID = "addGuid";
  public final static String SETPASSWORD = "setPassword";
  public final static String REMOVEACCOUNT = "removeAccount";
  public final static String LOOKUPGUID = "lookupGuid";
  public final static String LOOKUPGUIDRECORD = "lookupGuidRecord";
  //
  // new
  public final static String CREATE = "create";
  public final static String APPENDORCREATE = "appendOrCreate";
  public final static String REPLACE = "replace";
  public final static String REPLACEORCREATE = "replaceOrCreate";
  public final static String APPENDWITHDUPLICATION = "appendDup";
  public final static String APPEND = "append";
  public final static String REMOVE = "remove";
  public final static String CREATELIST = "createList";
  public final static String APPENDORCREATELIST = "appendOrCreateList";
  public final static String REPLACEORCREATELIST = "replaceOrCreateList";
  public final static String REPLACELIST = "replaceList";
  public final static String APPENDLISTWITHDUPLICATION = "appendListDup";
  public final static String APPENDLIST = "appendList";
  public final static String REMOVELIST = "removeList";
  public final static String SUBSTITUTE = "substitute";
  public final static String SUBSTITUTELIST = "substituteList";
  public final static String CLEAR = "clear";
  public final static String READ = "read";
  public final static String READONE = "readOne";
  //
  public final static String ACLADD = "aclAdd";
  public final static String ACLREMOVE = "aclRemove";
  public final static String ACLRETRIEVE = "aclRetrieve";
  public final static String ADDTOGROUP = "addToGroup";
  public final static String REMOVEFROMGROUP = "removeFromGroup";
  public final static String GETGROUPMEMBERS = "getGroupMembers";
  public final static String HELP = "help";
  // demo commands (not accesible in "public" version")
  public final static String DEMO = "demo";
  public final static String DELETEALLRECORDS = "deleteAllRecords";
  public final static String RESETDATABASE = "resetDatabase";
  public final static String CLEARCACHE = "clearCache";
  public final static String DUMPCACHE = "dumpCache";
  //public final static String DELETEALLGUIDRECORDS = "deleteAllGuidRecords";
  public final static String DUMP = "dump";
  public final static String ADDTAG = "addTag";
  public final static String REMOVETAG = "removeTag";
  public final static String GETTAGGED = "getTagged";
  public final static String CLEARTAGGED = "clearTagged";
  //
  public final static String OKRESPONSE = "+OK+";
  public final static String NULLRESPONSE = "+EMPTY+";
  public final static String BADRESPONSE = "+NO+";
  public final static String BADSIGNATURE = "+BADSIGNATURE+";
  public final static String ACCESSDENIED = "+ACCESSDENIED+";
  public final static String OPERATIONNOTSUPPORTED = "+OPERATIONNOTSUPPORTED+";
  public final static String QUERYPROCESSINGERROR = "+QUERYPROCESSINGERROR+";
  public final static String NOACTIONFOUND = "+NOACTIONFOUND+";
  public final static String BADREADERGUID = "+BADREADERGUID+";
  public final static String BADWRITERGUID = "+BADWRITERGUID+";
  public final static String BADGUID = "+BADGUID+";
  public final static String BADALIAS = "+BADALIAS+";
  public final static String BADACCOUNT = "+BADACCOUNT+";
  public final static String BADGROUP = "+BADGROUP+";
  public final static String BADFIELD = "+BADFIELD+";
  public final static String BADACLTYPE = "+BADACLTYPE+";
  public final static String DUPLICATENAME = "+DUPLICATENAME+";
  public final static String DUPLICATEGUID = "+DUPLICATEGUID+";
  public final static String DUPLICATEGROUP = "+DUPLICATEGROUP+";
  public final static String DUPLICATEFIELD = "+DUPLICATEFIELD+";
  public final static String JSONPARSEERROR = "+JSONPARSEERROR+";
  public final static String TOMANYALIASES = "+TOMANYALIASES+";
  public final static String TOMANYGUIDS = "+TOMANYGUIDS+";
  public final static String UPDATEERROR = "+UPDATEERROR+";
  public final static String GENERICEERROR = "+GENERICEERROR+";
  public final static String ALLFIELDS = "+ALL+";
  public final static String EVERYONE = "+ALL+";
  public final static String EMPTY = "+EMPTY+";
  //
  public static final String RASALGORITHM = "RSA";
  public static final String SIGNATUREALGORITHM = "SHA1withRSA";
  private final static String NEWLINE = System.getProperty("line.separator");
  // Fields for HTTP get queries
  public final static String NAME = "name";
  public final static String GUID = "guid";
  public final static String READER = "reader";
  public final static String WRITER = "writer";
  public final static String APPGUID = "appGuid";
  public final static String ACCESSER = "accesser";
  public final static String FIELD = "field";
  public final static String VALUE = "value";
  public final static String OLDVALUE = "oldvalue";
  public final static String MEMBER = "member";
  // Fields for HTTP get queries
  public final static String ACLTYPE = "aclType";
  // Special fields for ACL 
  public final static String GUID_ACL = "+GUID_ACL+";
  public final static String GROUP_ACL = "+GROUP_ACL+";
  //public final static String JSONSTRING = "jsonstring";
  public final static String GROUP = "group";
  public final static String PUBLICKEY = "publickey";
  public final static String PASSWORD = "password";
  public final static String SIGNATURE = "signature";
  public final static String PASSKEY = "passkey";
  //public final static String TABLE = "table";
  //
  private boolean demoMode = true;
  private RecordAccess recordAccess = RecordAccess.getInstance();
  private AccountAccess accountAccess = AccountAccess.getInstance();
  private AclAccess aclAccess = AclAccess.getInstance();
  //private GroupAccessV1 groupAccessV1 = GroupAccessV1.getInstance();
  private GroupAccess groupAccess = GroupAccess.getInstance();
  //
  // help string for HTTP query

  private String getHelpString(String hostString) {
    String urlPrefix = "http://" + hostString + "/" + GnsHttpServer.GNSPATH + "/";
    String main =
            "Commands are sent as HTTP GET queries." + NEWLINE + NEWLINE
            + "Note: We use the terms field and key interchangably below." + NEWLINE + NEWLINE
            + "Commands:" + NEWLINE
            + urlPrefix + HELP + NEWLINE
            + "  Returns this help message." + NEWLINE + NEWLINE
            //
            //            + urlPrefix + REGISTERACCOUNT + QUERYPREFIX + NAME + VALSEP + "<name>" + KEYSEP + PUBLICKEY + VALSEP + "<publickey>"
            //            + KEYSEP + APPGUID + VALSEP + "<app guid>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            //            + "  Creates a GUID associated with the the human readable name and the supplied publickey. Returns a guid." + NEWLINE
            //            + "  Must be signed by the given application guid." + NEWLINE + NEWLINE
            //            //
            //            + urlPrefix + REGISTERACCOUNT + QUERYPREFIX + NAME + VALSEP + "<name>" + KEYSEP + GUID + VALSEP + "<guid>" + KEYSEP + PUBLICKEY + VALSEP + "<publickey>"
            //            + KEYSEP + APPGUID + VALSEP + "<app guid>" 
            //            + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            //            + "  Associates the GUID supplied with the human readable name and the publickey." + NEWLINE
            //            + "  Must be signed by the given application guid." + NEWLINE + NEWLINE
            //            //
            //            + urlPrefix + DELETEACCOUNT + QUERYPREFIX + NAME + VALSEP + "<name>" + KEYSEP + GUID + VALSEP + "<guid>"
            //            + KEYSEP + APPGUID + VALSEP + "<app guid>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            //            + "  Associates the GUID supplied with the human readable name and the publickey." + NEWLINE
            //            + "  Must be signed by the given application guid." + NEWLINE + NEWLINE
            //            //
            //            + urlPrefix + LOOKUPGUID + QUERYPREFIX + NAME + VALSEP + "<name>"
            //            + KEYSEP + APPGUID + VALSEP + "<app guid>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            //            + "  Returns the guid registered for this human readable name. Returns " + BADACCOUNT + " if the GUID has not been registered." + NEWLINE
            //            + "  Must be signed by the given application guid." + NEWLINE + NEWLINE
            //            //
            //            + urlPrefix + LOOKUPGUIDRECORD + QUERYPREFIX + GUID + VALSEP + "<guid>"
            //            + KEYSEP + APPGUID + VALSEP + "<app guid>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            //            + "  Returns human readable name and public key associated with the given GUID. Returns " + BADGUID + " if the GUID has not been registered." + NEWLINE
            //            + "  Must be signed by the given application guid." + NEWLINE + NEWLINE
            //            //
            //            + "Unsecure versions of above operations - "
            //            + "soon will only work in DEMO mode" + NEWLINE + NEWLINE
            + urlPrefix + REGISTERACCOUNT + QUERYPREFIX + NAME + VALSEP + "<name>" + KEYSEP + PUBLICKEY + VALSEP + "<publickey>" + NEWLINE
            + "  Creates a GUID associated with the the human readable name (a human readable name) and the supplied publickey. Returns a guid." + NEWLINE + NEWLINE
            //
            + urlPrefix + REGISTERACCOUNT + QUERYPREFIX + NAME + VALSEP + "<name>" + KEYSEP + GUID + VALSEP + "<guid>" + KEYSEP + PUBLICKEY + VALSEP + "<publickey>" + NEWLINE
            + "  Associates the GUID supplied with the human readable name (a human readable name for the user) and the publickey." + NEWLINE + NEWLINE
            //
            + urlPrefix + REMOVEACCOUNT + QUERYPREFIX + NAME + VALSEP + "<name>" + KEYSEP + GUID + VALSEP + "<guid>"
            + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Removes the GUID associated with the human readable name. Must be signed by the guid."
            + NEWLINE + NEWLINE
            //
            + urlPrefix + LOOKUPGUID + QUERYPREFIX + NAME + VALSEP + "<name>" + NEWLINE
            + "  Returns the guid associated with for this human readable name. Returns " + BADACCOUNT + " if the GUID has not been registered." + NEWLINE + NEWLINE
            //
            + urlPrefix + LOOKUPGUIDRECORD + QUERYPREFIX + GUID + VALSEP + "<guid>" + NEWLINE
            + "  Returns human readable name and public key associated with the given GUID. Returns " + BADGUID + " if the GUID has not been registered." + NEWLINE + NEWLINE
            //
            + urlPrefix + ADDGUID + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + NAME + VALSEP + "<name>" + KEYSEP + PUBLICKEY + VALSEP + "<publickey>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Adds a GUID to the account associated with the GUID. Must be signed by the guid. Returns " + BADGUID + " if the GUID has not been registered." + NEWLINE + NEWLINE
            //
            + urlPrefix + ADDALIAS + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + NAME + VALSEP + "<alias>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Adds a additional human readble name to the account associated with the GUID. Must be signed by the guid. Returns " + BADGUID + " if the GUID has not been registered." + NEWLINE + NEWLINE
            //
            + urlPrefix + REMOVEALIAS + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + NAME + VALSEP + "<alias>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Removes the alias from the account associated with the GUID. Must be signed by the guid. Returns " + BADGUID + " if the GUID has not been registered." + NEWLINE + NEWLINE
            //
            + "DATABASE CREATE OPERATIONS" + NEWLINE + NEWLINE
            + urlPrefix + CREATE + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + KEYSEP + VALUE + VALSEP + "<value>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Adds a key value pair to the database for the given GUID. See below for more on the signature. "
            //+ "Returns " + BADRESPONSE + " " + BADGUID + " if the GUID has not been registered." 
            + NEWLINE + NEWLINE
            //
            + urlPrefix + CREATELIST + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + KEYSEP + VALUE + VALSEP + "<JSON List>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Adds a key value pair to the database for the given GUID. Value is a list of items formated as a JSON list. See below for more on the signature. "
            //+ "Returns " + BADRESPONSE + " " + BADGUID + " if the GUID has not been registered." 
            + NEWLINE + NEWLINE
            //
            + urlPrefix + CREATE + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Adds a key value pair to the database for the given GUID. See below for more on the signature. "
            //+ "Returns " + BADRESPONSE + " " + BADGUID + " if the GUID has not been registered." 
            + NEWLINE + NEWLINE
            // 
            + "DATABASE UPSERT OPERATIONS - "
            + "are also SET OPERATIONS" + NEWLINE + NEWLINE
            //
            + urlPrefix + APPENDORCREATE + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + KEYSEP + VALUE + VALSEP + "<value>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Adds a key value pair to the database for the given GUID if it doesn not exist otherwise append value onto existing value. See below for more on the signature. "
            + NEWLINE + NEWLINE
            //
            + urlPrefix + APPENDORCREATELIST + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + KEYSEP + VALUE + VALSEP + "<JSON List>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Adds a key value pair to the database for the given GUID if it doesn not exist otherwise appends values onto existing value. Value is a list of items formated as a JSON list."
            + NEWLINE + NEWLINE
            //
            + urlPrefix + REPLACEORCREATE + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + KEYSEP + VALUE + VALSEP + "<value>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Adds a key value pair to the database for the given GUID if it doesn not exist otherwise replaces the value of this key value pair for the given GUID. See below for more on the signature. "
            + NEWLINE + NEWLINE
            //
            + urlPrefix + REPLACEORCREATELIST + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + KEYSEP + VALUE + VALSEP + "<JSON List>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + " Adds a key value pair to the database for the given GUID if it doesn not exist otherwise replaces the value of this key value pair for the given GUID with the value.  Value is a list of items formated as a JSON list."
            //
            + NEWLINE + NEWLINE
            //
            + "DATABASE SET OPERATIONS - "
            + "treat the value associated with the key as a set - duplicate items are removed or not added" + NEWLINE + NEWLINE
            + urlPrefix + APPEND + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + KEYSEP + VALUE + VALSEP + "<value>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Appends the value onto the key value pair for the given GUID. See below for more on the signature. "
            + NEWLINE + NEWLINE
            //
            + urlPrefix + APPENDLIST + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + KEYSEP + VALUE + VALSEP + "<JSON List>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "   Appends the value onto of this key value pair for the given GUID. Value is a list of items formated as a JSON list. See below for more on the signature. "
            + NEWLINE + NEWLINE
            + urlPrefix + SUBSTITUTE + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + KEYSEP + VALUE + VALSEP + "<newValue>" + KEYSEP + OLDVALUE + VALSEP + "<oldValue>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Replaces oldvalue with newvalue in the key value pair for the given GUID. See below for more on the signature. "
            + NEWLINE + NEWLINE
            //
            + urlPrefix + SUBSTITUTELIST + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + KEYSEP + VALUE + VALSEP + "<JSON List>" + KEYSEP + OLDVALUE + VALSEP + "<JSON List>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "   Replaces oldvalue with newvalue in the key value pair for the given GUID. Value is a list of items formated as a JSON list. See below for more on the signature. "
            + NEWLINE + NEWLINE
            //
            + "DATABASE LIST OPERATIONS - "
            + "treat the value associated with the key as a list - duplicate are allowed" + NEWLINE + NEWLINE
            + urlPrefix + APPENDWITHDUPLICATION + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + KEYSEP + VALUE + VALSEP + "<value>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Appends the value onto the key value pair for the given GUID. See below for more on the signature. "
            + NEWLINE + NEWLINE
            //
            + urlPrefix + APPENDLISTWITHDUPLICATION + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + KEYSEP + VALUE + VALSEP + "<JSON List>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "   Appends the value onto of this key value pair for the given GUID. Value is a list of items formated as a JSON list. See below for more on the signature. "
            + NEWLINE + NEWLINE
            //
            + "DATABASE REMOVE OPERATIONS "
            + "- remove all instances of value" + NEWLINE + NEWLINE
            + urlPrefix + REMOVE + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + KEYSEP + VALUE + VALSEP + "<value>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Removes the value from the key value pair for the given GUID. See below for more on the signature. "
            //+ "Returns " + BADRESPONSE + " " + BADGUID + " if the GUID has not been registered." 
            + NEWLINE + NEWLINE
            //
            + urlPrefix + REMOVELIST + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + KEYSEP + VALUE + VALSEP + "<JSON List>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Removes all the values from the key value pair for the given GUID. Value is a list of items formated as a JSON list. See below for more on the signature. "
            //+ "Returns " + BADRESPONSE + " " + BADGUID + " if the GUID has not been registered." 
            + NEWLINE + NEWLINE
            + "DATABASE READ OPERATIONS" + NEWLINE + NEWLINE
            + urlPrefix + READ + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Returns one key value pair from the database for the given guid after authenticating that GUID making request has access authority."
            + " Values are always returned as a JSON list."
            + " Specify " + ALLFIELDS + " as the <field> to return all fields as a JSON object."
            + " See below for more on signature. "
            //+ "Returns " + BADRESPONSE + " " + BADGUID + " if the GUID has not been registered." 
            + NEWLINE + NEWLINE
            //
            + urlPrefix + READ + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + KEYSEP + READER + VALSEP + "<readerguid>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Returns one key value pair from the database for the given guid after authenticating that the readerguid (GUID making request) has access authority. "
            + "Values are always returned as a JSON list."
            //+ "Specify " + ALLFIELDS + " as the <field> to return all fields. "
            + " See below for more on signature. "
            //+ "Returns " + BADRESPONSE + " " + BADGUID + " if the GUID has not been registered." 
            + NEWLINE + NEWLINE
            //
            + urlPrefix + READONE + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Returns one key value pair from the database for the given guid after authenticating that GUID making request has access authority. "
            + "Treats the value of key value pair as a singleton item and returns that item."
            //+ "Specify " + ALLFIELDS + " as the <field> to return all fields. "
            + " See below for more on signature. "
            //+ "Returns " + BADRESPONSE + " " + BADGUID + " if the GUID has not been registered." 
            + NEWLINE + NEWLINE
            //
            + urlPrefix + READONE + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + KEYSEP + READER + VALSEP + "<readerguid>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Returns one key value pair from the database for the given guid after authenticating that the readerguid (GUID making request) has access authority. "
            + "Treats the value of key value pair as a singleton item and returns that item."
            //+ "Specify " + ALLFIELDS + " as the <field> to return all fields. "
            + " See below for more on signature. "
            //+ "Returns " + BADRESPONSE + " " + BADGUID + " if the GUID has not been registered." 
            + NEWLINE + NEWLINE
            + "DATABASE ACCESS CONTROL OPERATIONS "
            + "- regulate access to individual fields" + NEWLINE + NEWLINE
            + urlPrefix + ACLADD + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + KEYSEP + ACCESSER + VALSEP + "<accesserguid>" + KEYSEP + ACLTYPE + VALSEP + "<ACL type>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Updates the access control list of the given GUID's field to include the allowedreader. " + NEWLINE
            + "accessor guid can be guid or group guid or " + EVERYONE + " which means anyone." + NEWLINE
            + "field can be also be " + ALLFIELDS + " which means all fields can be read by the accessor" + NEWLINE
            + "See below for description of ACL type and signature. "
            + NEWLINE + NEWLINE
            //
            + urlPrefix + ACLREMOVE + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + KEYSEP + ACCESSER + VALSEP + "<accesserguid>" + KEYSEP + ACLTYPE + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Updates the access control list of the given GUID's field to remove the allowedreader. See below for description of ACL type and signature. "
            + NEWLINE + NEWLINE
            //
            + urlPrefix + ACLRETRIEVE + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + KEYSEP + ACLTYPE + VALSEP + "<ACL type>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Returns the access control list for a GUID's field. See below for description of ACL type and signature. "
            + NEWLINE + NEWLINE
            //
            + "DATABASE GROUP OPERATIONS "
            + "- all guids can act as a group GUID" + NEWLINE + NEWLINE
            + urlPrefix + ADDTOGROUP + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + MEMBER + VALSEP + "<member guid>" + KEYSEP + WRITER + VALSEP + "<writer guid>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Adds the member guid to the group specified by guid. Writer guid needs to have write access and sign the command." + NEWLINE + NEWLINE
            + urlPrefix + REMOVEFROMGROUP + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + MEMBER + VALSEP + "<member guid>" + KEYSEP + WRITER + VALSEP + "<writer guid>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Removes the member guid from the group specified by guid. Writer guid needs to have write access and sign the command." + NEWLINE + NEWLINE
            + urlPrefix + GETGROUPMEMBERS + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + READER + VALSEP + "<reader guid>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Returns the members of the group formatted as a JSON Array. Reader guid needs to have read access and sign the command." + NEWLINE + NEWLINE //                + NEWLINE + "DEPRECATED GROUP OPERATIONS - regulate access to individual fields" + NEWLINE
            + "GUID TAGGING "
            + NEWLINE + NEWLINE
            + urlPrefix + ADDTAG + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + NAME + VALSEP + "<tagname>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Adds a tag to the guid. Must be signed by the guid. Returns " + BADGUID + " if the GUID has not been registered." + NEWLINE + NEWLINE //
            + urlPrefix + REMOVETAG + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + NAME + VALSEP + "<tagname>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Removes a tag from the guid. Must be signed by the guid. Returns " + BADGUID + " if the GUID has not been registered." + NEWLINE + NEWLINE //
            + urlPrefix + CLEARTAGGED + QUERYPREFIX + NAME + VALSEP + "<tagname>" + NEWLINE
            + "  Removes all guids that contain the tag." + NEWLINE
            + urlPrefix + GETTAGGED + QUERYPREFIX + NAME + VALSEP + "<tagname>" + NEWLINE
            + "  Removes all guids that contain the tag."
            + NEWLINE + NEWLINE;

    String demo = "MISCELLANEOUS OPERATIONS " + NEWLINE + NEWLINE
            + urlPrefix + DEMO + QUERYPREFIX + PASSKEY + VALSEP + "<value>" + NEWLINE
            + "  Enters demo mode if supplied with the correct passkey. If passkey is 'off' turns demo mode off. " + NEWLINE
            + "In demo mode signatures and access control is ignored and additional commands are enabled. "
            + NEWLINE + NEWLINE
            + urlPrefix + DELETEALLRECORDS + NEWLINE
            + "  [ONLY IN DEMO MODE] Deletes all records." + NEWLINE + NEWLINE
            + urlPrefix + RESETDATABASE + NEWLINE
            + "  [ONLY IN DEMO MODE] Rests the database to an initialized state. The nuclear option." + NEWLINE + NEWLINE
            + urlPrefix + CLEARCACHE + NEWLINE
            + "  [ONLY IN DEMO MODE] Clears the local name server cache." + NEWLINE + NEWLINE
            + urlPrefix + DUMPCACHE + NEWLINE
            + "  [ONLY IN DEMO MODE] Returns the contents of the local name server cache." + NEWLINE + NEWLINE
            + urlPrefix + DUMP + NEWLINE
            + "  [ONLY IN DEMO MODE] Returns the contents of the database." + NEWLINE + NEWLINE //            + urlPrefix + DELETEALLGUIDRECORDS + QUERYPREFIX + GUID + VALSEP + "<guid>" + NEWLINE
            //            + "  [ONLY IN DEMO MODE] Removes all records for the given." + NEWLINE + NEWLINE
            ;

    String post = "Notes: " + NEWLINE + NEWLINE
            + "o) The signature is a digest of the entire command signed by the private key associated with the GUID." + NEWLINE + NEWLINE
            + "o) Group read and write access is controlled using the special " + GROUP_ACL + " field." + NEWLINE + NEWLINE
            + "o) ACL Type is one of " + AccessType.typesToString() + NEWLINE + NEWLINE
            + "o) Commands that don't return anything return the string " + OKRESPONSE + " if they are accepted." + NEWLINE + NEWLINE
            + "o) Commands that cannot be processed return the string that starts with the token " + BADRESPONSE + " " + NEWLINE
            + "followed by a token that indicates the problem, followed by an optional additional message." + NEWLINE
            + "The following line is an example bad response line (the tokens are separated by a space):" + NEWLINE
            + BADRESPONSE + " " + BADSIGNATURE + " " + "This string provides further information about the problem" + NEWLINE
            + "Problem indicator tokens (the second token above, also referenced in the query descriptions above) and their meanings are as follows:" + NEWLINE
            + "      " + BADSIGNATURE + " - an incorrect signature was supplied" + NEWLINE
            + "      " + ACCESSDENIED + " - access priveleges to the field do not exist for the given GUID" + NEWLINE
            + "      " + BADREADERGUID + " - the GUID does not exist" + NEWLINE
            + "      " + BADGUID + " - the GUID has not been registered" + NEWLINE
            + "      " + BADACCOUNT + " - the user has not been registered" + NEWLINE
            + "      " + BADGROUP + " - the group has not been registered" + NEWLINE
            + "      " + BADFIELD + " - the field does not exist" + NEWLINE
            + "      " + DUPLICATENAME + " - the human readable name already exists" + NEWLINE
            + "      " + DUPLICATEGUID + " - the GUID already exists" + NEWLINE
            + "      " + DUPLICATEGROUP + " - the group already exists" + NEWLINE
            + "      " + DUPLICATEFIELD + " - the field already exists" + NEWLINE
            + "      " + JSONPARSEERROR + " - there was a problem parsing a json list" + NEWLINE
            + "      " + NOACTIONFOUND + " - there was a problem parsing the action in the query" + NEWLINE
            + "      " + TOMANYALIASES + " - there is a limit to the number of human readable aliases for a GUID" + NEWLINE
            + "      " + TOMANYGUIDS + " - there is a limit to the number of GUIDs that can be created" + NEWLINE
            + "      " + OPERATIONNOTSUPPORTED + " - the indicated operation does not exist" + NEWLINE
            + "      " + QUERYPROCESSINGERROR + " - a general error occurred during the processing of a query" + NEWLINE
            + "      " + UPDATEERROR + " - a general error that occured while updating the ACCOUNT or GUID info" + NEWLINE
            + "      " + GENERICEERROR + " - covers those pesky cases where you just don't know" + NEWLINE
            + NEWLINE + "o) A JSON list with 3 values in it looks like this [value1, value2, value3]" + NEWLINE + NEWLINE;
    return main + (demoMode ? demo : "") + post;
  }

//  @Deprecated
//  private GroupInfo getGroupInfoV1(String guid) {
//    GroupEntry result = groupAccessV1.lookupFromGuid(guid);
//    if (result != null) {
//      return result.getGroupInfo();
//    } else {
//      return null;
//    }
//  }
  /**
   * RegisterAccount. Name is human readable name of primary guid of this account.
   *
   * @param name
   * @param guid
   * @param publicKey
   * @param password
   * @return GUID
   */
  public String processRegisterAccount(String name, String publicKey, String password) {
    String guid = createGuidFromPublicKey(publicKey);
    return processRegisterAccountWithGuid(name, guid, publicKey, password);
  }

  private String createGuidFromPublicKey(String publicKey) {
    byte[] publicKeyDigest = SHA1HashFunction.getInstance().hash(publicKey.getBytes());
    return ByteUtils.toHex(publicKeyDigest);
  }

  public String processRegisterAccountWithGuid(String name, String guid, String publicKey, String password) {
    String result = accountAccess.addAccount(name, guid, publicKey, password);
    if (OKRESPONSE.equals(result)) {
      return guid;
    } else {
      return result;
    }
  }

  public String processRemoveAccount(String name, String guid, String signature, String message) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo guidInfo = accountAccess.lookupGuidInfo(guid);
    if (verifySignature(guidInfo, signature, message)) {
      AccountInfo accountInfo = accountAccess.lookupAccountInfoFromName(name);
      if (accountInfo != null) {
        return accountAccess.removeAccount(accountInfo);
      } else {
        return BADRESPONSE + " " + BADACCOUNT;
      }
    } else {
      return BADRESPONSE + " " + BADSIGNATURE;
    }
  }

  public String processAddAlias(String guid, String alias, String signature, String message) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo guidInfo = accountAccess.lookupGuidInfo(guid);
    if (verifySignature(guidInfo, signature, message)) {
      AccountInfo accountInfo = accountAccess.lookupAccountInfoFromGuid(guid);
      if (accountInfo.getAliases().size() > Defs.MAXALIASES) {
        return BADRESPONSE + " " + TOMANYALIASES;
      } else {
        return accountAccess.addAlias(accountInfo, alias);
      }
    } else {
      return BADRESPONSE + " " + BADSIGNATURE;
    }
  }

  public String processRemoveAlias(String guid, String alias, String signature, String message) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo guidInfo = accountAccess.lookupGuidInfo(guid);
    if (verifySignature(guidInfo, signature, message)) {
      AccountInfo accountInfo = accountAccess.lookupAccountInfoFromGuid(guid);
      return accountAccess.removeAlias(accountInfo, alias);
    } else {
      return BADRESPONSE + " " + BADSIGNATURE;
    }
  }

  public String processRetrieveAliases(String guid, String signature, String message) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo guidInfo = accountAccess.lookupGuidInfo(guid);
    if (verifySignature(guidInfo, signature, message)) {
      AccountInfo accountInfo = accountAccess.lookupAccountInfoFromGuid(guid);
      ArrayList<String> aliases = accountInfo.getAliases();
      return new JSONArray(aliases).toString();
    } else {
      return BADRESPONSE + " " + BADSIGNATURE;
    }
  }

  public String processAddGuid(String guid, String name, String publicKey, String signature, String message) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    String newGuid = createGuidFromPublicKey(publicKey);
    GuidInfo guidInfo = accountAccess.lookupGuidInfo(guid);
    if (verifySignature(guidInfo, signature, message)) {
      AccountInfo accountInfo = accountAccess.lookupAccountInfoFromGuid(guid);
      if (accountInfo.getGuids().size() > Defs.MAXGUIDS) {
        return BADRESPONSE + " " + TOMANYGUIDS;
      } else {
        String result = accountAccess.addGuid(accountInfo, name, newGuid, publicKey);
        if (OKRESPONSE.equals(result)) {
          return newGuid;
        } else {
          return result;
        }
      }
    } else {
      return BADRESPONSE + " " + BADSIGNATURE;
    }
  }

  public String processSetPassword(String guid, String password, String signature, String message) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo userInfo = accountAccess.lookupGuidInfo(guid);
    if (verifySignature(userInfo, signature, message)) {
      AccountInfo accountInfo = accountAccess.lookupAccountInfoFromGuid(guid);
      return accountAccess.setPassword(accountInfo, password);
    } else {
      return BADRESPONSE + " " + BADSIGNATURE;
    }
  }

  public String processLookupGuid(String name) {
    // look for a primary guid
    String result = accountAccess.lookupGuid(name);
    if (result != null) {
      return result;
    } else {
      return BADRESPONSE + " " + BADACCOUNT;
    }
  }

  public String processLookupGuidInfo(String guid) {
    GuidInfo guidInfo = accountAccess.lookupGuidInfo(guid);
    if (guidInfo != null) {
      try {
        return guidInfo.toJSONObject().toString();
      } catch (JSONException e) {
        return BADRESPONSE + " " + JSONPARSEERROR;
      }
    } else {
      return BADRESPONSE + " " + BADGUID;
    }
  }

  public String processCreate(String guid, String field, String value, String signature, String message) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo userInfo;
    if ((userInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID;
    }
    if (verifySignature(userInfo, signature, message)) {
      if (recordAccess.create(userInfo.getGuid(), field, (value == null ? new ResultValue() : new ResultValue(Arrays.asList(value))))) {
        return OKRESPONSE;
      } else {
        return BADRESPONSE + " " + DUPLICATEFIELD;
      }

    } else {
      return BADRESPONSE + " " + BADSIGNATURE;
    }
  }

  public String processCreateList(String guid, String field, String value, String signature, String message) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo userInfo;
    if ((userInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID;
    }
    try {
      if (verifySignature(userInfo, signature, message)) {
        if (recordAccess.create(userInfo.getGuid(), field, JSONUtils.JSONArrayToResultValue(new JSONArray(value)))) {
          return OKRESPONSE;
        } else {
          return BADRESPONSE + " " + DUPLICATEFIELD;
        }
      } else {
        return BADRESPONSE + " " + BADSIGNATURE;
      }
    } catch (JSONException e) {
      return BADRESPONSE + " " + JSONPARSEERROR;
    }
  }

  public String processUpdateOperation(String guid, String field, String value, String oldValue, String signature, String message,
          UpdateOperation operation) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo userInfo;
    if ((userInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID;
    }
    if (verifySignature(userInfo, signature, message)) {
      if (recordAccess.update(userInfo.getGuid(), field,
              new ResultValue(Arrays.asList(value)),
              oldValue != null ? new ResultValue(Arrays.asList(oldValue)) : null,
              operation)) {
        return OKRESPONSE;
      } else {
        return BADRESPONSE + " " + BADFIELD;
      }
    } else {
      return BADRESPONSE + " " + BADSIGNATURE;
    }
  }

  public String processUpdateListOperation(String guid, String field, String value, String oldValue, String signature, String message,
          UpdateOperation operation) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo userInfo;
    if ((userInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID;
    }
    if (verifySignature(userInfo, signature, message)) {
      try {
        if (recordAccess.update(userInfo.getGuid(), field,
                JSONUtils.JSONArrayToResultValue(new JSONArray(value)),
                oldValue != null ? JSONUtils.JSONArrayToResultValue(new JSONArray(oldValue)) : null,
                operation)) {
          return OKRESPONSE;
        } else {
          return BADRESPONSE + " " + BADFIELD;
        }
      } catch (JSONException e) {
        return BADRESPONSE + " " + JSONPARSEERROR;
      }
    } else {
      return BADRESPONSE + " " + BADSIGNATURE;
    }
  }

  public String processRead(String guid, String field, String reader, String signature, String message) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo guidInfo, readerGuidInfo;
    if ((guidInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID;
    }
    if (reader.equals(guid)) {
      readerGuidInfo = guidInfo;
    } else if ((readerGuidInfo = accountAccess.lookupGuidInfo(reader)) == null) {
      return BADRESPONSE + " " + BADREADERGUID;
    }
    if (!verifySignature(readerGuidInfo, signature, message)) {
      return BADRESPONSE + " " + BADSIGNATURE;
    } else if (!verifyAccess(AccessType.READ_WHITELIST, guidInfo, field, readerGuidInfo)) {
      return BADRESPONSE + " " + ACCESSDENIED;
    } else if (ALLFIELDS.equals(field)) {
      return recordAccess.lookupMultipleValues(guid, ALLFIELDS);
    } else {
      return recordAccess.lookup(guidInfo.getGuid(), field);
    }
  }

  public String processReadOne(String guid, String field, String reader, String signature, String message) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo guidInfo, readerGuidInfo;
    if ((guidInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID;
    }
    if (reader.equals(guid)) {
      readerGuidInfo = guidInfo;
    } else if ((readerGuidInfo = accountAccess.lookupGuidInfo(reader)) == null) {
      return BADRESPONSE + " " + BADREADERGUID;
    }
    if (!verifySignature(readerGuidInfo, signature, message)) {
      return BADRESPONSE + " " + BADSIGNATURE;
    } else if (!verifyAccess(AccessType.READ_WHITELIST, guidInfo, field, readerGuidInfo)) {
      return BADRESPONSE + " " + ACCESSDENIED;
    } else if (ALLFIELDS.equals(field)) {
      return recordAccess.lookupOneMultipleValues(guid, ALLFIELDS);
    } else {
      return recordAccess.lookupOne(guidInfo.getGuid(), field);
    }
  }

  public String processAclAdd(String accessType, String guid, String field, String accesser, String signature, String message) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    AccessType access;
    if ((access = AccessType.valueOf(accessType)) == null) {
      return BADRESPONSE + " " + BADACLTYPE + "Should be one of " + AccessType.values().toString();
    }
    GuidInfo userInfo;
    if ((userInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID;
    }
    if (verifySignature(userInfo, signature, message)) {
      aclAccess.add(access, userInfo, field, accesser);
      return OKRESPONSE;
    } else {
      return BADRESPONSE + " " + BADSIGNATURE;
    }
  }

  public String processAclRemove(String accessType, String guid, String field, String accesser, String signature, String message) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    AccessType access;
    if ((access = AccessType.valueOf(accessType)) == null) {
      return BADRESPONSE + " " + BADACLTYPE + "Should be one of " + AccessType.values().toString();
    }
    GuidInfo userInfo;
    if ((userInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID;
    }
    if (verifySignature(userInfo, signature, message)) {
      aclAccess.remove(access, userInfo, field, accesser);
      return OKRESPONSE;
    } else {
      return BADRESPONSE + " " + BADSIGNATURE;
    }
  }

  public String processRetrieveAcl(String accessType, String guid, String field, String signature, String message) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    AccessType access;
    if ((access = AccessType.valueOf(accessType)) == null) {
      return BADRESPONSE + " " + BADACLTYPE + "Should be one of " + AccessType.values().toString();
    }
    GuidInfo userInfo;
    if ((userInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID;
    }
    if (verifySignature(userInfo, signature, message)) {
      Set values = aclAccess.lookup(access, userInfo, field);
      return new JSONArray(values).toString();
    } else {
      return BADRESPONSE + " " + BADSIGNATURE;
    }
  }

  public String processAddToGroup(String guid, String member, String writer, String signature, String message) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo userInfo, writerInfo;
    if ((userInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID;
    }
    if (writer.equals(guid)) {
      writerInfo = userInfo;
    } else if ((writerInfo = accountAccess.lookupGuidInfo(writer)) == null) {
      return BADRESPONSE + " " + BADWRITERGUID;
    }
    if (!verifySignature(writerInfo, signature, message)) {
      return BADRESPONSE + " " + BADSIGNATURE;
    } else if (!verifyAccess(AccessType.WRITE_WHITELIST, userInfo, GROUP_ACL, writerInfo)) {
      return BADRESPONSE + " " + ACCESSDENIED;
    } else if (groupAccess.addToGroup(guid, member)) {
      return OKRESPONSE;
    } else {
      return BADRESPONSE + " " + GENERICEERROR;
    }
  }

  public String processRemoveFromGroup(String guid, String member, String writer, String signature, String message) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo userInfo, writerInfo;
    if ((userInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID;
    }
    if (writer.equals(guid)) {
      writerInfo = userInfo;
    } else if ((writerInfo = accountAccess.lookupGuidInfo(writer)) == null) {
      return BADRESPONSE + " " + BADWRITERGUID;
    }
    if (!verifySignature(writerInfo, signature, message)) {
      return BADRESPONSE + " " + BADSIGNATURE;
    } else if (!verifyAccess(AccessType.WRITE_WHITELIST, userInfo, GROUP_ACL, writerInfo)) {
      return BADRESPONSE + " " + ACCESSDENIED;
    } else if (groupAccess.removeFromGroup(guid, member)) {
      return OKRESPONSE;
    } else {
      return BADRESPONSE + " " + GENERICEERROR;
    }
  }

  public String processGetGroupMembers(String guid, String reader, String signature, String message) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo userInfo, readInfo;
    if ((userInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID;
    }
    if (reader.equals(guid)) {
      readInfo = userInfo;
    } else if ((readInfo = accountAccess.lookupGuidInfo(reader)) == null) {
      return BADRESPONSE + " " + BADREADERGUID;
    }
    if (!verifySignature(readInfo, signature, message)) {
      return BADRESPONSE + " " + BADSIGNATURE;
    } else if (!verifyAccess(AccessType.READ_WHITELIST, userInfo, GROUP_ACL, readInfo)) {
      return BADRESPONSE + " " + ACCESSDENIED;
    } else {
      ResultValue values = groupAccess.lookup(guid);
      JSONArray list = new JSONArray(values);
      return list.toString();
    }
  }

  public String processDemo(String host, String passkey, String inputLine) {
    if (host.equals(passkey)) {
      demoMode = true;
      return OKRESPONSE;
    } else if ("off".equals(passkey)) {
      demoMode = false;
      return OKRESPONSE;
    }
    return BADRESPONSE + " " + OPERATIONNOTSUPPORTED + " Don't understand " + DEMO + QUERYPREFIX + inputLine;
  }

  public String processDump() {
    if (demoMode) {
      return Admintercessor.getInstance().sendDump();
    }
    return BADRESPONSE + " " + OPERATIONNOTSUPPORTED + " Don't understand " + DUMP;
  }

  public String processDump(String tagName) {
    if (demoMode) {
      return new JSONArray(Admintercessor.getInstance().collectTaggedGuids(tagName)).toString();
    }
    return BADRESPONSE + " " + OPERATIONNOTSUPPORTED + " Don't understand " + DUMP + QUERYPREFIX + NAME + VALSEP + tagName;
  }

  public String processDumpCache() {
    if (demoMode) {
      return Admintercessor.getInstance().sendDumpCache();
    }
    return BADRESPONSE + " " + OPERATIONNOTSUPPORTED + " Don't understand " + DUMPCACHE;
  }

  public String processDeleteAllRecords(String inputLine) {
    if (demoMode) {
      if (Admintercessor.getInstance().sendDeleteAllRecords()) {
        return OKRESPONSE;
      } else {
        return BADRESPONSE;
      }
    }
    return BADRESPONSE + " " + OPERATIONNOTSUPPORTED + " Don't understand " + DELETEALLRECORDS + QUERYPREFIX + inputLine;
  }

  public String processResetDatabase(String inputLine) {
    if (demoMode) {
      if (Admintercessor.getInstance().sendResetDB()) {
        return OKRESPONSE;
      } else {
        return BADRESPONSE;
      }
    }
    return BADRESPONSE + " " + OPERATIONNOTSUPPORTED + " Don't understand " + RESETDATABASE + QUERYPREFIX + inputLine;
  }

  public String processClearCache(String inputLine) {
    if (demoMode) {
      if (Admintercessor.getInstance().sendClearCache()) {
        return OKRESPONSE;
      } else {
        return BADRESPONSE;
      }
    }
    return BADRESPONSE + " " + OPERATIONNOTSUPPORTED + " Don't understand " + CLEARCACHE + QUERYPREFIX + inputLine;
  }

//  public String processDeleteAllGuidRecords(String name) {
//    if (demoMode) {
//      Intercessor.getInstance().sendDeleteAllGuidRecords(name);
//      return OKRESPONSE;
//    }
//    return BADRESPONSE + " " + OPERATIONNOTSUPPORTED + " Don't understand " + DELETEALLGUIDRECORDS + QUERYPREFIX + GUID + VALSEP + name;
//  }
  public String processAddTag(String guid, String tag, String signature, String message) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo guidInfo = accountAccess.lookupGuidInfo(guid);
    if (verifySignature(guidInfo, signature, message)) {
      return accountAccess.addTag(guidInfo, tag);
    } else {
      return BADRESPONSE + " " + BADSIGNATURE;
    }
  }

  public String processRemoveTag(String guid, String tag, String signature, String message) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo guidInfo = accountAccess.lookupGuidInfo(guid);
    if (verifySignature(guidInfo, signature, message)) {
      return accountAccess.removeTag(guidInfo, tag);
    } else {
      return BADRESPONSE + " " + BADSIGNATURE;
    }
  }

  public String processGetTagged(String tagName) {
    return new JSONArray(Admintercessor.getInstance().collectTaggedGuids(tagName)).toString();
  }
  // currently doesn't handle subGuids that are tagged

  public String processClearTagged(String tagName) {
    for (String guid : Admintercessor.getInstance().collectTaggedGuids(tagName)) {
      AccountInfo accountInfo = accountAccess.lookupAccountInfoFromGuid(guid);
      if (accountInfo != null) {
        accountAccess.removeAccount(accountInfo);
      }
    }
    return OKRESPONSE;
  }

  /**
   * process queries for the http service *
   */
  public String processQuery(String host, String action, String queryString) {
    String fullString = action + QUERYPREFIX + queryString; // for signature check
    Map<String, String> queryMap = Util.parseURIQueryString(queryString);
    //String action = queryMap.get(ACTION);
    try {
      // HELP
      if (HELP.equals(action)) {
        //return getHelpString(GnrsHttpServer.hostName + (GnrsHttpServer.address != 80 ? (":" + GnrsHttpServer.address) : ""));
        return getHelpString(host);
      } else if (REGISTERACCOUNT.equals(action) && queryMap.keySet().containsAll(Arrays.asList(NAME, GUID, PUBLICKEY, PASSWORD))) {
        // syntax: register userName guid public_key
        String userName = queryMap.get(NAME);
        String guid = queryMap.get(GUID);
        String publicKey = queryMap.get(PUBLICKEY);
        String password = queryMap.get(PASSWORD);
        return processRegisterAccountWithGuid(userName, guid, publicKey, password);
      } else if (REGISTERACCOUNT.equals(action) && queryMap.keySet().containsAll(Arrays.asList(NAME, PUBLICKEY, PASSWORD))) {
        // syntax: register userName guid public_key
        String userName = queryMap.get(NAME);
        String publicKey = queryMap.get(PUBLICKEY);
        String password = queryMap.get(PASSWORD);
        return processRegisterAccount(userName, publicKey, password);
      } else if (REGISTERACCOUNT.equals(action) && queryMap.keySet().containsAll(Arrays.asList(NAME, GUID, PUBLICKEY))) {
        // syntax: register userName guid public_key
        String userName = queryMap.get(NAME);
        String guid = queryMap.get(GUID);
        String publicKey = queryMap.get(PUBLICKEY);
        return processRegisterAccountWithGuid(userName, guid, publicKey, null);
      } else if (REGISTERACCOUNT.equals(action) && queryMap.keySet().containsAll(Arrays.asList(NAME, PUBLICKEY))) {
        // syntax: register userName guid public_key
        String userName = queryMap.get(NAME);
        String publicKey = queryMap.get(PUBLICKEY);
        return processRegisterAccount(userName, publicKey, null);
      } else if (REMOVEACCOUNT.equals(action) && queryMap.keySet().containsAll(Arrays.asList(NAME, GUID, SIGNATURE))) {
        // syntax: register userName guid public_key
        String userName = queryMap.get(NAME);
        String guid = queryMap.get(GUID);
        String signature = queryMap.get(SIGNATURE);
        return processRemoveAccount(userName, guid, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
      } else if (ADDGUID.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, NAME, PUBLICKEY, SIGNATURE))) {
        // syntax: register userName guid public_key
        String guid = queryMap.get(GUID);
        String userName = queryMap.get(NAME);
        String publicKey = queryMap.get(PUBLICKEY);
        String signature = queryMap.get(SIGNATURE);
        return processAddGuid(guid, userName, publicKey, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
      } else if (ADDALIAS.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, NAME, SIGNATURE))) {
        // syntax: register userName guid public_key
        String guid = queryMap.get(GUID);
        String name = queryMap.get(NAME);
        String signature = queryMap.get(SIGNATURE);
        return processAddAlias(guid, name, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
      } else if (REMOVEALIAS.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, NAME, SIGNATURE))) {
        // syntax: register userName guid public_key
        String guid = queryMap.get(GUID);
        String name = queryMap.get(NAME);
        String signature = queryMap.get(SIGNATURE);
        return processRemoveAlias(guid, name, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
      } else if (RETRIEVEALIASES.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String signature = queryMap.get(SIGNATURE);
        return processRetrieveAliases(guid, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
      } else if (SETPASSWORD.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, PASSWORD, SIGNATURE))) {
        // syntax: register userName guid public_key
        String guid = queryMap.get(GUID);
        String password = queryMap.get(PASSWORD);
        String signature = queryMap.get(SIGNATURE);
        return processSetPassword(guid, password, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
        // LOOKUP
      } else if (LOOKUPGUID.equals(action) && queryMap.keySet().containsAll(Arrays.asList(NAME))) {
        String userName = queryMap.get(NAME);
        return processLookupGuid(userName);
      } else if (LOOKUPGUIDRECORD.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID))) {
        String guid = queryMap.get(GUID);
        return processLookupGuidInfo(guid);
      } else if (READ.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, READER, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String reader = queryMap.get(READER);
        String signature = queryMap.get(SIGNATURE);
        return processRead(guid, field, reader, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
      } else if (READ.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String reader = guid;
        String signature = queryMap.get(SIGNATURE);
        return processRead(guid, field, reader, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
      } else if (READONE.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, READER, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String reader = queryMap.get(READER);
        String signature = queryMap.get(SIGNATURE);
        return processReadOne(guid, field, reader, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
      } else if (READONE.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String reader = guid;
        String signature = queryMap.get(SIGNATURE);
        return processReadOne(guid, field, reader, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
        // CREATE AN EMPTY FIELD
      } else if (CREATE.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String signature = queryMap.get(SIGNATURE);
        return processCreate(guid, field, value, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
      } else if (CREATE.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String signature = queryMap.get(SIGNATURE);
        return processCreate(guid, field, null, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
      } else if (CREATELIST.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String signature = queryMap.get(SIGNATURE);
        return processCreateList(guid, field, value, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
      } else if (REPLACE.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateOperation(guid, field, value, null, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.REPLACE_ALL);
      } else if (APPENDWITHDUPLICATION.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateOperation(guid, field, value, null, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.APPEND_WITH_DUPLICATION);
      } else if (APPEND.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateOperation(guid, field, value, null, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.APPEND);
      } else if (APPENDORCREATE.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateOperation(guid, field, value, null, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.APPEND_OR_CREATE);
      } else if (REPLACEORCREATE.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateOperation(guid, field, value, null, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.REPLACE_ALL_OR_CREATE);
      } else if (REMOVE.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateOperation(guid, field, value, null, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.REMOVE);
      } else if (REPLACELIST.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateListOperation(guid, field, value, null, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.REPLACE_ALL);
      } else if (APPENDLISTWITHDUPLICATION.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateListOperation(guid, field, value, null, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.APPEND_WITH_DUPLICATION);
      } else if (APPENDLIST.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateListOperation(guid, field, value, null, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.APPEND);
      } else if (APPENDORCREATELIST.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateListOperation(guid, field, value, null, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.APPEND_OR_CREATE);
      } else if (REPLACEORCREATELIST.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateListOperation(guid, field, value, null, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.REPLACE_ALL_OR_CREATE);
      } else if (REMOVELIST.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateListOperation(guid, field, value, null, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.REMOVE);
      } else if (SUBSTITUTE.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, OLDVALUE, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String oldValue = queryMap.get(OLDVALUE);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateOperation(guid, field, value, oldValue, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.SUBSTITUTE);
      } else if (SUBSTITUTELIST.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, OLDVALUE, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String oldValue = queryMap.get(OLDVALUE);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateListOperation(guid, field, value, oldValue, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.SUBSTITUTE);
      } else if (CLEAR.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateOperation(guid, field, "", null, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.CLEAR);
        // ACLADD
      } else if (ACLADD.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, ACCESSER, ACLTYPE, SIGNATURE))) {
        // syntax: aclAdd hash field allowedreader signature
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String accesser = queryMap.get(ACCESSER);
        String aclType = queryMap.get(ACLTYPE);
        String signature = queryMap.get(SIGNATURE);
        return processAclAdd(aclType, guid, field, accesser, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
        // ACLREMOVE
      } else if (ACLREMOVE.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, ACCESSER, ACLTYPE, SIGNATURE))) {
        // syntax: aclRemove guid field allowedreader signature
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String accesser = queryMap.get(ACCESSER);
        String aclType = queryMap.get(ACLTYPE);
        String signature = queryMap.get(SIGNATURE);
        return processAclRemove(aclType, guid, field, accesser, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
        // ACL
      } else if (ACLRETRIEVE.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, ACLTYPE, SIGNATURE))) {
        // show the acl list for user's field
        // syntax: acl guid field signature
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String aclType = queryMap.get(ACLTYPE);
        String signature = queryMap.get(SIGNATURE);
        return processRetrieveAcl(aclType, guid, field, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
      } else if (ADDTOGROUP.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, MEMBER, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String member = queryMap.get(MEMBER);
        String signature = queryMap.get(SIGNATURE);
        return processAddToGroup(guid, member, guid, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
      } else if (ADDTOGROUP.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, MEMBER, WRITER, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String member = queryMap.get(MEMBER);
        String writer = queryMap.get(WRITER);
        String signature = queryMap.get(SIGNATURE);
        return processAddToGroup(guid, member, writer, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
      } else if (REMOVEFROMGROUP.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, MEMBER, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String member = queryMap.get(MEMBER);
        String signature = queryMap.get(SIGNATURE);
        return processRemoveFromGroup(guid, member, guid, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
      } else if (REMOVEFROMGROUP.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, MEMBER, WRITER, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String member = queryMap.get(MEMBER);
        String writer = queryMap.get(WRITER);
        String signature = queryMap.get(SIGNATURE);
        return processRemoveFromGroup(guid, member, writer, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
      } else if (GETGROUPMEMBERS.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, READER, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String reader = queryMap.get(READER);
        String signature = queryMap.get(SIGNATURE);
        return processGetGroupMembers(guid, reader, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
      } else if (GETGROUPMEMBERS.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String signature = queryMap.get(SIGNATURE);
        return processGetGroupMembers(guid, guid, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
        // DEMO
      } else if (DEMO.equals(action) && queryMap.keySet().containsAll(Arrays.asList(PASSKEY))) {
        // pass in the host to use as a passkey check
        return processDemo(host, queryMap.get(PASSKEY), queryString);
        // CLEAR
      } else if (DELETEALLRECORDS.equals(action)) {
        return processDeleteAllRecords(queryString);
      } else if (RESETDATABASE.equals(action)) {
        return processResetDatabase(queryString);
      } else if (CLEARCACHE.equals(action)) {
        return processClearCache(queryString);
      } else if (DUMPCACHE.equals(action)) {
        return processDumpCache();
      } else if (ADDTAG.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, NAME, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String tagName = queryMap.get(NAME);
        String signature = queryMap.get(SIGNATURE);
        return processAddTag(guid, tagName, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
      } else if (REMOVETAG.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, NAME, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String tagName = queryMap.get(NAME);
        String signature = queryMap.get(SIGNATURE);
        return processRemoveTag(guid, tagName, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
      } else if (CLEARTAGGED.equals(action) && queryMap.keySet().containsAll(Arrays.asList(NAME))) {
        String tagName = queryMap.get(NAME);
        return processClearTagged(tagName);
      } else if (GETTAGGED.equals(action) && queryMap.keySet().containsAll(Arrays.asList(NAME))) {
        String tagName = queryMap.get(NAME);
        return processGetTagged(tagName);
      } else if (DUMP.equals(action) && queryMap.keySet().containsAll(Arrays.asList(NAME))) {
        String tagName = queryMap.get(NAME);
        return processDump(tagName);
      } else if (DUMP.equals(action)) {
        return processDump();
      } else {
        return BADRESPONSE + " " + OPERATIONNOTSUPPORTED + " - Don't understand " + action + QUERYPREFIX + queryString;
      }
    } catch (NoSuchAlgorithmException e) {
      return BADRESPONSE + " " + QUERYPROCESSINGERROR + " " + e;
    } catch (InvalidKeySpecException e) {
      return BADRESPONSE + " " + QUERYPROCESSINGERROR + " " + e;
    } catch (SignatureException e) {
      return BADRESPONSE + " " + QUERYPROCESSINGERROR + " " + e;
    } catch (InvalidKeyException e) {
      return BADRESPONSE + " " + QUERYPROCESSINGERROR + " " + e;
    }
  }

  private boolean verifySignature(GuidInfo userInfo, String signature, String message) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    if (demoMode) {
      return true;
    }
    byte[] messageDigest = SHA1HashFunction.getInstance().hash(message.getBytes());


    byte[] encodedPublicKey = Base64.decode(userInfo.getPublicKey());
    //byte[] encodedPublicKey = MoreUtils.hexStringToByteArray(userInfo.getPublicKey());
    KeyFactory keyFactory = KeyFactory.getInstance(RASALGORITHM);
    X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(encodedPublicKey);
    PublicKey publicKey = keyFactory.generatePublic(publicKeySpec);

    Signature sig = Signature.getInstance(Protocol.SIGNATUREALGORITHM);
    sig.initVerify(publicKey);
    sig.update(messageDigest);
    boolean result = sig.verify(ByteUtils.hexStringToByteArray(signature));
    GNS.getLogger().fine("User " + userInfo.getName() + (result ? " verified " : " NOT verified ") + "as author of message " + message);
    return result;
  }

  /**
   * Checks to see if the reader given in readerInfo can access all of the fields of the user given by userInfo.
   *
   * @param access
   * @param contectInfo
   * @param readerInfo
   * @return
   */
  private boolean verifyAccess(AccessType access, GuidInfo contectInfo, GuidInfo readerInfo) {
    return verifyAccess(access, contectInfo, ALLFIELDS, readerInfo);
  }

  /**
   * Checks to see if the reader given in readerInfo can access the field of the user given by userInfo. Access type is some combo
   * of read, write, blacklist and whitelist. Note: Blacklists are currently not activated.
   *
   * @param access
   * @param userInfo
   * @param field
   * @param readerInfo
   * @return
   */
  private boolean verifyAccess(AccessType access, GuidInfo userInfo, String field, GuidInfo readerInfo) {
    GNS.getLogger().finer("User: " + userInfo.getName() + " Reader: " + readerInfo.getName() + " Field: " + field);
    if (userInfo.getGuid().equals(readerInfo.getGuid())) {
      return true; // can always read your own stuff
    } else {
      Set<String> allowedusers = aclAccess.lookup(access, userInfo, field);
      GNS.getLogger().fine(userInfo.getName() + " allowed users of " + field + " : " + allowedusers);
      if (checkAllowedUsers(readerInfo.getGuid(), allowedusers)) {
        GNS.getLogger().fine("User " + readerInfo.getName() + " allowed to access user " + userInfo.getName() + "'s " + field + " field");
        return true;
      }
      // otherwise find any users that can access all of the fields
      allowedusers = aclAccess.lookup(access, userInfo, ALLFIELDS);
      if (checkAllowedUsers(readerInfo.getGuid(), allowedusers)) {
        GNS.getLogger().fine("User " + readerInfo.getName() + " allowed to access all of user " + userInfo.getName() + "'s fields");
        return true;
      }
    }
    GNS.getLogger().fine("User " + readerInfo.getName() + " NOT allowed to access user " + userInfo.getName() + "'s " + field + " field");
    return false;
  }

  private boolean checkAllowedUsers(String accesserGuid, Set<String> allowedusers) {
    if (allowedusers.contains(accesserGuid)) {
      return true;
    } else if (allowedusers.contains(EVERYONE)) {
      return true;
    } else {
      // map over the allowedusers and see if any of them are groups that the user belongs to
      for (String potentialGroupGuid : allowedusers) {
//        // old groups
//        GroupEntry entry = groupAccessV1.lookupFromGuid(potentialGroupGuid);
//        if (entry != null && entry.getMembers().contains(accesserGuid)) {
//          return true;
//        }
        // new groups
        if (groupAccess.lookup(potentialGroupGuid).contains(accesserGuid)) {
          return true;
        }
      }
      return false;
    }
  }

  private String removeSignature(String fullString, String fullSignatureField) {
    GNS.getLogger().finer("fullstring = " + fullString + " fullSignatureField = " + fullSignatureField);
    String result = fullString.substring(0, fullString.lastIndexOf(fullSignatureField));
    GNS.getLogger().finer("result = " + result);
    return result;
  }
  public static String Version = "$Revision$";
}
