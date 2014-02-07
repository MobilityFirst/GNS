package edu.umass.cs.gns.clientprotocol;

import edu.umass.cs.gns.client.AccountAccess;
import edu.umass.cs.gns.client.AccountInfo;
import edu.umass.cs.gns.client.Admintercessor;
import edu.umass.cs.gns.client.FieldAccess;
import edu.umass.cs.gns.client.FieldMetaData;
import edu.umass.cs.gns.client.FieldMetaData.MetaDataTypeName;
import edu.umass.cs.gns.client.GroupAccess;
import edu.umass.cs.gns.client.GuidInfo;
import edu.umass.cs.gns.client.UpdateOperation;
import edu.umass.cs.gns.httpserver.Defs;
import edu.umass.cs.gns.httpserver.GnsHttpServer;
import edu.umass.cs.gns.httpserver.SHA1HashFunction;
import edu.umass.cs.gns.httpserver.SystemParameter;
import static edu.umass.cs.gns.httpserver.Defs.*;
import static edu.umass.cs.gns.clientprotocol.Defs.*;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.ResultValue;
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
 * Implements the GNS server protocol. Used by the 
 *
 * @author westy
 */
public class Protocol {
  //
  private boolean adminMode = false;
  private FieldAccess fieldAccess = FieldAccess.getInstance();
  private AccountAccess accountAccess = AccountAccess.getInstance();
  private FieldMetaData fieldMetaData = FieldMetaData.getInstance();
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
            + urlPrefix + REMOVEGUID + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + GUID2 + VALSEP + "<guidtoremove>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Removes the second GUID from the account associated with the first GUID. Must be signed by the guid. Returns " + BADGUID + " if the GUID has not been registered." + NEWLINE + NEWLINE
            //
            + urlPrefix + ADDALIAS + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + NAME + VALSEP + "<alias>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Adds a additional human readble name to the account associated with the GUID. Must be signed by the guid. Returns " + BADGUID + " if the GUID has not been registered." + NEWLINE + NEWLINE
            //
            + urlPrefix + REMOVEALIAS + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + NAME + VALSEP + "<alias>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Removes the alias from the account associated with the GUID. Must be signed by the guid. Returns " + BADGUID + " if the GUID has not been registered." + NEWLINE + NEWLINE
            //
            + "GNS CREATE OPERATIONS" + NEWLINE + NEWLINE
            + urlPrefix + CREATE + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + KEYSEP + VALUE + VALSEP + "<value>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Adds a key value pair to the GNS for the given GUID. See below for more on the signature. "
            + NEWLINE + NEWLINE
            //
            + urlPrefix + CREATELIST + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + KEYSEP + VALUE + VALSEP + "<JSON List>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Adds a key value pair to the GNS for the given GUID. Value is a list of items formated as a JSON list. See below for more on the signature. "
            + NEWLINE + NEWLINE
            //
            + urlPrefix + CREATE + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Adds a key value pair to the GNS for the given GUID. See below for more on the signature. "
            + NEWLINE + NEWLINE
            // 
            + "GNS UPSERT OPERATIONS - "
            + "are also SET OPERATIONS" + NEWLINE + NEWLINE
            //
            + urlPrefix + APPENDORCREATE + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + KEYSEP + VALUE + VALSEP + "<value>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Adds a key value pair to the GNS for the given GUID if it doesn not exist otherwise append value onto existing value. See below for more on the signature. "
            + NEWLINE + NEWLINE
            //
            + urlPrefix + APPENDORCREATELIST + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + KEYSEP + VALUE + VALSEP + "<JSON List>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Adds a key value pair to the GNS for the given GUID if it doesn not exist otherwise appends values onto existing value. Value is a list of items formated as a JSON list."
            + NEWLINE + NEWLINE
            //
            + urlPrefix + REPLACEORCREATE + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + KEYSEP + VALUE + VALSEP + "<value>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Adds a key value pair to the GNS for the given GUID if it doesn not exist otherwise replaces the value of this key value pair for the given GUID. See below for more on the signature. "
            + NEWLINE + NEWLINE
            //
            + urlPrefix + REPLACEORCREATELIST + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + KEYSEP + VALUE + VALSEP + "<JSON List>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + " Adds a key value pair to the GNS for the given GUID if it doesn not exist otherwise replaces the value of this key value pair for the given GUID with the value.  Value is a list of items formated as a JSON list."
            //
            + NEWLINE + NEWLINE
            //
            + "GNS SET OPERATIONS - "
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
            + "GNS LIST OPERATIONS - "
            + "treat the value associated with the key as a list - duplicate are allowed" + NEWLINE + NEWLINE
            + urlPrefix + APPENDWITHDUPLICATION + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + KEYSEP + VALUE + VALSEP + "<value>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Appends the value onto the key value pair for the given GUID. See below for more on the signature. "
            + NEWLINE + NEWLINE
            //
            + urlPrefix + APPENDLISTWITHDUPLICATION + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + KEYSEP + VALUE + VALSEP + "<JSON List>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "   Appends the value onto of this key value pair for the given GUID. Value is a list of items formated as a JSON list. See below for more on the signature. "
            + NEWLINE + NEWLINE
            //
            + "GNS REMOVE OPERATIONS "
            + "- remove all instances of value" + NEWLINE + NEWLINE
            + urlPrefix + REMOVE + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + KEYSEP + VALUE + VALSEP + "<value>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Removes the value from the key value pair for the given GUID. See below for more on the signature. "
            + NEWLINE + NEWLINE
            //
            + urlPrefix + REMOVELIST + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + KEYSEP + VALUE + VALSEP + "<JSON List>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Removes all the values from the key value pair for the given GUID. Value is a list of items formated as a JSON list. See below for more on the signature. "
            + NEWLINE + NEWLINE
            //
            + urlPrefix + REMOVEFIELD + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Removes the key value pair from the GNS. See below for more on the signature. "
            + NEWLINE + NEWLINE
            //
            + "GNS READ OPERATIONS" + NEWLINE + NEWLINE
            + urlPrefix + READ + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Returns one key value pair from the GNS for the given guid after authenticating that GUID making request has access authority."
            + " Values are always returned as a JSON list."
            + " Specify " + ALLFIELDS + " as the <field> to return all fields as a JSON object."
            + " See below for more on signature. "
            + NEWLINE + NEWLINE
            //
            + urlPrefix + READ + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + KEYSEP + READER + VALSEP + "<readerguid>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Returns one key value pair from the GNS for the given guid after authenticating that the readerguid (GUID making request) has access authority. "
            + "Values are always returned as a JSON list."
            + " Specify " + ALLFIELDS + " as the <field> to return all fields. "
            + " See below for more on signature. "
            + NEWLINE + NEWLINE
            //
            + urlPrefix + READ + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + NEWLINE
            + "  Returns one key value pair from the GNS. Does not require authentication but field must be set to be readable by everyone."
            + " Values are always returned as a JSON list."
            + " Specify " + ALLFIELDS + " as the <field> to return all fields. "
            + " See below for more on signature. "
            + NEWLINE + NEWLINE
            //
            + urlPrefix + READONE + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Returns one key value pair from the GNS for the given guid after authenticating that GUID making request has access authority. "
            + "Treats the value of key value pair as a singleton item and returns that item."
            + " Specify " + ALLFIELDS + " as the <field> to return all fields. "
            + " See below for more on signature. "
            + NEWLINE + NEWLINE
            //
            + urlPrefix + READONE + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + KEYSEP + READER + VALSEP + "<readerguid>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Returns one key value pair from the GNS for the given guid after authenticating that the readerguid (GUID making request) has access authority. "
            + "Treats the value of key value pair as a singleton item and returns that item."
            + " Specify " + ALLFIELDS + " as the <field> to return all fields. "
            + " See below for more on signature. "
            + NEWLINE + NEWLINE
            //
            + urlPrefix + READONE + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + FIELD + VALSEP + "<field>" + NEWLINE
            + "  Returns one key value pair from the GNS for the given guid. Does not require authentication but field must be set to be readable by everyone."
            + " Treats the value of key value pair as a singleton item and returns that item."
            + " Specify " + ALLFIELDS + " as the <field> to return all fields. "
            + " See below for more on signature. "
            + NEWLINE + NEWLINE
            //
             + "SELECT OPERATIONS "
            + urlPrefix + SELECT + QUERYPREFIX + QUERY + VALSEP + "<query>" + NEWLINE
            + "  Returns all records that satisfy the query. For details see http://mobilityfirst.cs.umass.edu/wiki/index.php/Query_Syntax"
            + " Values are returned as a JSON array of JSON Objects."
            + NEWLINE + NEWLINE
            + urlPrefix + SELECT + QUERYPREFIX + FIELD + VALSEP + "<field>" + KEYSEP + VALUE + VALSEP + "<value>" + NEWLINE
            + "  Returns all records that have a field with the given value."
            + " Values are returned as a JSON array of JSON Objects."
            + NEWLINE + NEWLINE
            + "GNS ACCESS CONTROL OPERATIONS "
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
            + "GNS GROUP OPERATIONS "
            + "- all guids can act as a group GUID" + NEWLINE + NEWLINE
            + urlPrefix + ADDTOGROUP + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + MEMBER + VALSEP + "<member guid>" + KEYSEP + WRITER + VALSEP + "<writer guid>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Adds the member guid to the group specified by guid. Writer guid needs to have write access and sign the command." + NEWLINE + NEWLINE
            + urlPrefix + REMOVEFROMGROUP + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + MEMBER + VALSEP + "<member guid>" + KEYSEP + WRITER + VALSEP + "<writer guid>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Removes the member guid from the group specified by guid. Writer guid needs to have write access and sign the command." + NEWLINE + NEWLINE
            + urlPrefix + GETGROUPMEMBERS + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + READER + VALSEP + "<reader guid>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Returns the members of the group formatted as a JSON Array. Reader guid needs to have read access and sign the command." + NEWLINE + NEWLINE
            + urlPrefix + REQUESTJOINGROUP + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + MEMBER + VALSEP + "<member guid>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Request membership in the group specified by guid. Member guid needs to sign the command." + NEWLINE + NEWLINE
            + urlPrefix + RETRIEVEGROUPJOINREQUESTS + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Returns member join requests formatted as a JSON Array. Guid needs to sign the command." + NEWLINE + NEWLINE
            + urlPrefix + GRANTMEMBERSHIP + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + MEMBER + VALSEP + "<member guid>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Approves membership of member in the group. Guid needs to sign the command." + NEWLINE + NEWLINE
            + urlPrefix + GRANTMEMBERSHIP + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + MEMBERS + VALSEP + "<list of members>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Approves membership of members in the group. Members should be a list of guids formated as a JSON list. Guid needs to sign the command." + NEWLINE + NEWLINE
            + urlPrefix + REQUESTLEAVEGROUP + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + MEMBER + VALSEP + "<member guid>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Request revocation of membership in the group specified by guid. Member guid needs to sign the command." + NEWLINE + NEWLINE
            + urlPrefix + RETRIEVEGROUPLEAVEREQUESTS + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Returns member leave requests formatted as a JSON Array. Guid needs to sign the command." + NEWLINE + NEWLINE
            + urlPrefix + REVOKEMEMBERSHIP + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + MEMBER + VALSEP + "<member guid>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Revokes membership of member in the group. Guid needs to sign the command." + NEWLINE + NEWLINE
            + urlPrefix + REVOKEMEMBERSHIP + QUERYPREFIX + GUID + VALSEP + "<guid>" + KEYSEP + MEMBERS + VALSEP + "<list of members>" + KEYSEP + SIGNATURE + VALSEP + "<signature>" + NEWLINE
            + "  Approves revoking of membership of members in the group. Members should be a list of guids formated as a JSON list. Guid needs to sign the command." + NEWLINE + NEWLINE
            //
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

    String admin = "MISCELLANEOUS OPERATIONS " + NEWLINE + NEWLINE
            + urlPrefix + ADMIN + QUERYPREFIX + PASSKEY + VALSEP + "<value>" + NEWLINE
            + "  Enters admin mode if supplied with the correct passkey. If passkey is 'off' turns admin mode off. " + NEWLINE
            + "In admin additional commands are enabled. "
            + NEWLINE + NEWLINE
            + urlPrefix + DELETEALLRECORDS + NEWLINE
            + "  [ONLY IN ADMIN MODE] Deletes all records." + NEWLINE + NEWLINE
            + urlPrefix + RESETDATABASE + NEWLINE
            + "  [ONLY IN ADMIN MODE] Rests the GNS to an initialized state. The nuclear option." + NEWLINE + NEWLINE
            + urlPrefix + CLEARCACHE + NEWLINE
            + "  [ONLY IN ADMIN MODE] Clears the local name server cache." + NEWLINE + NEWLINE
            + urlPrefix + DUMPCACHE + NEWLINE
            + "  [ONLY IN ADMIN MODE] Returns the contents of the local name server cache." + NEWLINE + NEWLINE
            + urlPrefix + DUMP + NEWLINE
            + "  [ONLY IN ADMIN MODE] Returns the contents of the GNS." + NEWLINE + NEWLINE
            + urlPrefix + SETPARAMETER + QUERYPREFIX + NAME + VALSEP + "<parameter>" + KEYSEP + VALUE + VALSEP + "<value>" + NEWLINE
            + "  [ONLY IN ADMIN MODE] Changes a parameter value." + NEWLINE + NEWLINE;

    String post = "Notes: " + NEWLINE + NEWLINE
            + "o) The signature is a digest of the entire command signed by the private key associated with the GUID." + NEWLINE + NEWLINE
            + "o) Group read and write access is controlled using the special " + GROUP_ACL + " field." + NEWLINE + NEWLINE
            + "o) ACL Type is one of " + MetaDataTypeName.typesToString() + NEWLINE + NEWLINE
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
    return main + (adminMode ? admin : "") + post;
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
  private String processRegisterAccount(String host, String name, String publicKey, String password) {
    String guid = createGuidFromPublicKey(publicKey);
    return processRegisterAccountWithGuid(host, name, guid, publicKey, password);
  }

  private String createGuidFromPublicKey(String publicKey) {
    byte[] publicKeyDigest = SHA1HashFunction.getInstance().hash(publicKey.getBytes());
    return ByteUtils.toHex(publicKeyDigest);
  }

  private String processRegisterAccountWithGuid(String host, String name, String guid, String publicKey, String password) {
    String result = accountAccess.addAccountWithVerification(host, name, guid, publicKey, password);
    if (OKRESPONSE.equals(result)) {
      // set up the default read access
      fieldMetaData.add(MetaDataTypeName.READ_WHITELIST, guid, ALLFIELDS, EVERYONE);
      return guid;
    } else {
      return result;
    }
  }

  private String processVerifyAccount(String guid, String code) {
    return accountAccess.verifyAccount(guid, code);
  }

 private String processRemoveAccount(String name, String guid, String signature, String message) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo guidInfo;
    if ((guidInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
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

  private String processAddAlias(String guid, String alias, String signature, String message) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo guidInfo;
    if ((guidInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (verifySignature(guidInfo, signature, message)) {
      AccountInfo accountInfo = accountAccess.lookupAccountInfoFromGuid(guid);
      if (!accountInfo.isVerified()) {
        return BADRESPONSE + " " + VERIFICATIONERROR + "Account not verified";
      } else if (accountInfo.getAliases().size() > Defs.MAXALIASES) {
        return BADRESPONSE + " " + TOMANYALIASES;
      } else {
        return accountAccess.addAlias(accountInfo, alias);
      }
    } else {
      return BADRESPONSE + " " + BADSIGNATURE;
    }
  }

  private String processRemoveAlias(String guid, String alias, String signature, String message) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo guidInfo;
    if ((guidInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (verifySignature(guidInfo, signature, message)) {
      AccountInfo accountInfo = accountAccess.lookupAccountInfoFromGuid(guid);
      return accountAccess.removeAlias(accountInfo, alias);
    } else {
      return BADRESPONSE + " " + BADSIGNATURE;
    }
  }

  private String processRetrieveAliases(String guid, String signature, String message) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo guidInfo;
    if ((guidInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (verifySignature(guidInfo, signature, message)) {
      AccountInfo accountInfo = accountAccess.lookupAccountInfoFromGuid(guid);
      ArrayList<String> aliases = accountInfo.getAliases();
      return new JSONArray(aliases).toString();
    } else {
      return BADRESPONSE + " " + BADSIGNATURE;
    }
  }

  private String processAddGuid(String accountGuid, String name, String publicKey, String signature, String message) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    String newGuid = createGuidFromPublicKey(publicKey);
    GuidInfo accountGuidInfo;
    if ((accountGuidInfo = accountAccess.lookupGuidInfo(accountGuid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + accountGuid;
    }
    if (verifySignature(accountGuidInfo, signature, message)) {
      AccountInfo accountInfo = accountAccess.lookupAccountInfoFromGuid(accountGuid);
      if (!accountInfo.isVerified()) {
        return BADRESPONSE + " " + VERIFICATIONERROR + "Account not verified";
      } else if (accountInfo.getGuids().size() > Defs.MAXGUIDS) {
        return BADRESPONSE + " " + TOMANYGUIDS;
      } else {
        String result = accountAccess.addGuid(accountInfo, name, newGuid, publicKey);
        if (OKRESPONSE.equals(result)) {
          // set up the default read access
          fieldMetaData.add(MetaDataTypeName.READ_WHITELIST, newGuid, ALLFIELDS, EVERYONE);
          // give account guid read and write access to all fields in the new guid
          fieldMetaData.add(MetaDataTypeName.READ_WHITELIST, newGuid, ALLFIELDS, accountGuid);
          fieldMetaData.add(MetaDataTypeName.WRITE_WHITELIST, newGuid, ALLFIELDS, accountGuid);
          return newGuid;
        } else {
          return result;
        }
      }
    } else {
      return BADRESPONSE + " " + BADSIGNATURE;
    }
  }

  private String processRemoveGuid(String guid, String guid2, String signature, String message) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo guidInfo, guid2Info;
    if ((guid2Info = accountAccess.lookupGuidInfo(guid2)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid2;
    }
    if ((guidInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (verifySignature(guidInfo, signature, message)) {
      AccountInfo accountInfo = accountAccess.lookupAccountInfoFromGuid(guid);
      return accountAccess.removeGuid(accountInfo, guid2Info);
    } else {
      return BADRESPONSE + " " + BADSIGNATURE;
    }
  }

  private String processSetPassword(String guid, String password, String signature, String message) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo guidInfo;
    if ((guidInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (verifySignature(guidInfo, signature, message)) {
      AccountInfo accountInfo = accountAccess.lookupAccountInfoFromGuid(guid);
      return accountAccess.setPassword(accountInfo, password);
    } else {
      return BADRESPONSE + " " + BADSIGNATURE;
    }
  }

  private String processLookupGuid(String name) {
    // look for a primary guid
    String result = accountAccess.lookupGuid(name);
    if (result != null) {
      return result;
    } else {
      return BADRESPONSE + " " + BADACCOUNT;
    }
  }

  private String processLookupGuidInfo(String guid) {
    GuidInfo guidInfo;
    if ((guidInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (guidInfo != null) {
      try {
        return guidInfo.toJSONObject().toString();
      } catch (JSONException e) {
        return BADRESPONSE + " " + JSONPARSEERROR;
      }
    } else {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
  }

  private String processLookupAccountInfo(String guid) {
    AccountInfo acccountInfo;
    if ((acccountInfo = accountAccess.lookupAccountInfoFromGuid(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (acccountInfo != null) {
      try {
        return acccountInfo.toJSONObject().toString();
      } catch (JSONException e) {
        return BADRESPONSE + " " + JSONPARSEERROR;
      }
    } else {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
  }

  private String processCreate(String guid, String field, String value, String writer, String signature, String message)
          throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo guidInfo, writerGuidInfo;
    if ((guidInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (writer.equals(guid)) {
      writerGuidInfo = guidInfo;
    } else if ((writerGuidInfo = accountAccess.lookupGuidInfo(writer)) == null) {
      return BADRESPONSE + " " + BADWRITERGUID + " " + writer;
    }
    if (!verifySignature(writerGuidInfo, signature, message)) {
      return BADRESPONSE + " " + BADSIGNATURE;
    } else if (!verifyAccess(MetaDataTypeName.WRITE_WHITELIST, guidInfo, field, writerGuidInfo)) {
      return BADRESPONSE + " " + ACCESSDENIED;
    } else {
      if (fieldAccess.create(guidInfo.getGuid(), field, (value == null ? new ResultValue() : new ResultValue(Arrays.asList(value))))) {
        return OKRESPONSE;
      } else {
        return BADRESPONSE + " " + DUPLICATEFIELD;
      }
    }
  }

  private String processCreateList(String guid, String field, String value, String writer, String signature, String message)
          throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo guidInfo, writerGuidInfo;
    if ((guidInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (writer.equals(guid)) {
      writerGuidInfo = guidInfo;
    } else if ((writerGuidInfo = accountAccess.lookupGuidInfo(writer)) == null) {
      return BADRESPONSE + " " + BADWRITERGUID + " " + writer;
    }
    try {
      if (!verifySignature(writerGuidInfo, signature, message)) {
        return BADRESPONSE + " " + BADSIGNATURE;
      } else if (!verifyAccess(MetaDataTypeName.WRITE_WHITELIST, guidInfo, field, writerGuidInfo)) {
        return BADRESPONSE + " " + ACCESSDENIED;
      } else {
        if (fieldAccess.create(guidInfo.getGuid(), field, new ResultValue(value))) {
          return OKRESPONSE;
        } else {
          return BADRESPONSE + " " + DUPLICATEFIELD;
        }
      }
    } catch (JSONException e) {
      return BADRESPONSE + " " + JSONPARSEERROR;
    }
  }

  private String processUpdateOperation(String guid, String field, String value, String oldValue, String writer, String signature, String message,
          UpdateOperation operation) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo guidInfo, writerGuidInfo;
    if ((guidInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (writer.equals(guid)) {
      writerGuidInfo = guidInfo;
    } else if ((writerGuidInfo = accountAccess.lookupGuidInfo(writer)) == null) {
      return BADRESPONSE + " " + BADWRITERGUID + " " + writer;
    }
    if (!verifySignature(writerGuidInfo, signature, message)) {
      return BADRESPONSE + " " + BADSIGNATURE;
    } else if (!verifyAccess(MetaDataTypeName.WRITE_WHITELIST, guidInfo, field, writerGuidInfo)) {
      return BADRESPONSE + " " + ACCESSDENIED;
    } else {
      if (fieldAccess.update(guidInfo.getGuid(), field,
              new ResultValue(Arrays.asList(value)),
              oldValue != null ? new ResultValue(Arrays.asList(oldValue)) : null,
              operation)) {
        return OKRESPONSE;
      } else {
        return BADRESPONSE + " " + BADFIELD + " " + field;
      }
    }
  }

  private String processUpdateListOperation(String guid, String field, String value, String oldValue,
          String writer, String signature, String message, UpdateOperation operation)
          throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo guidInfo, writerGuidInfo;
    if ((guidInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (writer.equals(guid)) {
      writerGuidInfo = guidInfo;
    } else if ((writerGuidInfo = accountAccess.lookupGuidInfo(writer)) == null) {
      return BADRESPONSE + " " + BADWRITERGUID + " " + writer;
    }
    if (!verifySignature(writerGuidInfo, signature, message)) {
      return BADRESPONSE + " " + BADSIGNATURE;
    } else if (!verifyAccess(MetaDataTypeName.WRITE_WHITELIST, guidInfo, field, writerGuidInfo)) {
      return BADRESPONSE + " " + ACCESSDENIED;
    } else {
      try {
        if (fieldAccess.update(guidInfo.getGuid(), field,
                JSONUtils.JSONArrayToResultValue(new JSONArray(value)),
                oldValue != null ? JSONUtils.JSONArrayToResultValue(new JSONArray(oldValue)) : null,
                operation)) {
          return OKRESPONSE;
        } else {
          return BADRESPONSE + " " + BADFIELD + " " + field;
        }
      } catch (JSONException e) {
        return BADRESPONSE + " " + JSONPARSEERROR;
      }
    }
  }

  private String processUnsignedUpdateOperation(String guid, String field, String value, String oldValue, UpdateOperation operation) {
    GuidInfo guidInfo;
    if ((guidInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (!fieldWriteableByEveryone(guidInfo.getGuid(), field)) {
      return BADRESPONSE + " " + ACCESSDENIED;
    } else {
      if (fieldAccess.update(guidInfo.getGuid(), field,
              new ResultValue(Arrays.asList(value)),
              oldValue != null ? new ResultValue(Arrays.asList(oldValue)) : null,
              operation)) {
        return OKRESPONSE;
      } else {
        return BADRESPONSE + " " + BADFIELD + " " + field;
      }
    }
  }

  private String processUnsignedUpdateListOperation(String guid, String field, String value, String oldValue, UpdateOperation operation) {
    GuidInfo guidInfo;
    if ((guidInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (!fieldWriteableByEveryone(guidInfo.getGuid(), field)) {
      return BADRESPONSE + " " + ACCESSDENIED;
    } else {
      try {
        if (fieldAccess.update(guidInfo.getGuid(), field,
                JSONUtils.JSONArrayToResultValue(new JSONArray(value)),
                oldValue != null ? JSONUtils.JSONArrayToResultValue(new JSONArray(oldValue)) : null,
                operation)) {
          return OKRESPONSE;
        } else {
          return BADRESPONSE + " " + BADFIELD + " " + field;
        }
      } catch (JSONException e) {
        return BADRESPONSE + " " + JSONPARSEERROR;
      }
    }
  }

  private String processRead(String guid, String field, String reader, String signature, String message)
          throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo guidInfo, readerGuidInfo;
    if ((guidInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (reader.equals(guid)) {
      readerGuidInfo = guidInfo;
    } else if ((readerGuidInfo = accountAccess.lookupGuidInfo(reader)) == null) {
      return BADRESPONSE + " " + BADREADERGUID + " " + reader;
    }
    if (!verifySignature(readerGuidInfo, signature, message)) {
      return BADRESPONSE + " " + BADSIGNATURE;
    } else if (!verifyAccess(MetaDataTypeName.READ_WHITELIST, guidInfo, field, readerGuidInfo)) {
      return BADRESPONSE + " " + ACCESSDENIED;
    } else if (ALLFIELDS.equals(field)) {
      return fieldAccess.lookupMultipleValues(guid, ALLFIELDS);
    } else {
      return fieldAccess.lookup(guidInfo.getGuid(), field);
    }
  }

  private String processReadOne(String guid, String field, String reader, String signature, String message)
          throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo guidInfo, readerGuidInfo;
    if ((guidInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (reader.equals(guid)) {
      readerGuidInfo = guidInfo;
    } else if ((readerGuidInfo = accountAccess.lookupGuidInfo(reader)) == null) {
      return BADRESPONSE + " " + BADREADERGUID + " " + reader;
    }
    if (!verifySignature(readerGuidInfo, signature, message)) {
      return BADRESPONSE + " " + BADSIGNATURE;
    } else if (!verifyAccess(MetaDataTypeName.READ_WHITELIST, guidInfo, field, readerGuidInfo)) {
      return BADRESPONSE + " " + ACCESSDENIED;
    } else if (ALLFIELDS.equals(field)) {
      return fieldAccess.lookupOneMultipleValues(guid, ALLFIELDS);
    } else {
      return fieldAccess.lookupOne(guidInfo.getGuid(), field);
    }
  }

  private String processUnsignedRead(String guid, String field) {
    GuidInfo guidInfo;
    if ((guidInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (!fieldReadableByEveryone(guidInfo.getGuid(), field)) {
      return BADRESPONSE + " " + ACCESSDENIED;
    } else if (ALLFIELDS.equals(field)) {
      return fieldAccess.lookupMultipleValues(guid, ALLFIELDS);
    } else {
      return fieldAccess.lookup(guidInfo.getGuid(), field);
    }
  }

  private String processUnsignedReadOne(String guid, String field) {
    GuidInfo guidInfo;
    if ((guidInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (!fieldReadableByEveryone(guidInfo.getGuid(), field)) {
      return BADRESPONSE + " " + ACCESSDENIED;
    } else if (ALLFIELDS.equals(field)) {
      return fieldAccess.lookupOneMultipleValues(guid, ALLFIELDS);
    } else {
      return fieldAccess.lookupOne(guidInfo.getGuid(), field);
    }
  }

  private boolean fieldReadableByEveryone(String guid, String field) {
    return fieldMetaData.lookup(MetaDataTypeName.READ_WHITELIST, guid, field).contains(EVERYONE)
            || fieldMetaData.lookup(MetaDataTypeName.READ_WHITELIST, guid, ALLFIELDS).contains(EVERYONE);
  }

  private boolean fieldWriteableByEveryone(String guid, String field) {
    return fieldMetaData.lookup(MetaDataTypeName.WRITE_WHITELIST, guid, field).contains(EVERYONE)
            || fieldMetaData.lookup(MetaDataTypeName.WRITE_WHITELIST, guid, ALLFIELDS).contains(EVERYONE);
  }

  private String processSelect(String field, Object value) {
    return fieldAccess.select(field, value);
  }

  private String processSelectWithin(String field, String value) {
    return fieldAccess.selectWithin(field, value);
  }

  private String processSelectNear(String field, String value, String maxDistance) {
    return fieldAccess.selectNear(field, value, maxDistance);
  }
  
  private String processSelectQuery(String query) {
    return fieldAccess.selectQuery(query);
  }

  private String processAclAdd(String accessType, String guid, String field, String accesser, String signature, String message)
          throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    MetaDataTypeName access;
    if ((access = MetaDataTypeName.valueOf(accessType)) == null) {
      return BADRESPONSE + " " + BADACLTYPE + "Should be one of " + MetaDataTypeName.values().toString();
    }
    GuidInfo guidInfo;
    if ((guidInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (verifySignature(guidInfo, signature, message)) {
      fieldMetaData.add(access, guidInfo, field, accesser);
      return OKRESPONSE;
    } else {
      return BADRESPONSE + " " + BADSIGNATURE;
    }
  }

  private String processAclRemove(String accessType, String guid, String field, String accesser, String signature, String message)
          throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    MetaDataTypeName access;
    if ((access = MetaDataTypeName.valueOf(accessType)) == null) {
      return BADRESPONSE + " " + BADACLTYPE + "Should be one of " + MetaDataTypeName.values().toString();
    }
    GuidInfo guidInfo;
    if ((guidInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (verifySignature(guidInfo, signature, message)) {
      fieldMetaData.remove(access, guidInfo, field, accesser);
      return OKRESPONSE;
    } else {
      return BADRESPONSE + " " + BADSIGNATURE;
    }
  }

  private String processRetrieveAcl(String accessType, String guid, String field, String signature, String message)
          throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    MetaDataTypeName access;
    if ((access = MetaDataTypeName.valueOf(accessType)) == null) {
      return BADRESPONSE + " " + BADACLTYPE + "Should be one of " + MetaDataTypeName.values().toString();
    }
    GuidInfo guidInfo;
    if ((guidInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (verifySignature(guidInfo, signature, message)) {
      Set<String> values = fieldMetaData.lookup(access, guidInfo, field);
      return new JSONArray(values).toString();
    } else {
      return BADRESPONSE + " " + BADSIGNATURE;
    }
  }

  private String processAddToGroup(String guid, String member, String writer, String signature, String message)
          throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo guidInfo, writerInfo;
    if ((guidInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (writer.equals(guid)) {
      writerInfo = guidInfo;
    } else if ((writerInfo = accountAccess.lookupGuidInfo(writer)) == null) {
      return BADRESPONSE + " " + BADWRITERGUID + " " + writer;
    }
    if (!verifySignature(writerInfo, signature, message)) {
      return BADRESPONSE + " " + BADSIGNATURE;
    } else if (!verifyAccess(MetaDataTypeName.WRITE_WHITELIST, guidInfo, GROUP_ACL, writerInfo)) {
      return BADRESPONSE + " " + ACCESSDENIED;
    } else if (groupAccess.addToGroup(guid, member)) {
      return OKRESPONSE;
    } else {
      return BADRESPONSE + " " + GENERICEERROR;
    }
  }

  private String processAddMembersToGroup(String guid, String members, String writer, String signature, String message)
          throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo guidInfo, writerInfo;
    if ((guidInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (writer.equals(guid)) {
      writerInfo = guidInfo;
    } else if ((writerInfo = accountAccess.lookupGuidInfo(writer)) == null) {
      return BADRESPONSE + " " + BADWRITERGUID + " " + writer;
    }
    if (!verifySignature(writerInfo, signature, message)) {
      return BADRESPONSE + " " + BADSIGNATURE;
    } else if (!verifyAccess(MetaDataTypeName.WRITE_WHITELIST, guidInfo, GROUP_ACL, writerInfo)) {
      return BADRESPONSE + " " + ACCESSDENIED;
    } else {
      try {
        if (groupAccess.addToGroup(guid, new ResultValue(members))) {
          return OKRESPONSE;
        } else {
          return BADRESPONSE + " " + GENERICEERROR;
        }
      } catch (JSONException e) {
        return BADRESPONSE + " " + JSONPARSEERROR;
      }
    }
  }

  private String processRemoveFromGroup(String guid, String member, String writer, String signature, String message)
          throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo guidInfo, writerInfo;
    if ((guidInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (writer.equals(guid)) {
      writerInfo = guidInfo;
    } else if ((writerInfo = accountAccess.lookupGuidInfo(writer)) == null) {
      return BADRESPONSE + " " + BADWRITERGUID + " " + writer;
    }
    if (!verifySignature(writerInfo, signature, message)) {
      return BADRESPONSE + " " + BADSIGNATURE;
    } else if (!verifyAccess(MetaDataTypeName.WRITE_WHITELIST, guidInfo, GROUP_ACL, writerInfo)) {
      return BADRESPONSE + " " + ACCESSDENIED;
    } else if (groupAccess.removeFromGroup(guid, member)) {
      return OKRESPONSE;
    } else {
      return BADRESPONSE + " " + GENERICEERROR;
    }
  }

  private String processRemoveMembersFromGroup(String guid, String members, String writer, String signature, String message)
          throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo guidInfo, writerInfo;
    if ((guidInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (writer.equals(guid)) {
      writerInfo = guidInfo;
    } else if ((writerInfo = accountAccess.lookupGuidInfo(writer)) == null) {
      return BADRESPONSE + " " + BADWRITERGUID + " " + writer;
    }
    if (!verifySignature(writerInfo, signature, message)) {
      return BADRESPONSE + " " + BADSIGNATURE;
    } else if (!verifyAccess(MetaDataTypeName.WRITE_WHITELIST, guidInfo, GROUP_ACL, writerInfo)) {
      return BADRESPONSE + " " + ACCESSDENIED;
    } else {
      try {
        if (groupAccess.removeFromGroup(guid, new ResultValue(members))) {
          return OKRESPONSE;
        } else {
          return BADRESPONSE + " " + GENERICEERROR;
        }
      } catch (JSONException e) {
        return BADRESPONSE + " " + JSONPARSEERROR;
      }
    }
  }

  private String processGetGroupMembers(String guid, String reader, String signature, String message)
          throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo guidInfo, readInfo;
    if ((guidInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (reader.equals(guid)) {
      readInfo = guidInfo;
    } else if ((readInfo = accountAccess.lookupGuidInfo(reader)) == null) {
      return BADRESPONSE + " " + BADREADERGUID + " " + reader;
    }
    if (!verifySignature(readInfo, signature, message)) {
      return BADRESPONSE + " " + BADSIGNATURE;
    } else if (!verifyAccess(MetaDataTypeName.READ_WHITELIST, guidInfo, GROUP_ACL, readInfo)) {
      return BADRESPONSE + " " + ACCESSDENIED;
    } else {
      ResultValue values = groupAccess.lookup(guid);
      JSONArray list = new JSONArray(values);
      return list.toString();
    }
  }

  private String processRequestJoinGroup(String guid, String member, String signature, String message)
          throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo guidInfo, memberInfo;
    if ((guidInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (member.equals(guid)) {
      memberInfo = guidInfo;
    } else if ((memberInfo = accountAccess.lookupGuidInfo(member)) == null) {
      return BADRESPONSE + " " + BADREADERGUID + " " + member;
    }
    if (!verifySignature(memberInfo, signature, message)) {
      return BADRESPONSE + " " + BADSIGNATURE;
    } else {
      if (groupAccess.requestJoinGroup(guid, member)) {
        return OKRESPONSE;
      } else {
        return BADRESPONSE + " " + GENERICEERROR;
      }
    }
  }

  private String processRetrieveJoinGroupRequests(String guid, String signature, String message)
          throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo guidInfo;
    if ((guidInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (!verifySignature(guidInfo, signature, message)) {
      return BADRESPONSE + " " + BADSIGNATURE;
      // no need to verify ACL because only the GUID can access this
    } else {
      ResultValue values = groupAccess.retrieveGroupJoinRequests(guid);
      JSONArray list = new JSONArray(values);
      return list.toString();
    }
  }

  private String processGrantMembership(String guid, String member, String signature, String message)
          throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    return processGrantMemberships(guid, new ResultValue(new ArrayList(Arrays.asList(member))), signature, message);
  }

  private String processGrantMemberships(String guid, String members, String signature, String message)
          throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    try {
      return processGrantMemberships(guid, new ResultValue(members), signature, message);
    } catch (JSONException e) {
      return BADRESPONSE + " " + JSONPARSEERROR;
    }
  }

  private String processGrantMemberships(String guid, ResultValue members, String signature, String message)
          throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo guidInfo;
    if ((guidInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (!verifySignature(guidInfo, signature, message)) {
      return BADRESPONSE + " " + BADSIGNATURE;
      // no need to verify ACL because only the GUID can access this
    } else if (groupAccess.grantMembership(guid, members)) {
      return OKRESPONSE;
    } else {
      return BADRESPONSE + " " + GENERICEERROR;
    }
  }

  private String processRequestLeaveGroup(String guid, String member, String signature, String message)
          throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo guidInfo, memberInfo;
    if ((guidInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (member.equals(guid)) {
      memberInfo = guidInfo;
    } else if ((memberInfo = accountAccess.lookupGuidInfo(member)) == null) {
      return BADRESPONSE + " " + BADREADERGUID + " " + member;
    }
    if (!verifySignature(memberInfo, signature, message)) {
      return BADRESPONSE + " " + BADSIGNATURE;
    } else {
      if (groupAccess.requestLeaveGroup(guid, member)) {
        return OKRESPONSE;
      } else {
        return BADRESPONSE + " " + GENERICEERROR;
      }
    }
  }

  private String processRetrieveLeaveGroupRequests(String guid, String signature, String message)
          throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo guidInfo;
    if ((guidInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (!verifySignature(guidInfo, signature, message)) {
      return BADRESPONSE + " " + BADSIGNATURE;
      // no need to verify ACL because only the GUID can access this
    } else {
      ResultValue values = groupAccess.retrieveGroupLeaveRequests(guid);
      JSONArray list = new JSONArray(values);
      return list.toString();
    }
  }

  private String processRevokeMembership(String guid, String member, String signature, String message)
          throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    return processRevokeMemberships(guid, new ResultValue(new ArrayList(Arrays.asList(member))), signature, message);
  }

  private String processRevokeMemberships(String guid, String members, String signature, String message)
          throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    try {
      return processRevokeMemberships(guid, new ResultValue(members), signature, message);
    } catch (JSONException e) {
      return BADRESPONSE + " " + JSONPARSEERROR;
    }
  }

  private String processRevokeMemberships(String guid, ResultValue members, String signature, String message)
          throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo guidInfo;
    if ((guidInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (!verifySignature(guidInfo, signature, message)) {
      return BADRESPONSE + " " + BADSIGNATURE;
      // no need to verify ACL because only the GUID can access this
    } else if (groupAccess.revokeMembership(guid, members)) {
      return OKRESPONSE;
    } else {
      return BADRESPONSE + " " + GENERICEERROR;
    }
  }

  private String processAdmin(String host, String passkey, String inputLine) {
    if (host.equals(passkey)) {
      adminMode = true;
      return OKRESPONSE;
    } else if ("off".equals(passkey)) {
      adminMode = false;
      return OKRESPONSE;
    }
    return BADRESPONSE + " " + OPERATIONNOTSUPPORTED + " Don't understand " + ADMIN + QUERYPREFIX + inputLine;
  }

  // Currently only handles boolean parameters
  private String processSetParameter(String parameterString, String value) {
    if (adminMode) {
      try {
        SystemParameter.valueOf(parameterString.toUpperCase()).setFieldBoolean(Boolean.parseBoolean(value));
        return OKRESPONSE;
      } catch (Exception e) {
        System.out.println("Problem setting parameter: " + e);
      }
    }
    return BADRESPONSE + " " + OPERATIONNOTSUPPORTED + " Don't understand " + SETPARAMETER + " " + parameterString + " " + VALUE + " " + value;
  }

  // Currently only handles boolean parameters
  private String processGetParameter(String parameterString) {
    if (adminMode) {
      try {
        return SystemParameter.valueOf(parameterString.toUpperCase()).getFieldBoolean().toString();
      } catch (Exception e) {
        System.out.println("Problem getting parameter: " + e);
      }
    }
    return BADRESPONSE + " " + OPERATIONNOTSUPPORTED + " Don't understand " + GETPARAMETER + " " + parameterString;
  }

  private String processDump() {
    if (adminMode) {
      return Admintercessor.getInstance().sendDump();
    }
    return BADRESPONSE + " " + OPERATIONNOTSUPPORTED + " Don't understand " + DUMP;
  }

  private String processDump(String tagName) {
    if (adminMode) {
      return new JSONArray(Admintercessor.getInstance().collectTaggedGuids(tagName)).toString();
    }
    return BADRESPONSE + " " + OPERATIONNOTSUPPORTED + " Don't understand " + DUMP + QUERYPREFIX + NAME + VALSEP + tagName;
  }

  private String processDumpCache() {
    if (adminMode) {
      return Admintercessor.getInstance().sendDumpCache();
    }
    return BADRESPONSE + " " + OPERATIONNOTSUPPORTED + " Don't understand " + DUMPCACHE;
  }

  private String processDeleteAllRecords(String inputLine) {
    if (adminMode) {
      if (Admintercessor.getInstance().sendDeleteAllRecords()) {
        return OKRESPONSE;
      } else {
        return BADRESPONSE;
      }
    }
    return BADRESPONSE + " " + OPERATIONNOTSUPPORTED + " Don't understand " + DELETEALLRECORDS + QUERYPREFIX + inputLine;
  }

  private String processResetDatabase(String inputLine) {
    if (adminMode) {
      if (Admintercessor.getInstance().sendResetDB()) {
        return OKRESPONSE;
      } else {
        return BADRESPONSE;
      }
    }
    return BADRESPONSE + " " + OPERATIONNOTSUPPORTED + " Don't understand " + RESETDATABASE + QUERYPREFIX + inputLine;
  }

  private String processClearCache(String inputLine) {
    if (adminMode) {
      if (Admintercessor.getInstance().sendClearCache()) {
        return OKRESPONSE;
      } else {
        return BADRESPONSE;
      }
    }
    return BADRESPONSE + " " + OPERATIONNOTSUPPORTED + " Don't understand " + CLEARCACHE + QUERYPREFIX + inputLine;
  }

  private String processAddTag(String guid, String tag, String signature, String message)
          throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo guidInfo;
    if ((guidInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (verifySignature(guidInfo, signature, message)) {
      return accountAccess.addTag(guidInfo, tag);
    } else {
      return BADRESPONSE + " " + BADSIGNATURE;
    }
  }

  private String processRemoveTag(String guid, String tag, String signature, String message)
          throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    GuidInfo guidInfo;
    if ((guidInfo = accountAccess.lookupGuidInfo(guid)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (verifySignature(guidInfo, signature, message)) {
      return accountAccess.removeTag(guidInfo, tag);
    } else {
      return BADRESPONSE + " " + BADSIGNATURE;
    }
  }

  private String processGetTagged(String tagName) {
    return new JSONArray(Admintercessor.getInstance().collectTaggedGuids(tagName)).toString();
  }
  // currently doesn't handle subGuids that are tagged

  private String processClearTagged(String tagName) {
    for (String guid : Admintercessor.getInstance().collectTaggedGuids(tagName)) {
      AccountInfo accountInfo = accountAccess.lookupAccountInfoFromGuid(guid);
      if (accountInfo != null) {
        accountAccess.removeAccount(accountInfo);
      }
    }
    return OKRESPONSE;
  }

  /**
   * Top level routine to process queries for the http service *
   */
  public String processQuery(String host, String action, String queryString) {
    String fullString = action + QUERYPREFIX + queryString; // for signature check
    Map<String, String> queryMap = Util.parseURIQueryString(queryString);
    //String action = queryMap.get(ACTION);
    try {

      //
      // !!!DON'T FORGET TO PUT THE ONES WITH SHORTER ARGUMENT LISTS *AFTER* THE ONES WITH LONGER ARGUMENT LISTS!!!
      //
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
        return processRegisterAccountWithGuid(host, userName, guid, publicKey, password);
      } else if (REGISTERACCOUNT.equals(action) && queryMap.keySet().containsAll(Arrays.asList(NAME, PUBLICKEY, PASSWORD))) {
        // syntax: register userName guid public_key
        String userName = queryMap.get(NAME);
        String publicKey = queryMap.get(PUBLICKEY);
        String password = queryMap.get(PASSWORD);
        return processRegisterAccount(host, userName, publicKey, password);
      } else if (REGISTERACCOUNT.equals(action) && queryMap.keySet().containsAll(Arrays.asList(NAME, GUID, PUBLICKEY))) {
        // syntax: register userName guid public_key
        String userName = queryMap.get(NAME);
        String guid = queryMap.get(GUID);
        String publicKey = queryMap.get(PUBLICKEY);
        return processRegisterAccountWithGuid(host, userName, guid, publicKey, null);
      } else if (REGISTERACCOUNT.equals(action) && queryMap.keySet().containsAll(Arrays.asList(NAME, PUBLICKEY))) {
        // syntax: register userName guid public_key
        String userName = queryMap.get(NAME);
        String publicKey = queryMap.get(PUBLICKEY);
        return processRegisterAccount(host, userName, publicKey, null);
      } else if (VERIFYACCOUNT.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, CODE))) {
        // syntax: register userName guid public_key
        String guid = queryMap.get(GUID);
        String code = queryMap.get(CODE);
        return processVerifyAccount(guid, code);
      } else if (REMOVEACCOUNT.equals(action) && queryMap.keySet().containsAll(Arrays.asList(NAME, GUID, SIGNATURE))) {
        // syntax: remove userName guid public_key
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
      } else if (REMOVEGUID.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, GUID2, SIGNATURE))) {
        // syntax: register userName guid public_key
        String guid = queryMap.get(GUID);
        String guid2 = queryMap.get(GUID2);
        String signature = queryMap.get(SIGNATURE);
        return processRemoveGuid(guid, guid2, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
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
      } else if (LOOKUPACCOUNTRECORD.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID))) {
        String guid = queryMap.get(GUID);
        return processLookupAccountInfo(guid);
        //
        // READ OPERATIONS
        //
      } else if (READ.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, READER, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String reader = queryMap.get(READER);
        String signature = queryMap.get(SIGNATURE);
        return processRead(guid, field, reader, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
      } else if (READ.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String signature = queryMap.get(SIGNATURE);
        return processRead(guid, field, guid, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
      } else if (READONE.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, READER, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String reader = queryMap.get(READER);
        String signature = queryMap.get(SIGNATURE);
        return processReadOne(guid, field, reader, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
      } else if (READONE.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String signature = queryMap.get(SIGNATURE);
        return processReadOne(guid, field, guid, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
        // UNSIGNED READ
      } else if (READ.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        return processUnsignedRead(guid, field);
      } else if (READONE.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        return processUnsignedReadOne(guid, field);
        //
        // CREATE OPERATIONS
        //
      } else if (CREATE.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, WRITER, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String writer = queryMap.get(WRITER);
        String signature = queryMap.get(SIGNATURE);
        return processCreate(guid, field, value, writer, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
      } else if (CREATE.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, WRITER, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String writer = queryMap.get(WRITER);
        String signature = queryMap.get(SIGNATURE);
        return processCreate(guid, field, null, writer, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
      } else if (CREATE.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String signature = queryMap.get(SIGNATURE);
        return processCreate(guid, field, value, guid, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
      } else if (CREATE.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String signature = queryMap.get(SIGNATURE);
        return processCreate(guid, field, null, guid, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
      } else if (CREATELIST.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, WRITER, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String writer = queryMap.get(WRITER);
        String signature = queryMap.get(SIGNATURE);
        return processCreateList(guid, field, value, writer, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
      } else if (CREATELIST.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String signature = queryMap.get(SIGNATURE);
        return processCreateList(guid, field, value, guid, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
        //
        // UPDATE OPERATIONS WITH DIFFERENT WRITER
        //
      } else if (REPLACE.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, WRITER, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String writer = queryMap.get(WRITER);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateOperation(guid, field, value, null, writer, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.REPLACE_ALL);
      } else if (APPENDWITHDUPLICATION.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, WRITER, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String writer = queryMap.get(WRITER);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateOperation(guid, field, value, null, writer, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.APPEND_WITH_DUPLICATION);
      } else if (APPEND.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, WRITER, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String writer = queryMap.get(WRITER);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateOperation(guid, field, value, null, writer, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.APPEND);
      } else if (APPENDORCREATE.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, WRITER, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String writer = queryMap.get(WRITER);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateOperation(guid, field, value, null, writer, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.APPEND_OR_CREATE);
      } else if (REPLACEORCREATE.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, WRITER, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String writer = queryMap.get(WRITER);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateOperation(guid, field, value, null, writer, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.REPLACE_ALL_OR_CREATE);
      } else if (REMOVE.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, WRITER, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String writer = queryMap.get(WRITER);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateOperation(guid, field, value, null, writer, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.REMOVE);
      } else if (REPLACELIST.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, WRITER, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String writer = queryMap.get(WRITER);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateListOperation(guid, field, value, null, writer, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.REPLACE_ALL);
      } else if (APPENDLISTWITHDUPLICATION.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, WRITER, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String writer = queryMap.get(WRITER);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateListOperation(guid, field, value, null, writer, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.APPEND_WITH_DUPLICATION);
      } else if (APPENDLIST.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, WRITER, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String writer = queryMap.get(WRITER);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateListOperation(guid, field, value, null, writer, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.APPEND);
      } else if (APPENDORCREATELIST.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, WRITER, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String writer = queryMap.get(WRITER);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateListOperation(guid, field, value, null, writer, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.APPEND_OR_CREATE);
      } else if (REPLACEORCREATELIST.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, WRITER, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String writer = queryMap.get(WRITER);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateListOperation(guid, field, value, null, writer, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.REPLACE_ALL_OR_CREATE);
      } else if (REMOVELIST.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, WRITER, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String writer = queryMap.get(WRITER);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateListOperation(guid, field, value, null, writer, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.REMOVE);
      } else if (SUBSTITUTE.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, OLDVALUE, WRITER, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String oldValue = queryMap.get(OLDVALUE);
        String writer = queryMap.get(WRITER);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateOperation(guid, field, value, oldValue, writer, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.SUBSTITUTE);
      } else if (SUBSTITUTELIST.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, OLDVALUE, WRITER, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String oldValue = queryMap.get(OLDVALUE);
        String writer = queryMap.get(WRITER);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateListOperation(guid, field, value, oldValue, writer, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.SUBSTITUTE);
      } else if (CLEAR.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, WRITER, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String writer = queryMap.get(WRITER);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateOperation(guid, field, "", null, writer, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.CLEAR);
      } else if (REMOVEFIELD.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, WRITER, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String writer = queryMap.get(WRITER);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateOperation(guid, field, "", null, writer, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.REMOVE_FIELD);
        //
        // UPDATE OPERATIONS WITH NO WRITER
        //
      } else if (REPLACE.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateOperation(guid, field, value, null, guid, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.REPLACE_ALL);
      } else if (APPENDWITHDUPLICATION.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateOperation(guid, field, value, null, guid, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.APPEND_WITH_DUPLICATION);
      } else if (APPEND.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateOperation(guid, field, value, null, guid, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.APPEND);
      } else if (APPENDORCREATE.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateOperation(guid, field, value, null, guid, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.APPEND_OR_CREATE);
      } else if (REPLACEORCREATE.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateOperation(guid, field, value, null, guid, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.REPLACE_ALL_OR_CREATE);
      } else if (REMOVE.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateOperation(guid, field, value, null, guid, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.REMOVE);
      } else if (REPLACELIST.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateListOperation(guid, field, value, null, guid, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.REPLACE_ALL);
      } else if (APPENDLISTWITHDUPLICATION.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateListOperation(guid, field, value, null, guid, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.APPEND_WITH_DUPLICATION);
      } else if (APPENDLIST.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateListOperation(guid, field, value, null, guid, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.APPEND);
      } else if (APPENDORCREATELIST.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateListOperation(guid, field, value, null, guid, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.APPEND_OR_CREATE);
      } else if (REPLACEORCREATELIST.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateListOperation(guid, field, value, null, guid, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.REPLACE_ALL_OR_CREATE);
      } else if (REMOVELIST.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateListOperation(guid, field, value, null, guid, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.REMOVE);
      } else if (SUBSTITUTE.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, OLDVALUE, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String oldValue = queryMap.get(OLDVALUE);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateOperation(guid, field, value, oldValue, guid, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.SUBSTITUTE);
      } else if (SUBSTITUTELIST.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, OLDVALUE, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String oldValue = queryMap.get(OLDVALUE);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateListOperation(guid, field, value, oldValue, guid, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.SUBSTITUTE);
      } else if (CLEAR.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateOperation(guid, field, "", null, guid, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.CLEAR);
      } else if (REMOVEFIELD.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String signature = queryMap.get(SIGNATURE);
        return processUpdateOperation(guid, field, "", null, guid, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature),
                UpdateOperation.REMOVE_FIELD);
         //
        // UNSIGNED UPDATE OPERATIONS
        //
      } else if (REPLACE.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        return processUnsignedUpdateOperation(guid, field, value, null, UpdateOperation.REPLACE_ALL);
      } else if (APPENDWITHDUPLICATION.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        return processUnsignedUpdateOperation(guid, field, value, null, UpdateOperation.APPEND_WITH_DUPLICATION);
      } else if (APPEND.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        return processUnsignedUpdateOperation(guid, field, value, null, UpdateOperation.APPEND);
      } else if (APPENDORCREATE.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        return processUnsignedUpdateOperation(guid, field, value, null, UpdateOperation.APPEND_OR_CREATE);
      } else if (REPLACEORCREATE.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        return processUnsignedUpdateOperation(guid, field, value, null, UpdateOperation.REPLACE_ALL_OR_CREATE);
      } else if (REMOVE.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        return processUnsignedUpdateOperation(guid, field, value, null, UpdateOperation.REMOVE);
      } else if (REPLACELIST.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        return processUnsignedUpdateListOperation(guid, field, value, null, UpdateOperation.REPLACE_ALL);
      } else if (APPENDLISTWITHDUPLICATION.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        return processUnsignedUpdateListOperation(guid, field, value, null, UpdateOperation.APPEND_WITH_DUPLICATION);
      } else if (APPENDLIST.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        return processUnsignedUpdateListOperation(guid, field, value, null, UpdateOperation.APPEND);
      } else if (APPENDORCREATELIST.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        return processUnsignedUpdateListOperation(guid, field, value, null, UpdateOperation.APPEND_OR_CREATE);
      } else if (REPLACEORCREATELIST.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        return processUnsignedUpdateListOperation(guid, field, value, null, UpdateOperation.REPLACE_ALL_OR_CREATE);
      } else if (REMOVELIST.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        return processUnsignedUpdateListOperation(guid, field, value, null, UpdateOperation.REMOVE);
      } else if (SUBSTITUTE.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, OLDVALUE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String oldValue = queryMap.get(OLDVALUE);
        return processUnsignedUpdateOperation(guid, field, value, oldValue, UpdateOperation.SUBSTITUTE);
      } else if (SUBSTITUTELIST.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD, VALUE, OLDVALUE))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        String value = queryMap.get(VALUE);
        String oldValue = queryMap.get(OLDVALUE);
        return processUnsignedUpdateListOperation(guid, field, value, oldValue, UpdateOperation.SUBSTITUTE);
      } else if (CLEAR.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        return processUnsignedUpdateOperation(guid, field, "", null, UpdateOperation.CLEAR);
      } else if (REMOVEFIELD.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, FIELD))) {
        String guid = queryMap.get(GUID);
        String field = queryMap.get(FIELD);
        return processUnsignedUpdateOperation(guid, field, "", null, UpdateOperation.REMOVE_FIELD);
        //
        // SELECT OPERATIONS
        //
      } else if (SELECT.equals(action) && queryMap.keySet().containsAll(Arrays.asList(FIELD, VALUE))) {
        String field = queryMap.get(FIELD);
        Object value = queryMap.get(VALUE);
        return processSelect(field, value);
      } else if (SELECT.equals(action) && queryMap.keySet().containsAll(Arrays.asList(FIELD, NEAR, MAXDISTANCE))) {
        String field = queryMap.get(FIELD);
        String near = queryMap.get(NEAR);
        String maxDistance = queryMap.get(MAXDISTANCE);
        return processSelectNear(field, near, maxDistance);
      } else if (SELECT.equals(action) && queryMap.keySet().containsAll(Arrays.asList(FIELD, WITHIN))) {
        String field = queryMap.get(FIELD);
        String within = queryMap.get(WITHIN);
        return processSelectWithin(field, within);
      } else if (SELECT.equals(action) && queryMap.keySet().containsAll(Arrays.asList(QUERY))) {
        String query = queryMap.get(QUERY);
        return processSelectQuery(query);
        //
        // ACL OPERATIONS
        //
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
        //
        // GROUP OPERATIONS
        //
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
      } else if (ADDTOGROUP.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, MEMBERS, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String members = queryMap.get(MEMBERS);
        String signature = queryMap.get(SIGNATURE);
        return processAddMembersToGroup(guid, members, guid, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
      } else if (ADDTOGROUP.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, MEMBERS, WRITER, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String members = queryMap.get(MEMBERS);
        String writer = queryMap.get(WRITER);
        String signature = queryMap.get(SIGNATURE);
        return processAddMembersToGroup(guid, members, writer, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
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
      } else if (REMOVEFROMGROUP.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, MEMBERS, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String members = queryMap.get(MEMBERS);
        String signature = queryMap.get(SIGNATURE);
        return processRemoveMembersFromGroup(guid, members, guid, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
      } else if (REMOVEFROMGROUP.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, MEMBERS, WRITER, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String members = queryMap.get(MEMBERS);
        String writer = queryMap.get(WRITER);
        String signature = queryMap.get(SIGNATURE);
        return processRemoveMembersFromGroup(guid, members, writer, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
      } else if (GETGROUPMEMBERS.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, READER, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String reader = queryMap.get(READER);
        String signature = queryMap.get(SIGNATURE);
        return processGetGroupMembers(guid, reader, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
      } else if (GETGROUPMEMBERS.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String signature = queryMap.get(SIGNATURE);
        return processGetGroupMembers(guid, guid, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
      } else if (REQUESTJOINGROUP.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, MEMBER, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String member = queryMap.get(MEMBER);
        String signature = queryMap.get(SIGNATURE);
        return processRequestJoinGroup(guid, member, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
      } else if (RETRIEVEGROUPJOINREQUESTS.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String signature = queryMap.get(SIGNATURE);
        return processRetrieveJoinGroupRequests(guid, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
      } else if (GRANTMEMBERSHIP.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, MEMBERS, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String members = queryMap.get(MEMBERS);
        String signature = queryMap.get(SIGNATURE);
        return processGrantMemberships(guid, members, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
      } else if (GRANTMEMBERSHIP.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, MEMBER, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String member = queryMap.get(MEMBER);
        String signature = queryMap.get(SIGNATURE);
        return processGrantMembership(guid, member, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
      } else if (REQUESTLEAVEGROUP.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, MEMBER, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String member = queryMap.get(MEMBER);
        String signature = queryMap.get(SIGNATURE);
        return processRequestLeaveGroup(guid, member, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
      } else if (RETRIEVEGROUPLEAVEREQUESTS.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String signature = queryMap.get(SIGNATURE);
        return processRetrieveLeaveGroupRequests(guid, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
      } else if (REVOKEMEMBERSHIP.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, MEMBERS, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String members = queryMap.get(MEMBERS);
        String signature = queryMap.get(SIGNATURE);
        return processRevokeMemberships(guid, members, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
      } else if (REVOKEMEMBERSHIP.equals(action) && queryMap.keySet().containsAll(Arrays.asList(GUID, MEMBER, SIGNATURE))) {
        String guid = queryMap.get(GUID);
        String member = queryMap.get(MEMBER);
        String signature = queryMap.get(SIGNATURE);
        return processRevokeMembership(guid, member, signature, removeSignature(fullString, KEYSEP + SIGNATURE + VALSEP + signature));
        //
        // MISC OPERATIONS
        //
        // ADMIN
      } else if (ADMIN.equals(action) && queryMap.keySet().containsAll(Arrays.asList(PASSKEY))) {
        // pass in the host to use as a passkey check
        return processAdmin(host, queryMap.get(PASSKEY), queryString);
      } else if (SETPARAMETER.equals(action) && queryMap.keySet().containsAll(Arrays.asList(NAME, VALUE))) {
        String parameter = queryMap.get(NAME);
        String value = queryMap.get(VALUE);
        return processSetParameter(parameter, value);
      } else if (GETPARAMETER.equals(action) && queryMap.keySet().containsAll(Arrays.asList(NAME))) {
        String parameter = queryMap.get(NAME);
        return processGetParameter(parameter);
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

  private boolean verifySignature(GuidInfo guidInfo, String signature, String message) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    if (!GNS.enableSignatureVerification) {
      return true;
    }
    byte[] encodedPublicKey = Base64.decode(guidInfo.getPublicKey());
    if (encodedPublicKey == null) { // bogus signature
      return false;
    }
    KeyFactory keyFactory = KeyFactory.getInstance(RASALGORITHM);
    X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(encodedPublicKey);
    PublicKey publicKey = keyFactory.generatePublic(publicKeySpec);

    Signature sig = Signature.getInstance(SIGNATUREALGORITHM);
    sig.initVerify(publicKey);
    sig.update(message.getBytes());
    boolean result = sig.verify(ByteUtils.hexStringToByteArray(signature));
    GNS.getLogger().fine("User " + guidInfo.getName() + (result ? " verified " : " NOT verified ") + "as author of message " + message);
    return result;
  }

  /**
   * Checks to see if the reader given in readerInfo can access all of the fields of the user given by guidInfo.
   *
   * @param access
   * @param contectInfo
   * @param readerInfo
   * @return
   */
  private boolean verifyAccess(MetaDataTypeName access, GuidInfo contectInfo, GuidInfo readerInfo) {
    return verifyAccess(access, contectInfo, ALLFIELDS, readerInfo);
  }

  /**
   * Checks to see if the reader given in readerInfo can access the field of the user given by guidInfo. Access type is some combo
   * of read, write, blacklist and whitelist. Note: Blacklists are currently not activated.
   *
   * @param access
   * @param guidInfo
   * @param field
   * @param accessorInfo
   * @return
   */
  private boolean verifyAccess(MetaDataTypeName access, GuidInfo guidInfo, String field, GuidInfo accessorInfo) {
    GNS.getLogger().finer("User: " + guidInfo.getName() + " Reader: " + accessorInfo.getName() + " Field: " + field);
    if (guidInfo.getGuid().equals(accessorInfo.getGuid())) {
      return true; // can always read your own stuff
    } else {
      Set<String> allowedusers = fieldMetaData.lookup(access, guidInfo, field);
      GNS.getLogger().fine(guidInfo.getName() + " allowed users of " + field + " : " + allowedusers);
      if (checkAllowedUsers(accessorInfo.getGuid(), allowedusers)) {
        GNS.getLogger().fine("User " + accessorInfo.getName() + " allowed to access user " + guidInfo.getName() + "'s " + field + " field");
        return true;
      }
      // otherwise find any users that can access all of the fields
      allowedusers = fieldMetaData.lookup(access, guidInfo, ALLFIELDS);
      if (checkAllowedUsers(accessorInfo.getGuid(), allowedusers)) {
        GNS.getLogger().fine("User " + accessorInfo.getName() + " allowed to access all of user " + guidInfo.getName() + "'s fields");
        return true;
      }
    }
    GNS.getLogger().fine("User " + accessorInfo.getName() + " NOT allowed to access user " + guidInfo.getName() + "'s " + field + " field");
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
