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
 *  Initial developer(s): Abhigyan Sharma, Westy
 *
 */
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport;

import edu.umass.cs.gnscommon.GnsProtocol;
import edu.umass.cs.gnsserver.gnsApp.QueryResult;
import edu.umass.cs.gnscommon.utils.Base64;
import static edu.umass.cs.gnscommon.GnsProtocol.*;
import edu.umass.cs.gnsserver.exceptions.GnsRuntimeException;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.gnsApp.AppReconfigurableNodeOptions;
import edu.umass.cs.gnscommon.utils.ByteUtils;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnsserver.utils.Email;
import edu.umass.cs.gnsserver.gnsApp.NSResponseCode;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.json.JSONException;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Provides the basic interface to GNS accounts.
 * <p>
 * See {@link AccountInfo} for more details about accounts.
 * <p>
 * Some of the internal records used to maintain account information are as follows:
 * <p>
 * GUID: "ACCOUNT_INFO" -- {account} for primary guid<br>
 * GUID: "GUID" -- GUID (primary) for secondary guid<br>
 * GUID: "GUID_INFO" -- {guid info}<br>
 * HRN: "GUID" -- GUID<br>
 * <p>
 * GUID = Globally Unique Identifier<br>
 * HRN = Human Readable Name<br>
 *
 * @author westy
 */
public class AccountAccess {

  /**
   * Defines the field name in an account guid where account information is stored.
   */
  public static final String ACCOUNT_INFO = InternalField.makeInternalFieldString("account_info");

  /**
   * Defines the field name in an HRN record (the reverse record) where guid is stored.
   */
  public static final String HRN_GUID = InternalField.makeInternalFieldString("guid");

  /**
   * Defines the field name in the subguid where parent guid is stored.
   */
  public static final String PRIMARY_GUID = InternalField.makeInternalFieldString("primary_guid");

  /**
   * Defines the field name in the guid where guid info is stored.
   */
  public static final String GUID_INFO = InternalField.makeInternalFieldString("guid_info");

  private static MessageDigest messageDigest;

  static {
    try {
      messageDigest = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      GNS.getLogger().severe("Unable to initialize for authentication:" + e);
    }
  }

  /**
   * Obtains the account info record for the given GUID if that GUID
   * was used to create an account.
   * <p>
   * GUID: "ACCOUNT_INFO" -- {account} for primary guid<br>
   * GUID: "GUID" -- GUID (primary) for secondary guid<br>
   * GUID: "GUID_INFO" -- {guid info}<br>
   * HRN: "GUID" -- GUID<br>
   * <p>
   * GUID = Globally Unique Identifier<br>
   * HRN = Human Readable Name<br>
   *
   * @param guid
   * @param handler
   * @return the account info record or null if it could not be found
   */
  public static AccountInfo lookupAccountInfoFromGuid(String guid,
          //boolean allowSubGuids, 
          ClientRequestHandlerInterface handler) {
    QueryResult<String> accountResult = handler.getIntercessor().sendFullQueryBypassingAuthentication(guid, ACCOUNT_INFO);
    //QueryResult<String> accountResult = handler.getIntercessor().sendSingleFieldQueryBypassingAuthentication(guid, ACCOUNT_INFO);
    if (AppReconfigurableNodeOptions.debuggingEnabled) {
      GNS.getLogger().fine("###QUERY RESULT:" + accountResult);
    }
    if (!accountResult.isError()) {
      try {
        return new AccountInfo(accountResult.getValuesMap().getJSONObject(ACCOUNT_INFO));
        //return new AccountInfo(accountResult.getArray(ACCOUNT_INFO).toResultValueString());
      } catch (JSONException e) {
        GNS.getLogger().severe("Problem parsing accountinfo:" + e);
      } catch (ParseException e) {
        GNS.getLogger().severe("Problem parsing accountinfo:" + e);
      }
    }
    return null;
  }

  /**
   * If GUID is associated with another account, returns the GUID of that account,
   * otherwise returns null.
   * <p>
   * GUID = Globally Unique Identifier
   *
   * @param guid
   * @param handler
   * @return a GUID
   */
  public static String lookupPrimaryGuid(String guid, ClientRequestHandlerInterface handler) {

    QueryResult<String> guidResult = handler.getIntercessor().sendFullQueryBypassingAuthentication(guid, PRIMARY_GUID);
    //QueryResult<String> guidResult = handler.getIntercessor().sendSingleFieldQueryBypassingAuthentication(guid, PRIMARY_GUID);
    try {
      if (!guidResult.isError()) {
        return guidResult.getValuesMap().getString(PRIMARY_GUID);
        //return (String) guidResult.getArray(PRIMARY_GUID).get(0);
      }
    } catch (JSONException e) {
    }
    return null;
  }

  /**
   * Returns the GUID associated with name which is a HRN or null if one of that name does not exist.
   * <p>
   * GUID = Globally Unique Identifier<br>
   * HRN = Human Readable Name<br>
   *
   * @param name
   * @param handler
   * @return a GUID
   */
  public static String lookupGuid(String name, ClientRequestHandlerInterface handler) {

    try {
      QueryResult<String> guidResult = handler.getIntercessor().sendFullQueryBypassingAuthentication(name, HRN_GUID);
      //QueryResult<String> guidResult = handler.getIntercessor().sendSingleFieldQueryBypassingAuthentication(name, HRN_GUID);
      if (!guidResult.isError()) {
        return guidResult.getValuesMap().getString(HRN_GUID);
        //return (String) guidResult.getArray(HRN_GUID).get(0);
      }
    } catch (JSONException e) {
    }
    return null;
  }

  /**
   * Obtains the guid info record from the database for GUID given.
   * <p>
   * GUID = Globally Unique Identifier<br>
   *
   * @param guid
   * @param handler
   * @return an {@link GuidInfo} instance
   */
  public static GuidInfo lookupGuidInfo(String guid, ClientRequestHandlerInterface handler) {

    QueryResult<String> guidResult = handler.getIntercessor().sendFullQueryBypassingAuthentication(guid, GUID_INFO);
    //QueryResult<String> guidResult = handler.getIntercessor().sendSingleFieldQueryBypassingAuthentication(guid, GUID_INFO);
    if (!guidResult.isError()) {
      try {
        return new GuidInfo(guidResult.getValuesMap().getJSONObject(GUID_INFO));
        //return new GuidInfo(guidResult.getArray(GUID_INFO).toResultValueString());
      } catch (JSONException e) {
        GNS.getLogger().severe("Problem parsing guidinfo:" + e);
      } catch (ParseException e) {
        GNS.getLogger().severe("Problem parsing guidinfo:" + e);
      }
    }
    return null;
  }

  /**
   * Obtains the account info record from the database for the account whose HRN is name.
   * <p>
   * HRN = Human Readable Name<br>
   *
   * @param name
   * @param handler
   * @return an {@link AccountInfo} instance
   */
  public static AccountInfo lookupAccountInfoFromName(String name, ClientRequestHandlerInterface handler) {
    String guid = lookupGuid(name, handler);
    if (guid != null) {
      return lookupAccountInfoFromGuid(guid, handler);
    }
    return null;
  }
  private static final String VERIFY_COMMAND = "account_verify";
  private static final String EMAIL_BODY = "This is an automated message informing you that an account has been created for %s on the GNS server.\n\n"
          + "This is the verification code: %s\n\n"
          + "To verify this account you can enter this query into a browser:\n\n"
          + "http://%s/" + GNS.GNS_URL_PATH + "/verifyAccount?guid=%s&code=%s\n\n"
          + "or enter this command into the GNS CLI that you used to create the account:\n\n"
          + VERIFY_COMMAND + " %s %s\n\n"
          + "If you did not create this account please ignore this message.";
  private static final String SUCCESS_NOTICE = "A confirmation email has been sent to %s. "
          + "Please follow the instructions in that email to verify your account.\n";
  private static final String PROBLEM_NOTICE = "There is some system problem in sending your confirmation email to %s. "
          + "Your account has been created. Please email us at %s and we will attempt to fix the problem.\n";
  //
  private static final String ADMIN_NOTICE = "This is an automated message informing you that an account has been created for %s on the GNS server at %s.\n"
          + "You can view their information using the link below:\n\nhttp://register.gns.name/admin/showuser.php?show=%s \n";

  /**
   * Adds an account guid.
   *
   * @param host
   * @param name
   * @param guid
   * @param publicKey
   * @param password
   * @param handler
   * @return the command response
   */
  public static CommandResponse<String> addAccountWithVerification(final String host, final String name, final String guid,
          String publicKey, String password,
          ClientRequestHandlerInterface handler) {

    CommandResponse<String> response;
    String verifyCode = createVerificationCode(name); // make this even if we don't need it
    if ((response = addAccount(name, guid, publicKey, password,
            GNS.enableEmailAccountVerification, verifyCode, handler)).getReturnValue().equals(OK_RESPONSE)) {
      if (GNS.enableEmailAccountVerification) {
        // if (updateAccountInfoNoAuthentication(accountInfo, handler)) {
        boolean emailOK = Email.email("GNS Account Verification", name,
                String.format(EMAIL_BODY, name, verifyCode, host, guid, verifyCode, name, verifyCode));
        // do the admin email in another thread so it's faster and because we don't care if it completes
        (new Thread() {
          @Override
          public void run() {
            boolean adminEmailOK = Email.email("GNS Account Notification",
                    Email.ACCOUNT_CONTACT_EMAIL,
                    String.format(ADMIN_NOTICE, name, host, guid));
          }
        }).start();

        if (emailOK) {
          return new CommandResponse<String>(OK_RESPONSE);
        } else {
          // if we can't send the confirmation back out of the account creation
          AccountInfo accountInfo = lookupAccountInfoFromGuid(guid, handler);
          if (accountInfo != null) {
            removeAccount(accountInfo, handler);
          }
          return new CommandResponse<String>(BAD_RESPONSE + " " + VERIFICATION_ERROR + " " + "Unable to send verification email");
        }
      } else {
        if (AppReconfigurableNodeOptions.debuggingEnabled) {
          GNS.getLogger().warning("**** EMAIL VERIFICATION IS OFF! ****");
        }
      }
    }
    return response;
  }

  private static final int VERIFICATION_CODE_LENGTH = 3; // Six hex characters

  private static final String SECRET = "AN4pNmLGcGQGKwtaxFFOKG05yLlX0sXRye9a3awdQd2aNZ5P1ZBdpdy98Za3qcE"
          + "o0u6BXRBZBrcH8r2NSbqpOoWfvcxeSC7wSiOiVHN7fW0eFotdFz0fiKjHj3h0ri";

  private static String createVerificationCode(String name) {
    // Take the first N bytes of the array for our code
    return ByteUtils.toHex(Arrays.copyOf(SHA1HashFunction.getInstance().hash(name + SECRET), VERIFICATION_CODE_LENGTH));
  }

  private static final long TWO_HOURS_IN_MILLESECONDS = 60 * 60 * 1000 * 2;

  /**
   * Performs the account verification for a given guid using the verification code.
   *
   * @param guid
   * @param code
   * @param handler
   * @return the command response
   */
  public static CommandResponse<String> verifyAccount(String guid, String code, ClientRequestHandlerInterface handler) {
    AccountInfo accountInfo;
    if ((accountInfo = lookupAccountInfoFromGuid(guid, handler)) == null) {
      return new CommandResponse<String>(GnsProtocol.BAD_RESPONSE + " " + GnsProtocol.VERIFICATION_ERROR + " " + "Unable to read account info");
    }
    if (accountInfo.isVerified()) {
      return new CommandResponse<String>(GnsProtocol.BAD_RESPONSE + " " + GnsProtocol.VERIFICATION_ERROR + " " + "Account already verified");
    }
    if (accountInfo.getVerificationCode() == null && code == null) {
      return new CommandResponse<String>(GnsProtocol.BAD_RESPONSE + " " + GnsProtocol.VERIFICATION_ERROR + " " + "Bad verification code");
    }
    if ((new Date()).getTime() - accountInfo.getCreated().getTime() > TWO_HOURS_IN_MILLESECONDS) {
      return new CommandResponse<String>(GnsProtocol.BAD_RESPONSE + " " + GnsProtocol.VERIFICATION_ERROR + " " + "Account code no longer valid");
    }
    if (!accountInfo.getVerificationCode().equals(code)) {
      return new CommandResponse<String>(GnsProtocol.BAD_RESPONSE + " " + GnsProtocol.VERIFICATION_ERROR + " " + "Code not correct");
    }
    accountInfo.setVerificationCode(null);
    accountInfo.setVerified(true);
    accountInfo.noteUpdate();
    if (updateAccountInfoNoAuthentication(accountInfo, handler, false)) {
      return new CommandResponse<String>(GnsProtocol.OK_RESPONSE + " " + "Your account has been verified."); // add a little something for the kids
    } else {
      return new CommandResponse<String>(GnsProtocol.BAD_RESPONSE + " " + GnsProtocol.VERIFICATION_ERROR + " " + "Unable to update account info");
    }
  }

  /**
   * Reset the public key for a guid.
   *
   * @param guid
   * @param password
   * @param publicKey
   * @param handler
   * @return the command response
   */
  public static CommandResponse<String> resetPublicKey(String guid, String password, String publicKey,
          ClientRequestHandlerInterface handler) {
    AccountInfo accountInfo;
    if ((accountInfo = lookupAccountInfoFromGuid(guid, handler)) == null) {
      return new CommandResponse<String>(GnsProtocol.BAD_RESPONSE + " " + GnsProtocol.VERIFICATION_ERROR + " " + "Not an account guid");
    }
    if (verifyPassword(accountInfo, password)) {
      GuidInfo guidInfo;
      if ((guidInfo = lookupGuidInfo(guid, handler)) == null) {
        return new CommandResponse<String>(GnsProtocol.BAD_RESPONSE + " " + GnsProtocol.VERIFICATION_ERROR + " " + "Unable to read guid info");
      } else {
        guidInfo.setPublicKey(publicKey);
        guidInfo.noteUpdate();
        if (updateGuidInfoNoAuthentication(guidInfo, handler)) {
          return new CommandResponse<String>(GnsProtocol.OK_RESPONSE + " " + "Public key has been updated.");
        } else {
          return new CommandResponse<String>(GnsProtocol.BAD_RESPONSE + " " + GnsProtocol.VERIFICATION_ERROR + " " + "Unable to update guid info");
        }
      }
    } else {
      return new CommandResponse<String>(GnsProtocol.BAD_RESPONSE + " " + GnsProtocol.VERIFICATION_ERROR + " " + "Password mismatch");
    }
  }

  // synchronized use of the MessageDigest
  private static synchronized boolean verifyPassword(AccountInfo accountInfo, String password) {
    try {
      messageDigest.update((password + SALT + accountInfo.getPrimaryName()).getBytes("UTF-8"));
      return accountInfo.getPassword().equals(encryptPassword(password, accountInfo.getPrimaryName()));
    } catch (NoSuchAlgorithmException e) {
      GNS.getLogger().warning("Problem hashing password:" + e);
      return false;
    } catch (UnsupportedEncodingException e) {
      GNS.getLogger().warning("Problem hashing password:" + e);
      return false;
    }
  }

  //Code is duplicated in all of the clients (currently java, php, iphone).
  //function encryptPassword($password, $username) {
  //	return base64_encode(hash('sha256', $password . "42shabiz" . $username, true));
  //}
  private static final String SALT = "42shabiz";

  // synchronized use of the MessageDigest
  private static synchronized String encryptPassword(String password, String alias) throws NoSuchAlgorithmException,
          UnsupportedEncodingException {
    messageDigest.update((password + SALT + alias).getBytes("UTF-8"));
    return Base64.encodeToString(messageDigest.digest(), false);
  }

  /**
   * Create a new GNS user account.
   *
   * THIS CAN BYPASS THE EMAIL VERIFICATION if you set emailVerify to false;
   *
   * <p>
   * This adds three records to the GNS for the account:<br>
   * NAME: "_GNS_GUID" -- guid<br>
   * GUID: "_GNS_ACCOUNT_INFO" -- {account record - an AccountInfo object stored as JSON}<br>
   * GUID: "_GNS_GUID_INFO" -- {guid record - a GuidInfo object stored as JSON}<br>
   *
   * @param name
   * @param guid
   * @param publicKey
   * @param password
   * @param emailVerify
   * @param verifyCode
   * @param handler
   * @return status result
   */
  public static CommandResponse<String> addAccount(String name, String guid, String publicKey, String password,
          boolean emailVerify, String verifyCode,
          ClientRequestHandlerInterface handler) {
    try {

      NSResponseCode returnCode;
      // First try to create the HRN record to make sure this name isn't already registered
      JSONObject jsonHRN = new JSONObject();
      jsonHRN.put(HRN_GUID, guid);
      if (!(returnCode = handler.getIntercessor().sendFullAddRecord(name, jsonHRN)).isAnError()) {
        // if that's cool then add the entry that links the GUID to the username and public key
        // this one could fail if someone uses the same public key to register another one... that's a nono
        AccountInfo accountInfo = new AccountInfo(name, guid, password);
        accountInfo.noteUpdate();
        // if email verifications are off we just set it to verified
        if (!emailVerify) {
          accountInfo.setVerified(true);
        } else {
          accountInfo.setVerificationCode(verifyCode);
        }
        JSONObject json = new JSONObject();
        json.put(ACCOUNT_INFO, accountInfo.toJSONObject());
        GuidInfo guidInfo = new GuidInfo(name, guid, publicKey);
        json.put(GUID_INFO, guidInfo.toJSONObject());
        // set up ACL to look like this
        //"_GNS_ACL": {
        //  "READ_WHITELIST": {"+ALL+": {"MD": "+ALL+"]}}}
        JSONObject acl = createACL(ALL_FIELDS, Arrays.asList(EVERYONE), null, null);
        // prefix is the same for all acls so just pick one to use here
        json.put(MetaDataTypeName.READ_WHITELIST.getPrefix(), acl);
        // For active code
        //json.put(ActiveCode.ON_READ, new JSONArray());
        //json.put(ActiveCode.ON_WRITE, new JSONArray());
        // set up the default read access
        if (!(returnCode = handler.getIntercessor().sendFullAddRecord(guid, json)).isAnError()) {
          return new CommandResponse<String>(OK_RESPONSE);
        } else {
          // delete the record we added above
          // might be nice to have a notion of a transaction that we could roll back
          handler.getIntercessor().sendRemoveRecord(name);
          return new CommandResponse<String>(BAD_RESPONSE + " " + returnCode.getProtocolCode() + " " + guid);
        }

//        if (!(returnCode = handler.getIntercessor().sendAddRecordWithSingleField(guid, ACCOUNT_INFO, accountInfo.toDBFormat())).isAnError()) {
//          GuidInfo guidInfo = new GuidInfo(name, guid, publicKey);
//          handler.getIntercessor().sendUpdateRecordBypassingAuthentication(guid, GUID_INFO, guidInfo.toDBFormat(),
//                  null, UpdateOperation.SINGLE_FIELD_CREATE);
//          return new CommandResponse<String>(OK_RESPONSE);
//        } else {
//          // delete the record we added above
//          // might be nice to have a notion of a transaction that we could roll back
//          handler.getIntercessor().sendRemoveRecord(name);
//          return new CommandResponse<String>(BAD_RESPONSE + " " + returnCode.getProtocolCode() + " " + guid);
//        }
      } else {
        return new CommandResponse<String>(BAD_RESPONSE + " " + returnCode.getProtocolCode() + " " + name);
      }
    } catch (JSONException e) {
      return new CommandResponse<String>(BAD_RESPONSE + " " + JSON_PARSE_ERROR + " " + e.getMessage());
    }
  }

  /**
   * Removes a GNS user account.
   *
   * @param accountInfo
   * @param handler
   * @return status result
   */
  public static CommandResponse<String> removeAccount(AccountInfo accountInfo, ClientRequestHandlerInterface handler) {
    // First remove any group links
    GroupAccess.cleanupGroupsForDelete(accountInfo.getPrimaryGuid(), handler);
    // Then remove the HRN link
    if (!handler.getIntercessor().sendRemoveRecord(accountInfo.getPrimaryName()).isAnError()) {
      handler.getIntercessor().sendRemoveRecord(accountInfo.getPrimaryGuid());
      // remove all the alias reverse links
      for (String alias : accountInfo.getAliases()) {
        handler.getIntercessor().sendRemoveRecord(alias);
      }
      // getArray rid of all subguids
      for (String guid : accountInfo.getGuids()) {
        GuidInfo guidInfo = lookupGuidInfo(guid, handler);
        if (guidInfo != null) { // should not be null, ignore if it is
          removeGuid(guidInfo, accountInfo, true, handler);
        }
      }

      // all is well
      return new CommandResponse<String>(OK_RESPONSE);
    } else {
      return new CommandResponse<String>(BAD_RESPONSE + " " + BAD_ACCOUNT);
    }
  }

  /**
   * Adds a new GUID associated with an existing account.
   * <p>
   * These records will be created:<br>
   * GUID: "_GNS_PRIMARY_GUID" -- GUID (primary) for secondary guid<br>
   * GUID: "_GNS_GUID_INFO" -- {guid info}<br>
   * HRN: "_GNS_GUID" -- GUID<br>
   *
   * @param accountInfo - the accountInfo of the account to add the GUID to
   * @param accountGuidInfo
   * @param name = the human readable name to associate with the GUID
   * @param guid - the new GUID
   * @param publicKey - the public key to use with the new account
   * @param handler
   * @return status result
   */
  public static CommandResponse<String> addGuid(AccountInfo accountInfo, GuidInfo accountGuidInfo, String name,
          String guid, String publicKey, ClientRequestHandlerInterface handler) {
    try {
      // insure that the guid doesn't exist already
      if (lookupGuidInfo(guid, handler) != null) {
        return new CommandResponse<String>(BAD_RESPONSE + " " + DUPLICATE_GUID + " " + guid);
      }
      accountInfo.addGuid(guid);
      accountInfo.noteUpdate();

      NSResponseCode returnCode;
      // First try to create the HRN to insure that that name does not already exist
      JSONObject jsonHRN = new JSONObject();
      jsonHRN.put(HRN_GUID, guid);
      if (!(returnCode = handler.getIntercessor().sendFullAddRecord(name, jsonHRN)).isAnError()) {
        // now we update the account info
        if (updateAccountInfoNoAuthentication(accountInfo, handler, true)) {
          GuidInfo guidInfo = new GuidInfo(name, guid, publicKey);
          JSONObject json = new JSONObject();
          json.put(GUID_INFO, guidInfo.toJSONObject());
          json.put(PRIMARY_GUID, accountInfo.getPrimaryGuid());
          // set up ACL to look like this
          //"_GNS_ACL": {
          //  "READ_WHITELIST": {"+ALL+": {"MD": [<publickey>, "+ALL+"]}},
          //  "WRITE_WHITELIST": {"+ALL+": {"MD": [<publickey>]}}
          JSONObject acl = createACL(ALL_FIELDS, Arrays.asList(EVERYONE, accountGuidInfo.getPublicKey()),
                  ALL_FIELDS, Arrays.asList(accountGuidInfo.getPublicKey()));
          // prefix is the same for all acls so just pick one to use here
          json.put(MetaDataTypeName.READ_WHITELIST.getPrefix(), acl); 
          // For active code
          //json.put(ActiveCode.ON_READ, new JSONArray());
          //json.put(ActiveCode.ON_WRITE, new JSONArray());
          handler.getIntercessor().sendFullAddRecord(guid, json);
          return new CommandResponse<String>(OK_RESPONSE);
        }
      }
      // otherwise roll it back
      //accountInfo.removeGuid(guid);
      return new CommandResponse<String>(BAD_RESPONSE + " " + returnCode.getProtocolCode() + " " + name);
    } catch (JSONException e) {
      return new CommandResponse<String>(BAD_RESPONSE + " " + JSON_PARSE_ERROR + " " + e.getMessage());
    } catch (GnsRuntimeException e) {
      return new CommandResponse<String>(BAD_RESPONSE + " " + GENERIC_ERROR + " " + e.getMessage());
    }
  }

  /**
   * Add multiple guids to an account.
   * 
   * @param names - the list of names
   * @param publicKeys - the list of public keys associated with the names
   * @param accountInfo
   * @param accountGuidInfo
   * @param handler
   * @return 
   */
  public static CommandResponse<String> addMultipleGuids(List<String> names,
          List<String> publicKeys,
          AccountInfo accountInfo, GuidInfo accountGuidInfo,
          ClientRequestHandlerInterface handler) {
    try {
      Set<String> guids = new HashSet<>();
      Map<String, JSONObject> hrnMap = new HashMap<>();
      Map<String, JSONObject> guidInfoMap = new HashMap<>();
      for (int i = 0; i < names.size(); i++) {
        String name = names.get(i);
        String publicKey = publicKeys.get(i);
        String guid = ClientUtils.createGuidStringFromPublicKey(publicKey.getBytes());
        accountInfo.addGuid(guid);
        guids.add(guid);
        // HRN records
        JSONObject jsonHRN = new JSONObject();
        jsonHRN.put(HRN_GUID, guid);
        hrnMap.put(name, jsonHRN);
        // guid info record 
        GuidInfo guidInfo = new GuidInfo(name, guid, publicKey);
        JSONObject jsonGuid = new JSONObject();
        jsonGuid.put(GUID_INFO, guidInfo.toJSONObject());
        jsonGuid.put(PRIMARY_GUID, accountInfo.getPrimaryGuid());
        // set up ACL to look like this
        //"_GNS_ACL": {
        //  "READ_WHITELIST": {"+ALL+": {"MD": [<publickey>, "+ALL+"]}},
        //  "WRITE_WHITELIST": {"+ALL+": {"MD": [<publickey>]}}
        JSONObject acl = createACL(ALL_FIELDS, Arrays.asList(EVERYONE, accountGuidInfo.getPublicKey()),
                ALL_FIELDS, Arrays.asList(accountGuidInfo.getPublicKey()));
        jsonGuid.put("_GNS_ACL", acl);
        jsonGuid.put("environment", 8675309); // test value
        guidInfoMap.put(guid, jsonGuid);
      }
      accountInfo.noteUpdate();

      // first we create the HRN records as a batch
      NSResponseCode returnCode;
      // First try to create the HRNS to insure that that name does not already exist
      if (!(returnCode = handler.getIntercessor().sendAddBatchRecord(new HashSet<String>(names), hrnMap)).isAnError()) {
        // now we update the account info
        if (updateAccountInfoNoAuthentication(accountInfo, handler, true)) {
          handler.getIntercessor().sendAddBatchRecord(guids, guidInfoMap);
          return new CommandResponse<String>(OK_RESPONSE);
        }
      }
      return new CommandResponse<String>(BAD_RESPONSE + " " + returnCode.getProtocolCode() + " " + names);
    } catch (JSONException e) {
      return new CommandResponse<String>(BAD_RESPONSE + " " + JSON_PARSE_ERROR + " " + e.getMessage());
    } catch (GnsRuntimeException e) {
      return new CommandResponse<String>(BAD_RESPONSE + " " + GENERIC_ERROR + " " + e.getMessage());
    }
  }

  /**
   * Used by the batch test methods to create multiple guids.
   * This creates bunch of randomly names guids.
   * 
   * @param accountInfo
   * @param accountGuidInfo
   * @param count
   * @param handler 
   */
  public static void testBatchCreateGuids(AccountInfo accountInfo, GuidInfo accountGuidInfo,
          int count, ClientRequestHandlerInterface handler) {
    List<String> names = new ArrayList<>();
    List<String> publicKeys = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      String name = "N" + RandomString.randomString(10);
      names.add(name);
      String publicKey = "P" + name;
      publicKeys.add(publicKey);
    }
    addMultipleGuids(names, publicKeys, accountInfo, accountGuidInfo, handler);
  }

  /**
   * Remove a GUID. Guid should not be an account GUID.
   *
   * @param guid
   * @param handler
   * @return the command response
   */
  public static CommandResponse<String> removeGuid(GuidInfo guid, ClientRequestHandlerInterface handler) {
    return removeGuid(guid, null, false, handler);
  }

  /**
   * Remove a GUID associated with an account.
   *
   * @param accountInfo
   * @param guid
   * @param handler
   * @return status result
   */
  public static CommandResponse<String> removeGuid(GuidInfo guid, AccountInfo accountInfo, ClientRequestHandlerInterface handler) {
    return removeGuid(guid, accountInfo, false, handler);
  }

  /**
   * Remove a GUID associated with an account.
   * If ignoreAccountGuid is true we're deleting the account guid as well
   * so we don't have to check or update that info. The accountInfo parameter
   * can be null in which case we look it up using the guid.
   *
   * @param guid
   * @param accountInfo - can be null in which case we look it up
   * @param ignoreAccountGuid
   * @param handler
   * @return the command response
   */
  public static CommandResponse<String> removeGuid(GuidInfo guid, AccountInfo accountInfo, boolean ignoreAccountGuid,
          ClientRequestHandlerInterface handler) {
    // First make sure guid is not an account GUID (unless we're sure it's not because we're deleting an account guid)
    if (!ignoreAccountGuid) {
      if (lookupAccountInfoFromGuid(guid.getGuid(), handler) != null) {
        return new CommandResponse<String>(BAD_RESPONSE + " " + BAD_GUID + " " + guid.getGuid() + " is an account guid");
      }
    }
    // Fill in a missing account info
    if (accountInfo == null) {
      String accountGuid = AccountAccess.lookupPrimaryGuid(guid.getGuid(), handler);
      // should not happen unless records got messed up in GNS
      if (accountGuid == null) {
        return new CommandResponse<String>(BAD_RESPONSE + " " + BAD_ACCOUNT + " " + guid.getGuid() + " does not have a primary account guid");
      }
      if ((accountInfo = lookupAccountInfoFromGuid(accountGuid, handler)) == null) {
        return new CommandResponse<String>(BAD_RESPONSE + " " + BAD_ACCOUNT + " " + guid.getGuid() + " cannot find primary account guid for " + accountGuid);
      }
    }
    // First remove any group links
    GroupAccess.cleanupGroupsForDelete(guid.getGuid(), handler);
    // Then remove the guid record
    if (!handler.getIntercessor().sendRemoveRecord(guid.getGuid()).isAnError()) {
      // remove reverse record
      handler.getIntercessor().sendRemoveRecord(guid.getName());
      // Possibly update the account guid we are associated with to
      // tell them we are gone
      if (ignoreAccountGuid) {
        return new CommandResponse<String>(OK_RESPONSE);
      } else {
        // update the account guid to know that we deleted the guid
        accountInfo.removeGuid(guid.getGuid());
        accountInfo.noteUpdate();
        if (updateAccountInfoNoAuthentication(accountInfo, handler, true)) {
          return new CommandResponse<String>(OK_RESPONSE);
        } else {
          return new CommandResponse<String>(BAD_RESPONSE + " " + UPDATE_ERROR);
        }
      }
    } else {
      return new CommandResponse<String>(BAD_RESPONSE + " " + BAD_GUID);
    }
  }

  /**
   * Add a new human readable name (alias) to an account.
   * <p>
   * These records will be added:<br>
   * HRN: "_GNS_GUID" -- GUID<br>
   *
   * @param accountInfo
   * @param alias
   * @param writer
   * @param signature
   * @param message
   * @param handler
   * @return status result
   */
  public static CommandResponse<String> addAlias(AccountInfo accountInfo, String alias, String writer,
          String signature, String message, ClientRequestHandlerInterface handler) {
    // insure that that name does not already exist
    try {
      NSResponseCode returnCode;
      JSONObject jsonHRN = new JSONObject();
      jsonHRN.put(HRN_GUID, accountInfo.getPrimaryGuid());
      if ((returnCode = handler.getIntercessor().sendFullAddRecord(alias, jsonHRN)).isAnError()) {
//    if ((returnCode = handler.getIntercessor().sendAddRecordWithSingleField(alias, HRN_GUID,
//            new ResultValue(Arrays.asList(accountInfo.getPrimaryGuid())))).isAnError()) {
        // roll this back
        accountInfo.removeAlias(alias);
        return new CommandResponse<String>(BAD_RESPONSE + " " + returnCode.getProtocolCode() + " " + alias);
      }
      accountInfo.addAlias(alias);
      accountInfo.noteUpdate();
      if (updateAccountInfo(accountInfo.getPrimaryGuid(), accountInfo,
              writer, signature, message, handler, true).isAnError()) {
        // back out if we got an error
        handler.getIntercessor().sendRemoveRecord(alias);
        //accountInfo.removeAlias(alias);
        return new CommandResponse<String>(BAD_RESPONSE + " " + BAD_ALIAS);
      } else {
        return new CommandResponse<String>(OK_RESPONSE);
      }
    } catch (JSONException e) {
      return new CommandResponse<String>(BAD_RESPONSE + " " + JSON_PARSE_ERROR + " " + e.getMessage());
    }
  }

  /**
   * Remove an alias from an account.
   *
   * @param accountInfo
   * @param alias
   * @param writer
   * @param signature
   * @param message
   * @param handler
   * @return status result
   */
  public static CommandResponse<String> removeAlias(AccountInfo accountInfo, String alias, String writer, String signature, String message,
          ClientRequestHandlerInterface handler) {

    GNS.getLogger().info("ALIAS: " + alias + " ALIASES:" + accountInfo.getAliases());
    if (!accountInfo.containsAlias(alias)) {
      return new CommandResponse<String>(BAD_RESPONSE + " " + BAD_ALIAS);
    }
    // remove the NAME -- GUID record
    NSResponseCode responseCode;
    if ((responseCode = handler.getIntercessor().sendRemoveRecord(alias)).isAnError()) {
      return new CommandResponse<String>(BAD_RESPONSE + " " + responseCode.getProtocolCode());
    }
    // Now updated the account record
    accountInfo.removeAlias(alias);
    accountInfo.noteUpdate();
    if ((responseCode = updateAccountInfo(accountInfo.getPrimaryGuid(), accountInfo,
            writer, signature, message, handler, true)).isAnError()) {
      return new CommandResponse<String>(BAD_RESPONSE + " " + responseCode.getProtocolCode());
    }
    return new CommandResponse<String>(OK_RESPONSE);
  }

  /**
   * Set the password of an account.
   *
   * @param accountInfo
   * @param password
   * @param writer
   * @param signature
   * @param message
   * @param handler
   * @return status result
   */
  public static CommandResponse<String> setPassword(AccountInfo accountInfo, String password, String writer, String signature,
          String message, ClientRequestHandlerInterface handler) {
    accountInfo.setPassword(password);
    accountInfo.noteUpdate();
    if (updateAccountInfo(accountInfo.getPrimaryGuid(), accountInfo, writer, signature, message, handler, false).isAnError()) {
      return new CommandResponse<String>(BAD_RESPONSE + " " + UPDATE_ERROR);
    }
    return new CommandResponse<String>(OK_RESPONSE);
  }

  /**
   * Add a tag to a GUID.
   *
   * @param guidInfo
   * @param tag
   * @param writer
   * @param signature
   * @param message
   * @param handler
   * @return status result
   */
  public static CommandResponse<String> addTag(GuidInfo guidInfo, String tag, String writer, String signature, String message,
          ClientRequestHandlerInterface handler) {
    guidInfo.addTag(tag);
    guidInfo.noteUpdate();
    if (updateGuidInfo(guidInfo, writer, signature, message, handler).isAnError()) {
      return new CommandResponse<String>(BAD_RESPONSE + " " + UPDATE_ERROR);
    }
    return new CommandResponse<String>(OK_RESPONSE);
  }

  /**
   * Remove a tag from a GUID.
   *
   * @param guidInfo
   * @param tag
   * @param writer
   * @param signature
   * @param message
   * @param handler
   * @return status result
   */
  public static CommandResponse<String> removeTag(GuidInfo guidInfo, String tag, String writer, String signature, String message,
          ClientRequestHandlerInterface handler) {
    guidInfo.removeTag(tag);
    guidInfo.noteUpdate();
    if (updateGuidInfo(guidInfo, writer, signature, message, handler).isAnError()) {
      return new CommandResponse<String>(BAD_RESPONSE + " " + UPDATE_ERROR);
    }
    return new CommandResponse<String>(OK_RESPONSE);
  }

  private static NSResponseCode updateAccountInfo(String guid, AccountInfo accountInfo, String writer, String signature, String message,
          ClientRequestHandlerInterface handler, boolean sendToReplica) {
    try {
      JSONObject json = new JSONObject();
      json.put(ACCOUNT_INFO, accountInfo.toJSONObject());
      if (sendToReplica) {
        handler.setReallySendUpdateToReplica(true);
      }
      NSResponseCode response = handler.getIntercessor().sendUpdateUserJSON(guid,
              new ValuesMap(json), UpdateOperation.USER_JSON_REPLACE,
              writer, signature, message, sendToReplica);
      if (sendToReplica) {
        handler.setReallySendUpdateToReplica(false);
      }
      return response;
    } catch (JSONException e) {
      GNS.getLogger().severe("Problem parsing account info:" + e);
      return NSResponseCode.ERROR;
    }
  }

  private static boolean updateAccountInfoNoAuthentication(AccountInfo accountInfo,
          ClientRequestHandlerInterface handler, boolean sendToReplica) {
    //try {
    return !updateAccountInfo(accountInfo.getPrimaryGuid(), accountInfo,
            null, null, null, handler, sendToReplica).isAnError();
  }

  private static NSResponseCode updateGuidInfo(GuidInfo guidInfo, String writer, String signature, String message,
          ClientRequestHandlerInterface handler) {

    try {
      JSONObject json = new JSONObject();
      json.put(GUID_INFO, guidInfo.toJSONObject());
      NSResponseCode response = handler.getIntercessor().sendUpdateUserJSON(guidInfo.getGuid(),
              new ValuesMap(json), UpdateOperation.USER_JSON_REPLACE,
              writer, signature, message);
//      NSResponseCode response = handler.getIntercessor().sendUpdateRecord(guidInfo.getGuid(), GUID_INFO,
//              guidInfo.toDBFormat(), null, -1, UpdateOperation.SINGLE_FIELD_REPLACE_ALL, writer, signature, message);
      return response;
    } catch (JSONException e) {
      GNS.getLogger().severe("Problem parsing guid info:" + e);
      return NSResponseCode.ERROR;
    }
  }

  private static boolean updateGuidInfoNoAuthentication(GuidInfo guidInfo,
          ClientRequestHandlerInterface handler) {

    return !updateGuidInfo(guidInfo, null, null, null, handler).isAnError();
//    try {
//      JSONObject json = new JSONObject();
//      json.put(GUID_INFO, guidInfo.toJSONObject());
//      NSResponseCode response = handler.getIntercessor().sendUpdateUserJSON(guidInfo.getGuid(),
//              new ValuesMap(json), UpdateOperation.USER_JSON_REPLACE, 
//              null, null, null);
//      if (!response.isAnError()) {
////      ResultValue newvalue;
////      newvalue = guidInfo.toDBFormat();
////      if (!handler.getIntercessor().sendUpdateRecordBypassingAuthentication(guidInfo.getGuid(), GUID_INFO,
////              newvalue, null, UpdateOperation.SINGLE_FIELD_REPLACE_ALL).isAnError()) {
//        return true;
//      }
//    } catch (JSONException e) {
//      GNS.getLogger().warning("Problem parsing guid info:" + e);
//    }
//    return false;
  }

  /**
   * Returns an ACL set up to look like the JSON Object below.
   *
   * "_GNS_ACL": {
   * "READ_WHITELIST": {|readfield|: {"MD": [readAcessor1, readAcessor2,... ]}},
   * "WRITE_WHITELIST": {|writefield|: {"MD": [writeAcessor1, writeAcessor2,... ]}}
   *
   * @param readField
   * @param readAcessors
   * @param writeField
   * @param writeAcessors
   * @return a JSONObject
   * @throws JSONException
   */
  private static JSONObject createACL(String readField, List<String> readAcessors,
          String writeField, List<String> writeAcessors) throws JSONException {
    JSONObject result = new JSONObject();

    if (readField != null && readAcessors != null) {
      JSONArray readlist = new JSONArray(readAcessors);
      JSONObject mdReadList = new JSONObject();
      mdReadList.put("MD", readlist);
      JSONObject readWhiteList = new JSONObject();
      readWhiteList.put(readField, mdReadList);
      result.put("READ_WHITELIST", readWhiteList);
    }

    if (writeField != null && writeAcessors != null) {
      JSONArray writelist = new JSONArray(writeAcessors);
      JSONObject mdWriteList = new JSONObject();
      mdWriteList.put("MD", writelist);
      JSONObject writeWhiteList = new JSONObject();
      writeWhiteList.put(writeField, mdWriteList);
      result.put("WRITE_WHITELIST", writeWhiteList);
    }
    return result;
  }
}
