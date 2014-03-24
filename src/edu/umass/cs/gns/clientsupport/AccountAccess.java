/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.clientsupport;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.ResultValue;
import edu.umass.cs.gns.packet.NSResponseCode;
import edu.umass.cs.gns.util.Email;
import edu.umass.cs.gns.util.Util;
import java.text.ParseException;
import java.util.Arrays;
import org.json.JSONException;

/**
 * Provides the basic interface to GNS accounts. 
 * <p>
 * See {@link AccountInfo} for more details about accounts.
 * <p>
 * Some of the internal records used to maintain account information are as follows:
 * <p>
 * GUID: "ACCOUNT_INFO" -> {account} for primary guid<br>
 * GUID: "GUID" -> GUID (primary) for secondary guid<br>
 * GUID: "GUID_INFO" -> {guid info}<br>
 * HRN:  "GUID" -> GUID<br>
 * <p>
 * GUID = Globally Unique Identifier<br>
 * HRN = Human Readable Name<br>
 *
 * @author westy
 */
public class AccountAccess {
  
  public static final String ACCOUNT_INFO = InternalField.makeInternalFieldString("account_info");
  public static final String GUID = InternalField.makeInternalFieldString("guid");
  public static final String PRIMARY_GUID = InternalField.makeInternalFieldString("primary_guid");
  public static final String GUID_INFO = InternalField.makeInternalFieldString("guid_info");

  /**
   * Obtains the account info record for the given GUID if that GUID
   * was used to create an account or is one of the GUIDs associated with
   * account.
   * <p>
   * GUID: "ACCOUNT_INFO" -> {account} for primary guid<br>
   * GUID: "GUID" -> GUID (primary) for secondary guid<br>
   * GUID: "GUID_INFO" -> {guid info}<br>
   * HRN:  "GUID" -> GUID<br>
   * <p>
   * GUID = Globally Unique Identifier<br>
   * HRN = Human Readable Name<br>
   * 
   * @param guid
   * @return an {@link AccountInfo} instance
   */
  public static AccountInfo lookupAccountInfoFromGuid(String guid) {
    
    QueryResult accountResult = Intercessor.sendQueryBypassingAuthentication(guid, ACCOUNT_INFO);
    if (accountResult.isError()) {
      guid = lookupPrimaryGuid(guid);
      if (guid != null) {
        accountResult = Intercessor.sendQueryBypassingAuthentication(guid, ACCOUNT_INFO);
      }
    }
    if (!accountResult.isError()) {
      try {
        return new AccountInfo(accountResult.get(ACCOUNT_INFO).toResultValueString());
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
   * @return a GUID
   */
  public static String lookupPrimaryGuid(String guid) {
    
    QueryResult guidResult = Intercessor.sendQueryBypassingAuthentication(guid, PRIMARY_GUID);
    if (!guidResult.isError()) {
      return (String) guidResult.get(PRIMARY_GUID).get(0);
    } else {
      return null;
    }
  }

  /**
   * Returns the GUID associated with name which is a HRN or null if one of that name does not exist.
   * <p>
   * GUID = Globally Unique Identifier<br>
   * HRN = Human Readable Name<br>
   * 
   * @param name
   * @return a GUID
   */
  public static String lookupGuid(String name) {
    
    QueryResult guidResult = Intercessor.sendQueryBypassingAuthentication(name, GUID);
    if (!guidResult.isError()) {
      return (String) guidResult.get(GUID).get(0);
    } else {
      return null;
    }
  }

  /**
   * Obtains the guid info record from the database for GUID given.
   * <p>
   * GUID = Globally Unique Identifier<br>
   * 
   * @param guid
   * @return an {@link GuidInfo} instance
   */
  public static GuidInfo lookupGuidInfo(String guid) {
    
    QueryResult guidResult = Intercessor.sendQueryBypassingAuthentication(guid, GUID_INFO);
    if (!guidResult.isError()) {
      try {
        return new GuidInfo(guidResult.get(GUID_INFO).toResultValueString());
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
   * @return an {@link AccountInfo} instance
   */
  public static AccountInfo lookupAccountInfoFromName(String name) {
    String guid = lookupGuid(name);
    if (guid != null) {
      return lookupAccountInfoFromGuid(guid);
    }
    return null;
  }
  private static final String VERIFY_COMMAND = "account_verify";
  private static final String EMAIL_BODY = "This is an automated message informing you that an account has been created for %s on the GNS server.\n"
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
  
  public static String addAccountWithVerification(String host, String name, String guid, String publicKey, String password) {
    String response;
    if ((response = addAccount(name, guid, publicKey, password, GNS.enableEmailAccountAuthentication)).equals(Defs.OKRESPONSE)) {
      if (GNS.enableEmailAccountAuthentication) {
        String verifyCode = Util.randomString(6);
        AccountInfo accountInfo = lookupAccountInfoFromGuid(guid);
        accountInfo.setVerificationCode(verifyCode);
        accountInfo.noteUpdate();
        if (updateAccountInfo(accountInfo)) {
          boolean emailOK = Email.email("GNS Account Verification", name,
                  String.format(EMAIL_BODY, name, host, guid, verifyCode, name, verifyCode));
          boolean adminEmailOK = Email.email("GNS Account Notification",
                  Email.ACCOUNT_CONTACT_EMAIL,
                  String.format(ADMIN_NOTICE, name, host, guid));
          if (emailOK) {
            return Defs.OKRESPONSE;
          } else {
            // if we can't send the confirmation back out of the account creation
            removeAccount(accountInfo);
            return Defs.BADRESPONSE + " " + Defs.VERIFICATIONERROR + " " + "Unable to send email";
          }
        } else {
          // if we do this we're probably housed anyway, but just in case try to remove the account
          removeAccount(accountInfo);
          return Defs.BADRESPONSE + " " + Defs.VERIFICATIONERROR + " " + "Unable to update account info";
        }
      }
    }
    return response;
  }
  
  public static String verifyAccount(String guid, String code) {
    AccountInfo accountInfo;
    if ((accountInfo = lookupAccountInfoFromGuid(guid)) != null) {
      if (!accountInfo.isVerified()) {
        if (accountInfo.getVerificationCode() != null && code != null) {
          if (accountInfo.getVerificationCode().equals(code)) {
            accountInfo.setVerificationCode(null);
            accountInfo.setVerified(true);
            accountInfo.noteUpdate();
            if (updateAccountInfo(accountInfo)) {
              return Defs.OKRESPONSE + " " + "Your account has been verified."; // add a little something for the kids
            } else {
              return Defs.BADRESPONSE + " " + Defs.VERIFICATIONERROR + " " + "Unable to update account info";
            }
          } else {
            return Defs.BADRESPONSE + " " + Defs.VERIFICATIONERROR + " " + "Code not correct";
          }
        } else {
          return Defs.BADRESPONSE + " " + Defs.VERIFICATIONERROR + " " + "Bad verification code";
        }
      } else {
        return Defs.BADRESPONSE + " " + Defs.VERIFICATIONERROR + " " + "Account already verified";
      }
    } else {
      return Defs.BADRESPONSE + " " + Defs.VERIFICATIONERROR + " " + "Unable to read account info";
    }
  }

  /**
   * Create a new GNS user account.
   * 
   * THIS CAN BYPASS THE EMAIL VERIFICATION if you set emailVerify to false;
   * 
   * <p>
   * This adds three records to the GNS for the account:<br>
   * NAME: "_GNS_GUID" -> guid<br>
   * GUID: "_GNS_ACCOUNT_INFO" -> {account record - an AccountInfo object stored as JSON}<br>
   * GUID: "_GNS_GUID_INFO" -> {guid record - a GuidInfo object stored as JSON}<br>
   *
   * @param name
   * @param guid
   * @param publicKey
   * @param password
   * @return status result
   */
  public static String addAccount(String name, String guid, String publicKey, String password, boolean emailVerify) {
    try {

      // First try to create the HRN record to make sure this name isn't already registered
      if (!Intercessor.sendAddRecord(name, GUID, new ResultValue(Arrays.asList(guid))).isAnError()) {
        // if that's cool then add the entry that links the GUID to the username and public key
        // this one could fail if someone uses the same public key to register another one... that's a nono
        AccountInfo accountInfo = new AccountInfo(name, guid, password);
        // if email verifications are off we just set it to verified
        if (!emailVerify) {
          accountInfo.setVerified(true);
        }
        if (!Intercessor.sendAddRecord(guid, ACCOUNT_INFO, accountInfo.toDBFormat()).isAnError()) {
          GuidInfo guidInfo = new GuidInfo(name, guid, publicKey);
          Intercessor.sendUpdateRecordBypassingAuthentication(guid, GUID_INFO, guidInfo.toDBFormat(), null, UpdateOperation.CREATE);
          return Defs.OKRESPONSE;
        } else {
          // delete the record we added above
          // might be nice to have a notion of a transaction that we could roll back
          Intercessor.sendRemoveRecord(name);
          return Defs.BADRESPONSE + " " + Defs.DUPLICATEGUID;
        }
      } else {
        return Defs.BADRESPONSE + " " + Defs.DUPLICATENAME;
      }
    } catch (JSONException e) {
      return Defs.BADRESPONSE + " " + Defs.JSONPARSEERROR;
    }
  }

  /**
   * Removes a GNS user account.
   * 
   * @param accountInfo
   * @return status result 
   */
  public static String removeAccount(AccountInfo accountInfo) {

    // do this first add to make sure this account exists
    if (!Intercessor.sendRemoveRecord(accountInfo.getPrimaryName()).isAnError()) {
      Intercessor.sendRemoveRecord(accountInfo.getPrimaryGuid());
      // remove all the alias reverse links
      for (String alias : accountInfo.getAliases()) {
        Intercessor.sendRemoveRecord(alias);
      }
      for (String guid : accountInfo.getGuids()) {
        Intercessor.sendRemoveRecord(guid);
      }
      return Defs.OKRESPONSE;
    } else {
      return Defs.BADRESPONSE + " " + Defs.BADACCOUNT;
    }
  }

  /**
   * Adds a new GUID associated with an existing account.
   * <p>
   * These records will be created:<br>
   * GUID: "_GNS_PRIMARY_GUID" -> GUID (primary) for secondary guid<br>
   * GUID: "_GNS_GUID_INFO" -> {guid info}<br>
   * HRN: "_GNS_GUID" -> GUID<br>
   * 
   * @param accountInfo - the accountInfo of the account to add the GUID to
   * @param name = the human readable name to associate with the GUID
   * @param guid - the new GUID
   * @param publicKey - the public key to use with the new account
   * @return status result 
   */
  public static String addGuid(AccountInfo accountInfo, String name, String guid, String publicKey) {
    try {
      // insure that the guis doesn't exist already
      if (lookupGuidInfo(guid) != null) {
        return Defs.BADRESPONSE + " " + Defs.DUPLICATEGUID;
      }
      // do this first so if there is an execption we don't have to back out of anything
      ResultValue guidInfoFormatted = new GuidInfo(name, guid, publicKey).toDBFormat();
      
      accountInfo.addGuid(guid);
      accountInfo.noteUpdate();

      // First try to create the HRN to insure that that name does not already exist
      if (!Intercessor.sendAddRecord(name, GUID, new ResultValue(Arrays.asList(guid))).isAnError()) {
        // update the account info
        if (updateAccountInfo(accountInfo)) {
          // add the GUID_INFO link
          Intercessor.sendAddRecord(guid, GUID_INFO, guidInfoFormatted);
          // add a link the new GUID to primary GUID
          Intercessor.sendUpdateRecordBypassingAuthentication(guid, PRIMARY_GUID, new ResultValue(Arrays.asList(accountInfo.getPrimaryGuid())),
                  null, UpdateOperation.CREATE);
          return Defs.OKRESPONSE;
        }
      }
      // otherwise roll it back
      accountInfo.removeGuid(guid);
      return Defs.BADRESPONSE + " " + Defs.DUPLICATENAME;
    } catch (JSONException e) {
      return Defs.BADRESPONSE + " " + Defs.JSONPARSEERROR;
    }
  }

  /**
   * Remove a GUID associated with an account.
   * 
   * @param accountInfo
   * @param guid
   * @return status result
   */
  public static String removeGuid(AccountInfo accountInfo, GuidInfo guid) {
    
    if (!Intercessor.sendRemoveRecord(guid.getGuid()).isAnError()) {
      // remove reverse record
      Intercessor.sendRemoveRecord(guid.getName());
      accountInfo.removeGuid(guid.getGuid());
      accountInfo.noteUpdate();
      if (updateAccountInfo(accountInfo)) {
        return Defs.OKRESPONSE;
      } else {
        return Defs.BADRESPONSE + " " + Defs.UPDATEERROR;
      }
    } else {
      return Defs.BADRESPONSE + " " + Defs.BADGUID;
    }
  }

  /**
   * Add a new human readable name (alias) to an account.
   * <p>
   * These records will be added:<br>
   * HRN: "_GNS_GUID" -> GUID<br>
   * 
   * @param accountInfo
   * @param alias
   * @return status result 
   */
  public static String addAlias(AccountInfo accountInfo, String alias) {
    accountInfo.addAlias(alias);
    accountInfo.noteUpdate();

    // insure that that name does not already exist
    if (!Intercessor.sendAddRecord(alias, GUID, new ResultValue(Arrays.asList(accountInfo.getPrimaryGuid()))).isAnError()) {
      if (updateAccountInfo(accountInfo)) {
        return Defs.OKRESPONSE;
      } else {
        Intercessor.sendRemoveRecord(alias);
        accountInfo.removeAlias(alias);
        return Defs.BADRESPONSE + " " + Defs.BADALIAS;
      }
    }
    // roll this back
    accountInfo.removeAlias(alias);
    return Defs.BADRESPONSE + " " + Defs.DUPLICATENAME;
  }

  /**
   * Remove an alias from an account.
   * 
   * @param accountInfo
   * @param alias
   * @return status result 
   */
  public static String removeAlias(AccountInfo accountInfo, String alias) {
    
    if (accountInfo.containsAlias(alias)) {
      // remove the NAME -> GUID record
      NSResponseCode responseCode;
      if ((responseCode = Intercessor.sendRemoveRecord(alias)).isAnError()) {
        return Defs.BADRESPONSE + " " + responseCode.getProtocolCode();
      }
      accountInfo.removeAlias(alias);
      accountInfo.noteUpdate();
      if (updateAccountInfo(accountInfo)) {
        return Defs.OKRESPONSE;
      }
    }
    return Defs.BADRESPONSE + " " + Defs.BADALIAS;
  }

  /**
   * Set the password of an account.
   * 
   * @param accountInfo
   * @param password
   * @return status result 
   */
  public static String setPassword(AccountInfo accountInfo, String password) {
    accountInfo.setPassword(password);
    accountInfo.noteUpdate();
    if (updateAccountInfo(accountInfo)) {
      return Defs.OKRESPONSE;
    }
    return Defs.BADRESPONSE + " " + Defs.UPDATEERROR;
  }

  /**
   * Add a tag to a GUID.
   * 
   * @param guidInfo
   * @param tag
   * @return status result 
   */
  public static String addTag(GuidInfo guidInfo, String tag) {
    guidInfo.addTag(tag);
    guidInfo.noteUpdate();
    if (updateGuidInfo(guidInfo)) {
      return Defs.OKRESPONSE;
    }
    guidInfo.removeTag(tag);
    return Defs.BADRESPONSE + " " + Defs.UPDATEERROR;
  }

  /**
   * Remove a tag from a GUID.
   * 
   * @param guidInfo
   * @param tag
   * @return status result 
   */
  public static String removeTag(GuidInfo guidInfo, String tag) {
    guidInfo.removeTag(tag);
    guidInfo.noteUpdate();
    if (updateGuidInfo(guidInfo)) {
      return Defs.OKRESPONSE;
    }
    return Defs.BADRESPONSE + " " + Defs.UPDATEERROR;
  }
  
  private static boolean updateAccountInfo(AccountInfo accountInfo) {
    
    try {
      ResultValue newvalue;
      newvalue = accountInfo.toDBFormat();
      if (!Intercessor.sendUpdateRecordBypassingAuthentication(accountInfo.getPrimaryGuid(), ACCOUNT_INFO,
              newvalue, null, UpdateOperation.REPLACE_ALL).isAnError()) {
        return true;
      }
    } catch (JSONException e) {
      GNS.getLogger().warning("Problem parsing account info:" + e);
    }
    return false;
  }
  
  private static boolean updateGuidInfo(GuidInfo guidInfo) {
    
    try {
      ResultValue newvalue;
      newvalue = guidInfo.toDBFormat();
      if (!Intercessor.sendUpdateRecordBypassingAuthentication(guidInfo.getGuid(), GUID_INFO,
              newvalue, null, UpdateOperation.REPLACE_ALL).isAnError()) {
        return true;
      }
    } catch (JSONException e) {
      GNS.getLogger().warning("Problem parsing guid info:" + e);
    }
    return false;
  }
  public static String Version = "$Revision$";
}
