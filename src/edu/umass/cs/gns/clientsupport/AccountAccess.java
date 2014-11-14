/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.clientsupport;

import edu.umass.cs.gns.util.Base64;
import static edu.umass.cs.gns.clientsupport.Defs.*;
import edu.umass.cs.gns.exceptions.GnsRuntimeException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.util.ByteUtils;
import edu.umass.cs.gns.util.Email;
import edu.umass.cs.gns.util.NSResponseCode;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.ThreadUtils;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.json.JSONException;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

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
 * HRN: "GUID" -> GUID<br>
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

  /**
   * Obtains the account info record for the given GUID if that GUID
   * was used to create an account.
   * <p>
   * GUID: "ACCOUNT_INFO" -> {account} for primary guid<br>
   * GUID: "PRIMARY_GUID" -> GUID (primary) for secondary guid<br>
   * GUID: "GUID_INFO" -> {guid info}<br>
   * HRN: "GUID" -> GUID<br>
   * <p>
   * GUID = Globally Unique Identifier<br>
   * HRN = Human Readable Name<br>
   *
   * @param guid
   * @return
   */
  public static AccountInfo lookupAccountInfoFromGuid(String guid) {
    return lookupAccountInfoFromGuid(guid, false);
  }

  /**
   * Obtains the account info record for the given GUID if that GUID
   * was used to create an account. If allowSubGuids is true will also work
   * for GUIDs associated with an account.
   * <p>
   * GUID: "ACCOUNT_INFO" -> {account} for primary guid<br>
   * GUID: "GUID" -> GUID (primary) for secondary guid<br>
   * GUID: "GUID_INFO" -> {guid info}<br>
   * HRN: "GUID" -> GUID<br>
   * <p>
   * GUID = Globally Unique Identifier<br>
   * HRN = Human Readable Name<br>
   *
   * @param guid
   * @param allowSubGuids
   * @return
   */
  public static AccountInfo lookupAccountInfoFromGuid(String guid, boolean allowSubGuids) {
    QueryResult accountResult = Intercessor.sendQueryBypassingAuthentication(guid, ACCOUNT_INFO);
    if (Intercessor.debuggingEnabled) {
      GNS.getLogger().fine("###QUERY RESULT:" + accountResult);
    }
    if (accountResult.isError()) {
      if (allowSubGuids) {
        // if allowSubGuids is true assume this is a guid that is "owned" by an account guid so
        // we look  up the owning account guid
        guid = lookupPrimaryGuid(guid);
        if (guid != null) {
          accountResult = Intercessor.sendQueryBypassingAuthentication(guid, ACCOUNT_INFO);
        }
      }
    }
    if (!accountResult.isError()) {
      try {
        return new AccountInfo(accountResult.getArray(ACCOUNT_INFO).toResultValueString());
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
      return (String) guidResult.getArray(PRIMARY_GUID).get(0);
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

    QueryResult guidResult = Intercessor.sendQueryBypassingAuthentication(name, HRN_GUID);
    if (!guidResult.isError()) {
      return (String) guidResult.getArray(HRN_GUID).get(0);
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
        return new GuidInfo(guidResult.getArray(GUID_INFO).toResultValueString());
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
   * @return
   */
  public static CommandResponse addAccountWithVerification(String host, String name, String guid, String publicKey, String password) {
    CommandResponse response;
    if ((response = addAccount(name, guid, publicKey, password, GNS.enableEmailAccountAuthentication)).getReturnValue().equals(OKRESPONSE)) {
      if (GNS.enableEmailAccountAuthentication) {
        String verifyCode = createVerificationCode(name);
        AccountInfo accountInfo = lookupAccountInfoFromGuid(guid);
        if (accountInfo == null) {
          return new CommandResponse(BADRESPONSE + " " + BADACCOUNT + " " + guid);
        }
        accountInfo.setVerificationCode(verifyCode);
        accountInfo.noteUpdate();
        if (updateAccountInfoNoAuthentication(accountInfo)) {
          boolean emailOK = Email.email("GNS Account Verification", name,
                  String.format(EMAIL_BODY, name, verifyCode, host, guid, verifyCode, name, verifyCode));
          boolean adminEmailOK = Email.email("GNS Account Notification",
                  Email.ACCOUNT_CONTACT_EMAIL,
                  String.format(ADMIN_NOTICE, name, host, guid));
          if (emailOK) {
            return new CommandResponse(OKRESPONSE);
          } else {
            // if we can't send the confirmation back out of the account creation
            removeAccount(accountInfo);
            return new CommandResponse(BADRESPONSE + " " + VERIFICATIONERROR + " " + "Unable to send email");
          }
        } else {
          // Account info could not be updated.
          // If we're here we're probably hosed anyway, but just in case try to remove the account
          removeAccount(accountInfo);
          return new CommandResponse(BADRESPONSE + " " + Defs.VERIFICATIONERROR + " " + "Unable to update account info");
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
   * @return
   */
  public static CommandResponse verifyAccount(String guid, String code) {
    AccountInfo accountInfo;
    if ((accountInfo = lookupAccountInfoFromGuid(guid)) == null) {
      return new CommandResponse(Defs.BADRESPONSE + " " + Defs.VERIFICATIONERROR + " " + "Unable to read account info");
    }
    if (accountInfo.isVerified()) {
      return new CommandResponse(Defs.BADRESPONSE + " " + Defs.VERIFICATIONERROR + " " + "Account already verified");
    }
    if (accountInfo.getVerificationCode() == null && code == null) {
      return new CommandResponse(Defs.BADRESPONSE + " " + Defs.VERIFICATIONERROR + " " + "Bad verification code");
    }
    if ((new Date()).getTime() - accountInfo.getCreated().getTime() > TWO_HOURS_IN_MILLESECONDS) {
      return new CommandResponse(Defs.BADRESPONSE + " " + Defs.VERIFICATIONERROR + " " + "Account code no longer valid");
    }
    if (!accountInfo.getVerificationCode().equals(code)) {
      return new CommandResponse(Defs.BADRESPONSE + " " + Defs.VERIFICATIONERROR + " " + "Code not correct");
    }
    accountInfo.setVerificationCode(null);
    accountInfo.setVerified(true);
    accountInfo.noteUpdate();
    if (updateAccountInfoNoAuthentication(accountInfo)) {
      return new CommandResponse(Defs.OKRESPONSE + " " + "Your account has been verified."); // add a little something for the kids
    } else {
      return new CommandResponse(Defs.BADRESPONSE + " " + Defs.VERIFICATIONERROR + " " + "Unable to update account info");
    }
  }

  public static CommandResponse resetPublicKey(String guid, String password, String publicKey) {
    AccountInfo accountInfo;
    if ((accountInfo = lookupAccountInfoFromGuid(guid)) == null) {
      return new CommandResponse(Defs.BADRESPONSE + " " + Defs.VERIFICATIONERROR + " " + "Not an account guid");
    }
    if (verifyPassword(accountInfo, password)) {
      GuidInfo guidInfo;
      if ((guidInfo = lookupGuidInfo(guid)) == null) {
        return new CommandResponse(Defs.BADRESPONSE + " " + Defs.VERIFICATIONERROR + " " + "Unable to read guid info");
      } else {
        guidInfo.setPublicKey(publicKey);
        guidInfo.noteUpdate();
        if (updateGuidInfoNoAuthentication(guidInfo)) {
          return new CommandResponse(Defs.OKRESPONSE + " " + "Public key has been updated.");
        } else {
          return new CommandResponse(Defs.BADRESPONSE + " " + Defs.VERIFICATIONERROR + " " + "Unable to update guid info");
        }
      }
    } else {
      return new CommandResponse(Defs.BADRESPONSE + " " + Defs.VERIFICATIONERROR + " " + "Password mismatch");
    }
  }

  private static boolean verifyPassword(AccountInfo accountInfo, String password) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      md.update((password + SALT + accountInfo.getPrimaryName()).getBytes("UTF-8"));
      return accountInfo.getPassword().equals(encryptPassword(password, accountInfo.getPrimaryName()));
    } catch (NoSuchAlgorithmException e) {
      GNS.getLogger().warning("Problem hashing password:" + e);
      return false;
    } catch (UnsupportedEncodingException e) {
      GNS.getLogger().warning("Problem hashing password:" + e);
      return false;
    }
  }

  //Code is duplicated in the client and in the php code:
  //function encryptPassword($password, $username) {
  //	return base64_encode(hash('sha256', $password . "42shabiz" . $username, true));
  //}
  private static final String SALT = "42shabiz";

  private static String encryptPassword(String password, String alias) throws NoSuchAlgorithmException, 
          UnsupportedEncodingException {
    MessageDigest md = MessageDigest.getInstance("SHA-256");
    md.update((password + SALT + alias).getBytes("UTF-8"));
    return Base64.encodeToString(md.digest(), false);
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
   * @param emailVerify
   * @return status result
   */
  public static CommandResponse addAccount(String name, String guid, String publicKey, String password, boolean emailVerify) {
    try {

      // First try to create the HRN record to make sure this name isn't already registered
      if (!Intercessor.sendAddRecord(name, HRN_GUID, new ResultValue(Arrays.asList(guid))).isAnError()) {
        // if that's cool then add the entry that links the GUID to the username and public key
        // this one could fail if someone uses the same public key to register another one... that's a nono
        AccountInfo accountInfo = new AccountInfo(name, guid, password);
        // if email verifications are off we just set it to verified
        if (!emailVerify) {
          accountInfo.setVerified(true);
        }
        if (!Intercessor.sendAddRecord(guid, ACCOUNT_INFO, accountInfo.toDBFormat()).isAnError()) {
          GuidInfo guidInfo = new GuidInfo(name, guid, publicKey);
          Intercessor.sendUpdateRecordBypassingAuthentication(guid, GUID_INFO, guidInfo.toDBFormat(), null, UpdateOperation.SINGLE_FIELD_CREATE);
          return new CommandResponse(OKRESPONSE);
        } else {
          // delete the record we added above
          // might be nice to have a notion of a transaction that we could roll back
          Intercessor.sendRemoveRecord(name);
          return new CommandResponse(BADRESPONSE + " " + DUPLICATEGUID + " " + guid);
        }
      } else {
        return new CommandResponse(BADRESPONSE + " " + DUPLICATENAME + " " + name);
      }
    } catch (JSONException e) {
      return new CommandResponse(BADRESPONSE + " " + JSONPARSEERROR + " " + e.getMessage());
    }
  }

  /**
   * Removes a GNS user account.
   *
   * @param accountInfo
   * @return status result
   */
  public static CommandResponse removeAccount(AccountInfo accountInfo) {
    // First remove any group links
    GroupAccess.cleanupGroupsForDelete(accountInfo.getPrimaryGuid());
    // Then remove the HRN link
    if (!Intercessor.sendRemoveRecord(accountInfo.getPrimaryName()).isAnError()) {
      Intercessor.sendRemoveRecord(accountInfo.getPrimaryGuid());
      // remove all the alias reverse links
      for (String alias : accountInfo.getAliases()) {
        Intercessor.sendRemoveRecord(alias);
      }
      // getArray rid of all subguids
      for (String guid : accountInfo.getGuids()) {
        GuidInfo guidInfo = lookupGuidInfo(guid);
        if (guidInfo != null) { // should not be null, ignore if it is
          removeGuid(guidInfo, accountInfo, true);
        }
      }

      // all is well
      return new CommandResponse(OKRESPONSE);
    } else {
      return new CommandResponse(BADRESPONSE + " " + BADACCOUNT);
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
  public static CommandResponse addGuid(AccountInfo accountInfo, String name, String guid, String publicKey) {
    try {
      // insure that the guid doesn't exist already
      if (lookupGuidInfo(guid) != null) {
        return new CommandResponse(BADRESPONSE + " " + DUPLICATEGUID + " " + guid);
      }
      // do this first so if there is an execption we don't have to back out of anything
      ResultValue guidInfoFormatted = new GuidInfo(name, guid, publicKey).toDBFormat();

      accountInfo.addGuid(guid);
      accountInfo.noteUpdate();

      // First try to create the HRN to insure that that name does not already exist
      if (!Intercessor.sendAddRecord(name, HRN_GUID, new ResultValue(Arrays.asList(guid))).isAnError()) {
        // update the account info
        if (updateAccountInfoNoAuthentication(accountInfo)) {
          // add the GUID_INFO link
          Intercessor.sendAddRecord(guid, GUID_INFO, guidInfoFormatted);
          // added this step to insure that the pervious step happened.
          // probably should add a timeout here
          GuidInfo newGuidInfo = null;
          int cnt = 0;
          do {
            newGuidInfo = lookupGuidInfo(guid);
            ThreadUtils.sleep(100); // relax a bit
          } while (newGuidInfo == null && ++cnt < 10);
          if (newGuidInfo == null) {
            throw new GnsRuntimeException("Unable to locate guid info structure.");
          }
          // add a link the new GUID to primary GUID
          Intercessor.sendUpdateRecordBypassingAuthentication(guid, PRIMARY_GUID, new ResultValue(Arrays.asList(accountInfo.getPrimaryGuid())),
                  null, UpdateOperation.SINGLE_FIELD_CREATE);
          return new CommandResponse(OKRESPONSE);
        }
      }
      // otherwise roll it back
      accountInfo.removeGuid(guid);
      return new CommandResponse(BADRESPONSE + " " + DUPLICATENAME + " " + name);
    } catch (JSONException e) {
      return new CommandResponse(BADRESPONSE + " " + JSONPARSEERROR + " " + e.getMessage());
    } catch (GnsRuntimeException e) {
      return new CommandResponse(BADRESPONSE + " " + GENERICERROR + " " + e.getMessage());
    }
  }

  /**
   * Remove a GUID. Guid should not be an account GUID.
   *
   * @param guid
   * @return
   */
  public static CommandResponse removeGuid(GuidInfo guid) {
    return removeGuid(guid, null, false);
  }

  /**
   * Remove a GUID associated with an account.
   *
   * @param accountInfo
   * @param guid
   * @return status result
   */
  public static CommandResponse removeGuid(GuidInfo guid, AccountInfo accountInfo) {
    return removeGuid(guid, accountInfo, false);
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
   * @return
   */
  public static CommandResponse removeGuid(GuidInfo guid, AccountInfo accountInfo, boolean ignoreAccountGuid) {
    // First make sure guid is not an account GUID (unless we're sure it's not because we're deleting an account guid)
    if (!ignoreAccountGuid) {
      if (lookupAccountInfoFromGuid(guid.getGuid()) != null) {
        return new CommandResponse(BADRESPONSE + " " + BADGUID + " " + guid.getGuid() + " is an account guid");
      }
    }
    // Fill in a missing account info
    if (accountInfo == null) {
      String accountGuid = AccountAccess.lookupPrimaryGuid(guid.getGuid());
      // should not happen unless records got messed up in GNS
      if (accountGuid == null) {
        return new CommandResponse(BADRESPONSE + " " + BADACCOUNT + " " + guid.getGuid() + " does not have a primary account guid");
      }
      if ((accountInfo = lookupAccountInfoFromGuid(accountGuid)) == null) {
        return new CommandResponse(BADRESPONSE + " " + BADACCOUNT + " " + guid.getGuid() + " cannot find primary account guid for " + accountGuid);
      }
    }
    // First remove any group links
    GroupAccess.cleanupGroupsForDelete(guid.getGuid());
    // Then remove the guid record
    if (!Intercessor.sendRemoveRecord(guid.getGuid()).isAnError()) {
      // remove reverse record
      Intercessor.sendRemoveRecord(guid.getName());
      // Possibly update the account guid we are associated with to
      // tell them we are gone
      if (ignoreAccountGuid) {
        return new CommandResponse(OKRESPONSE);
      } else {
        // update the account guid to know that we deleted the guid
        accountInfo.removeGuid(guid.getGuid());
        accountInfo.noteUpdate();
        if (updateAccountInfoNoAuthentication(accountInfo)) {
          return new CommandResponse(OKRESPONSE);
        } else {
          return new CommandResponse(BADRESPONSE + " " + UPDATEERROR);
        }
      }
    } else {
      return new CommandResponse(BADRESPONSE + " " + BADGUID);
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
   * @param writer
   * @param signature
   * @param message
   * @return status result
   */
  public static CommandResponse addAlias(AccountInfo accountInfo, String alias, String writer, String signature, String message) {
    // insure that that name does not already exist
    if (Intercessor.sendAddRecord(alias, HRN_GUID, new ResultValue(Arrays.asList(accountInfo.getPrimaryGuid()))).isAnError()) {
      // roll this back
      accountInfo.removeAlias(alias);
      return new CommandResponse(BADRESPONSE + " " + DUPLICATENAME + " " + alias);
    }
    accountInfo.addAlias(alias);
    accountInfo.noteUpdate();
    if (updateAccountInfo(accountInfo, writer, signature, message).isAnError()) {
      // back out if we got an error
      Intercessor.sendRemoveRecord(alias);
      accountInfo.removeAlias(alias);
      return new CommandResponse(BADRESPONSE + " " + BADALIAS);
    } else {
      return new CommandResponse(OKRESPONSE);
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
   * @return status result
   */
  public static CommandResponse removeAlias(AccountInfo accountInfo, String alias, String writer, String signature, String message) {

    if (!accountInfo.containsAlias(alias)) {
      return new CommandResponse(BADRESPONSE + " " + BADALIAS);
    }
    // remove the NAME -> GUID record
    NSResponseCode responseCode;
    if ((responseCode = Intercessor.sendRemoveRecord(alias)).isAnError()) {
      return new CommandResponse(BADRESPONSE + " " + responseCode.getProtocolCode());
    }
    accountInfo.removeAlias(alias);
    accountInfo.noteUpdate();
    if ((responseCode = updateAccountInfo(accountInfo, writer, signature, message)).isAnError()) {
      return new CommandResponse(BADRESPONSE + " " + responseCode.getProtocolCode());
    }
    return new CommandResponse(OKRESPONSE);
  }

  /**
   * Set the password of an account.
   *
   * @param accountInfo
   * @param password
   * @param writer
   * @param signature
   * @param message
   * @return status result
   */
  public static CommandResponse setPassword(AccountInfo accountInfo, String password, String writer, String signature, String message) {
    accountInfo.setPassword(password);
    accountInfo.noteUpdate();
    if (updateAccountInfo(accountInfo, writer, signature, message).isAnError()) {
      return new CommandResponse(BADRESPONSE + " " + UPDATEERROR);
    }
    return new CommandResponse(OKRESPONSE);
  }

  /**
   * Add a tag to a GUID.
   *
   * @param guidInfo
   * @param tag
   * @param writer
   * @param signature
   * @param message
   * @return status result
   */
  public static CommandResponse addTag(GuidInfo guidInfo, String tag, String writer, String signature, String message) {
    guidInfo.addTag(tag);
    guidInfo.noteUpdate();
    if (updateGuidInfo(guidInfo, writer, signature, message).isAnError()) {
      return new CommandResponse(BADRESPONSE + " " + UPDATEERROR);
    }
    return new CommandResponse(OKRESPONSE);
  }

  /**
   * Remove a tag from a GUID.
   *
   * @param guidInfo
   * @param tag
   * @param writer
   * @param signature
   * @param message
   * @return status result
   */
  public static CommandResponse removeTag(GuidInfo guidInfo, String tag, String writer, String signature, String message) {
    guidInfo.removeTag(tag);
    guidInfo.noteUpdate();
    if (updateGuidInfo(guidInfo, writer, signature, message).isAnError()) {
      return new CommandResponse(BADRESPONSE + " " + UPDATEERROR);
    }
    return new CommandResponse(OKRESPONSE);
  }

  private static NSResponseCode updateAccountInfo(AccountInfo accountInfo, String writer, String signature, String message) {
    try {
      NSResponseCode response = Intercessor.sendUpdateRecord(accountInfo.getPrimaryGuid(), ACCOUNT_INFO,
              accountInfo.toDBFormat(), null, -1, UpdateOperation.SINGLE_FIELD_REPLACE_ALL, writer, signature, message);
      return response;
    } catch (JSONException e) {
      GNS.getLogger().severe("Problem parsing account info:" + e);
      return NSResponseCode.ERROR;
    }
  }

  private static boolean updateAccountInfoNoAuthentication(AccountInfo accountInfo) {
    try {
      ResultValue newvalue;
      newvalue = accountInfo.toDBFormat();
      if (!Intercessor.sendUpdateRecordBypassingAuthentication(accountInfo.getPrimaryGuid(), ACCOUNT_INFO,
              newvalue, null, UpdateOperation.SINGLE_FIELD_REPLACE_ALL).isAnError()) {
        return true;
      }
    } catch (JSONException e) {
      GNS.getLogger().warning("Problem parsing account info:" + e);
    }
    return false;
  }

  private static NSResponseCode updateGuidInfo(GuidInfo guidInfo, String writer, String signature, String message) {

    try {
      NSResponseCode response = Intercessor.sendUpdateRecord(guidInfo.getGuid(), GUID_INFO,
              guidInfo.toDBFormat(), null, -1, UpdateOperation.SINGLE_FIELD_REPLACE_ALL, writer, signature, message);
      return response;
    } catch (JSONException e) {
      GNS.getLogger().severe("Problem parsing guid info:" + e);
      return NSResponseCode.ERROR;
    }
  }

  private static boolean updateGuidInfoNoAuthentication(GuidInfo guidInfo) {

    try {
      ResultValue newvalue;
      newvalue = guidInfo.toDBFormat();
      if (!Intercessor.sendUpdateRecordBypassingAuthentication(guidInfo.getGuid(), GUID_INFO,
              newvalue, null, UpdateOperation.SINGLE_FIELD_REPLACE_ALL).isAnError()) {
        return true;
      }
    } catch (JSONException e) {
      GNS.getLogger().warning("Problem parsing guid info:" + e);
    }
    return false;
  }

  //
  public static String Version = "$Revision$";
}
