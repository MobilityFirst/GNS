/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.nsdesign.clientsupport;

import edu.umass.cs.gns.clientsupport.AccountAccess;
import edu.umass.cs.gns.clientsupport.AccountInfo;
import static edu.umass.cs.gns.clientsupport.Defs.*;
import edu.umass.cs.gns.clientsupport.GuidInfo;
import edu.umass.cs.gns.clientsupport.SHA1HashFunction;
import edu.umass.cs.gns.exceptions.FailedDBOperationException;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurableInterface;
import edu.umass.cs.gns.util.ByteUtils;
import edu.umass.cs.gns.util.Email;
import edu.umass.cs.gns.util.NSResponseCode;
import edu.umass.cs.gns.clientsupport.UpdateOperation;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurable;
import java.net.InetSocketAddress;
import org.json.JSONException;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;

/**
 * Provides Name Server side support for reading and writing guid account information from the database.
 *
 * @author westy
 */
public class NSAccountAccess {

  /**
   * Gets the AccountInfo record for guid if the guid is an account guid. Only looks
   * on this name server.
   *
   * @param guid
   * @param activeReplica
   * @return
   */
  public static AccountInfo lookupAccountInfoFromGuid(String guid, GnsReconfigurableInterface activeReplica,
          InetSocketAddress lnsAddress) throws FailedDBOperationException {
    return lookupAccountInfoFromGuid(guid, false, activeReplica, lnsAddress);
  }

  /**
   * Gets the AccountInfo record for guid if the guid is an account guid.
   * If allowQueryToOtherNSs is true and the record is not available locally
   * a query will be sent another name server to find the record.
   *
   * @param guid
   * @param allowQueryToOtherNSs
   * @param activeReplica
   * @return
   */
  public static AccountInfo lookupAccountInfoFromGuid(String guid, boolean allowQueryToOtherNSs,
          GnsReconfigurableInterface activeReplica, InetSocketAddress lnsAddress) throws FailedDBOperationException {
    ResultValue accountResult = NSFieldAccess.lookupListFieldAnywhere(guid, AccountAccess.ACCOUNT_INFO, allowQueryToOtherNSs, 
            activeReplica, lnsAddress);
    if (!accountResult.isEmpty()) {
      try {
        return new AccountInfo(accountResult.toResultValueString());
      } catch (JSONException e) {
        GNS.getLogger().severe("Problem parsing accountinfo:" + e);
      } catch (ParseException e) {
        GNS.getLogger().severe("Problem parsing accountinfo:" + e);
      }
    }
    return null;
  }

  /**
   * Gets the AccountInfo record for guid if the guid is an account guid OR if the guid is a subguid gets it from
   * the parent account guid. 
   * If allowQueryToOtherNSs is true and the record is not available locally
   * a query will be sent another name server to find the record.
   *
   *
   * @param guid
   * @param activeReplica
   * @return
   */
  public static AccountInfo lookupAccountInfoFromGuidOrParent(String guid, boolean allowQueryToOtherNSs, 
          GnsReconfigurable activeReplica, InetSocketAddress lnsAddress) throws FailedDBOperationException {
    AccountInfo info = lookupAccountInfoFromGuid(guid, allowQueryToOtherNSs, activeReplica, lnsAddress);
    if (info != null) {
      return info;
    } else {
      guid = lookupPrimaryGuid(guid, activeReplica);
      if (guid != null) {
        return lookupAccountInfoFromGuid(guid, allowQueryToOtherNSs, activeReplica, lnsAddress);
      } else {
        return null;
      }
    }
  }

  /**
   * If GUID has a parent guid account, returns the GUID of that account,
   * otherwise returns null.
   * <p>
   * GUID = Globally Unique Identifier
   *
   * @param guid
   * @return a GUID
   */
  public static String lookupPrimaryGuid(String guid, GnsReconfigurableInterface activeReplica) throws FailedDBOperationException {
    return NSFieldAccess.lookupSingletonFieldOnThisServer(guid, AccountAccess.PRIMARY_GUID, activeReplica);
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
  public static String lookupGuid(String name, GnsReconfigurableInterface activeReplica) throws FailedDBOperationException {
    return NSFieldAccess.lookupSingletonFieldOnThisServer(name, AccountAccess.HRN_GUID, activeReplica);
  }


  /**
   * Obtains the guid info record from the database for GUID given.
   * <p>
   * GUID = Globally Unique Identifier<br>
   *
   * @param guid
   * @return an {@link edu.umass.cs.gns.clientsupport.GuidInfo} instance
   */
  public static GuidInfo lookupGuidInfo(String guid, GnsReconfigurableInterface activeReplica,
          InetSocketAddress lnsAddress) throws FailedDBOperationException {
    return NSAccountAccess.lookupGuidInfo(guid, false, activeReplica, lnsAddress);
  }

  /**
   * Obtains the guid info record from the database for GUID given.
   * If allowQueryToOtherNSs is true and the record is not available locally
   * a query will be sent another name server to find the record.
   *
   * @param guid
   * @param allowQueryToOtherNSs
   * @return
   */
  public static GuidInfo lookupGuidInfo(String guid, boolean allowQueryToOtherNSs, GnsReconfigurableInterface activeReplica,
          InetSocketAddress lnsAddress) throws FailedDBOperationException {
    ResultValue guidResult = NSFieldAccess.lookupListFieldAnywhere(guid, AccountAccess.GUID_INFO, allowQueryToOtherNSs, activeReplica,
            lnsAddress);
    if (!guidResult.isEmpty()) {
      try {
        return new GuidInfo(guidResult.toResultValueString());
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
   * @return an {@link edu.umass.cs.gns.clientsupport.AccountInfo} instance
   */
  public static AccountInfo lookupAccountInfoFromName(String name, GnsReconfigurableInterface activeReplica,
          InetSocketAddress lnsAddress) throws FailedDBOperationException {
    String guid = lookupGuid(name, activeReplica);
    if (guid != null) {
      return lookupAccountInfoFromGuid(guid, activeReplica, lnsAddress);
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

  /**
   * Creates a user account using email verification to insure that a valid email account is associated with the account guid.
   * 
   * @param host
   * @param name
   * @param guid
   * @param publicKey
   * @param password
   * @param activeReplica
   * @return 
   */
  public static String addAccountWithVerification(String host, String name, String guid, String publicKey, String password, 
          GnsReconfigurableInterface activeReplica, InetSocketAddress lnsAddress) throws FailedDBOperationException {
    String response;
    if ((response = addAccount(name, guid, publicKey, password, GNS.enableEmailAccountAuthentication, activeReplica,
            lnsAddress)).equals(OKRESPONSE)) {
      if (GNS.enableEmailAccountAuthentication) {
        String verifyCode = createVerificationCode(name);
        // Make sure the addAccount above actually worked.
        AccountInfo accountInfo = lookupAccountInfoFromGuid(guid, activeReplica, lnsAddress);
        if (accountInfo == null) {
          return BADRESPONSE + " " + BADACCOUNT + " " + guid;
        }
        accountInfo.setVerificationCode(verifyCode);
        accountInfo.noteUpdate();
        if (updateAccountInfo(accountInfo, activeReplica, lnsAddress)) {
          boolean emailOK = Email.email("GNS Account Verification", name,
                  String.format(EMAIL_BODY, name, host, guid, verifyCode, name, verifyCode));
          boolean adminEmailOK = Email.email("GNS Account Notification",
                  Email.ACCOUNT_CONTACT_EMAIL,
                  String.format(ADMIN_NOTICE, name, host, guid));
          if (emailOK) {
            return OKRESPONSE;
          } else {
            // if we can't send the confirmation back out of the account creation
            removeAccount(accountInfo, activeReplica, lnsAddress);
            return BADRESPONSE + " " + VERIFICATIONERROR + " " + "Unable to send email";
          }
        } else {
          // Account info could not be updated.
          // If we're here we're probably hosed anyway, but just in case try to remove the account
          removeAccount(accountInfo, activeReplica, lnsAddress);
          return BADRESPONSE + " " + VERIFICATIONERROR + " " + "Unable to update account info";
        }
      }
    }
    return response;
  }

  private static final String SECRET = "AN4pNmLGcGQGKwtaxFFOKG05yLlX0sXRye9a3awdQd2aNZ5P1ZBdpdy98Za3qcE"
          + "o0u6BXRBZBrcH8r2NSbqpOoWfvcxeSC7wSiOiVHN7fW0eFotdFz0fiKjHj3h0ri";

  private static String createVerificationCode(String name) {
    return ByteUtils.toHex(SHA1HashFunction.getInstance().hash(name + SECRET));
  }

  private static final long TWO_HOURS_IN_MILLESECONDS = 60 * 60 * 1000 * 2;

  /**
   * Compares the code with the code stored with the GNS to insure that the email user created the account guid.
   * 
   * @param guid
   * @param code
   * @param activeReplica
   * @return 
   */
  public static String verifyAccount(String guid, String code, GnsReconfigurableInterface activeReplica,
          InetSocketAddress lnsAddress) throws FailedDBOperationException {
    AccountInfo accountInfo;
    if ((accountInfo = lookupAccountInfoFromGuid(guid, activeReplica, lnsAddress)) == null) {
      return BADRESPONSE + " " + VERIFICATIONERROR + " " + "Unable to read account info";
    }
    if (accountInfo.isVerified()) {
      return BADRESPONSE + " " + VERIFICATIONERROR + " " + "Account already verified";
    }
    if (accountInfo.getVerificationCode() == null && code == null) {
      return BADRESPONSE + " " + VERIFICATIONERROR + " " + "Bad verification code";
    }
    if ((new Date()).getTime() - accountInfo.getCreated().getTime() > TWO_HOURS_IN_MILLESECONDS) {
      return BADRESPONSE + " " + VERIFICATIONERROR + " " + "Account code no longer valid";
    }
    if (!accountInfo.getVerificationCode().equals(code)) {
      return BADRESPONSE + " " + VERIFICATIONERROR + " " + "Code not correct";
    }
    accountInfo.setVerificationCode(null);
    accountInfo.setVerified(true);
    accountInfo.noteUpdate();
    if (updateAccountInfo(accountInfo, activeReplica, lnsAddress)) {
      return OKRESPONSE + " " + "Your account has been verified."; // add a little something for the kids
    } else {
      return BADRESPONSE + " " + VERIFICATIONERROR + " " + "Unable to update account info";
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
  public static String addAccount(String name, String guid, String publicKey, String password, boolean emailVerify, 
          GnsReconfigurableInterface activeReplica, InetSocketAddress lnsAddress) {
    try {

      // First try to create the HRN record to make sure this name isn't already registered
      if (!LNSUpdateHandler.sendAddRecord(name, AccountAccess.HRN_GUID, new ResultValue(Arrays.asList(guid)), 
              activeReplica, lnsAddress).isAnError()) {
        // if that's cool then add the entry that links the GUID to the username and public key
        // this one could fail if someone uses the same public key to register another one... that's a nono
        AccountInfo accountInfo = new AccountInfo(name, guid, password);
        // if email verifications are off we just set it to verified
        if (!emailVerify) {
          accountInfo.setVerified(true);
        }
        if (!LNSUpdateHandler.sendAddRecord(guid, AccountAccess.ACCOUNT_INFO, accountInfo.toDBFormat(), activeReplica,
                lnsAddress).isAnError()) {
          GuidInfo guidInfo = new GuidInfo(name, guid, publicKey);
          LNSUpdateHandler.sendUpdate(guid, AccountAccess.GUID_INFO, guidInfo.toDBFormat(), 
                  UpdateOperation.SINGLE_FIELD_CREATE, activeReplica, lnsAddress);
          return OKRESPONSE;
        } else {
          // delete the record we added above
          // might be nice to have a notion of a transaction that we could roll back
          LNSUpdateHandler.sendRemoveRecord(name, activeReplica, lnsAddress);
          return BADRESPONSE + " " + DUPLICATEGUID + " " + guid;
        }
      } else {
        return BADRESPONSE + " " + DUPLICATENAME + " " + name;
      }
    } catch (JSONException e) {
      return BADRESPONSE + " " + JSONPARSEERROR + " " + e.getMessage();
    }
  }

  /**
   * Removes a GNS user account.
   *
   * @param accountInfo
   * @return status result
   */
  public static String removeAccount(AccountInfo accountInfo, GnsReconfigurableInterface activeReplica,
           InetSocketAddress lnsAddress) throws FailedDBOperationException {
    // First remove any group links
    NSGroupAccess.cleanupGroupsForDelete(accountInfo.getPrimaryGuid(), activeReplica, lnsAddress);
    // Then remove the HRN link
    if (!LNSUpdateHandler.sendRemoveRecord(accountInfo.getPrimaryName(), activeReplica, lnsAddress).isAnError()) {
      LNSUpdateHandler.sendRemoveRecord(accountInfo.getPrimaryGuid(), activeReplica, lnsAddress);
      // remove all the alias reverse links
      for (String alias : accountInfo.getAliases()) {
        LNSUpdateHandler.sendRemoveRecord(alias, activeReplica, lnsAddress);
      }
      // get rid of all subguids
      for (String guid : accountInfo.getGuids()) {
        GuidInfo guidInfo = lookupGuidInfo(guid, activeReplica, lnsAddress);
        if (guidInfo != null) { // should not be null, ignore if it is
          removeGuid(guidInfo, accountInfo, true, activeReplica, lnsAddress);
        }
      }

      // all is well
      return OKRESPONSE;
    } else {
      return BADRESPONSE + " " + BADACCOUNT;
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
  public static String addGuid(AccountInfo accountInfo, String name, String guid, String publicKey, 
          GnsReconfigurableInterface activeReplica, InetSocketAddress lnsAddress) throws FailedDBOperationException {
    try {
      // insure that the guis doesn't exist already
      if (lookupGuidInfo(guid, activeReplica, lnsAddress) != null) {
        return BADRESPONSE + " " + DUPLICATEGUID + " " + guid;
      }
      // do this first so if there is an execption we don't have to back out of anything
      ResultValue guidInfoFormatted = new GuidInfo(name, guid, publicKey).toDBFormat();

      accountInfo.addGuid(guid);
      accountInfo.noteUpdate();

      // First try to create the HRN to insure that that name does not already exist
      if (!LNSUpdateHandler.sendAddRecord(name, AccountAccess.HRN_GUID, new ResultValue(Arrays.asList(guid)),
              activeReplica, lnsAddress).isAnError()) {
        // update the account info
        if (updateAccountInfo(accountInfo, activeReplica, lnsAddress)) {
          // add the GUID_INFO link
          LNSUpdateHandler.sendAddRecord(guid, AccountAccess.GUID_INFO, guidInfoFormatted, 
                  activeReplica, lnsAddress);
          // add a link the new GUID to primary GUID
          LNSUpdateHandler.sendUpdate(guid, AccountAccess.PRIMARY_GUID, new ResultValue(Arrays.asList(accountInfo.getPrimaryGuid())),
                  UpdateOperation.SINGLE_FIELD_CREATE, activeReplica, lnsAddress);
          return OKRESPONSE;
        }
      }
      // otherwise roll it back
      accountInfo.removeGuid(guid);
      return BADRESPONSE + " " + DUPLICATENAME + " " + name;
    } catch (JSONException e) {
      return BADRESPONSE + " " + JSONPARSEERROR + " " + e.getMessage();
    }
  }

  /**
   * Remove a GUID. Guid should not be an account GUID.
   *
   * @param guid
   * @return
   */
  public static String removeGuid(GuidInfo guid, GnsReconfigurable activeReplica,
          InetSocketAddress lnsAddress) throws FailedDBOperationException {
    return removeGuid(guid, null, false, activeReplica, lnsAddress);
  }

  /**
   * Remove a GUID associated with an account.
   *
   * @param accountInfo
   * @param guid
   * @return status result
   */
  public static String removeGuid(GuidInfo guid, AccountInfo accountInfo, 
          GnsReconfigurableInterface activeReplica, InetSocketAddress lnsAddress) throws FailedDBOperationException {
    return removeGuid(guid, accountInfo, false, activeReplica, lnsAddress);
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
  public static String removeGuid(GuidInfo guid, AccountInfo accountInfo, boolean ignoreAccountGuid, 
          GnsReconfigurableInterface activeReplica, InetSocketAddress lnsAddress) throws FailedDBOperationException {
    // First make sure guid is not an account GUID (unless we're sure it's not because we're deleting an account guid)
    if (!ignoreAccountGuid) {
      if (lookupAccountInfoFromGuid(guid.getGuid(), activeReplica, lnsAddress) != null) {
        return BADRESPONSE + " " + BADGUID + " " + guid.getGuid() + " is an account guid";
      }
    }
    // Fill in a missing account info
    if (accountInfo == null) {
      String accountGuid = lookupPrimaryGuid(guid.getGuid(), activeReplica);
      // should not happen unless records got messed up in GNS
      if (accountGuid == null) {
        return BADRESPONSE + " " + BADACCOUNT + " " + guid.getGuid() + " does not have a primary account guid";
      }
      if ((accountInfo = lookupAccountInfoFromGuid(accountGuid, activeReplica, lnsAddress)) == null) {
        return BADRESPONSE + " " + BADACCOUNT + " " + guid.getGuid() + " cannot find primary account guid for " + accountGuid;
      }
    }
    // First remove any group links
    NSGroupAccess.cleanupGroupsForDelete(guid.getGuid(), activeReplica, lnsAddress);
    // Then remove the guid record
    if (!LNSUpdateHandler.sendRemoveRecord(guid.getGuid(), activeReplica, lnsAddress).isAnError()) {
      // remove reverse record
      LNSUpdateHandler.sendRemoveRecord(guid.getName(), activeReplica, lnsAddress);
      // Possibly update the account guid we are associated with to
      // tell them we are gone
      if (ignoreAccountGuid) {
        return OKRESPONSE;
      } else {
        // update the account guid to know that we deleted the guid
        accountInfo.removeGuid(guid.getGuid());
        accountInfo.noteUpdate();
        if (updateAccountInfo(accountInfo, activeReplica, lnsAddress)) {
          return OKRESPONSE;
        } else {
          return BADRESPONSE + " " + UPDATEERROR;
        }
      }
    } else {
      return BADRESPONSE + " " + BADGUID;
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
  public static String addAlias(AccountInfo accountInfo, String alias, GnsReconfigurableInterface activeReplica,
          InetSocketAddress lnsAddress) {
    accountInfo.addAlias(alias);
    accountInfo.noteUpdate();

    // insure that that name does not already exist
    if (!LNSUpdateHandler.sendAddRecord(alias, AccountAccess.HRN_GUID, new ResultValue(Arrays.asList(accountInfo.getPrimaryGuid())),
            activeReplica, lnsAddress).isAnError()) {
      if (updateAccountInfo(accountInfo, activeReplica, lnsAddress)) {
        return OKRESPONSE;
      } else { // back out if we got an error
        LNSUpdateHandler.sendRemoveRecord(alias, activeReplica, lnsAddress);
        accountInfo.removeAlias(alias);
        return BADRESPONSE + " " + BADALIAS;
      }
    }
    // roll this back
    accountInfo.removeAlias(alias);
    return BADRESPONSE + " " + DUPLICATENAME + " " + alias;
  }

  /**
   * Remove an alias from an account.
   *
   * @param accountInfo
   * @param alias
   * @return status result
   */
  public static String removeAlias(AccountInfo accountInfo, String alias, GnsReconfigurableInterface activeReplica,
          InetSocketAddress lnsAddress) {

    if (accountInfo.containsAlias(alias)) {
      // remove the NAME -> GUID record
      NSResponseCode responseCode;
      if ((responseCode = LNSUpdateHandler.sendRemoveRecord(alias, activeReplica, lnsAddress)).isAnError()) {
        return BADRESPONSE + " " + responseCode.getProtocolCode();
      }
      accountInfo.removeAlias(alias);
      accountInfo.noteUpdate();
      if (updateAccountInfo(accountInfo, activeReplica, lnsAddress)) {
        return OKRESPONSE;
      }
    }
    return BADRESPONSE + " " + BADALIAS;
  }

  /**
   * Set the password of an account.
   *
   * @param accountInfo
   * @param password
   * @return status result
   */
  public static String setPassword(AccountInfo accountInfo, String password, GnsReconfigurableInterface activeReplica,
          InetSocketAddress lnsAddress) {
    accountInfo.setPassword(password);
    accountInfo.noteUpdate();
    if (updateAccountInfo(accountInfo, activeReplica, lnsAddress)) {
      return OKRESPONSE;
    }
    return BADRESPONSE + " " + UPDATEERROR;
  }

  /**
   * Add a tag to a GUID.
   *
   * @param guidInfo
   * @param tag
   * @return status result
   */
  public static String addTag(GuidInfo guidInfo, String tag, GnsReconfigurable activeReplica,
          InetSocketAddress lnsAddress) {
    guidInfo.addTag(tag);
    guidInfo.noteUpdate();
    if (updateGuidInfo(guidInfo, activeReplica, lnsAddress)) {
      return OKRESPONSE;
    }
    guidInfo.removeTag(tag);
    return BADRESPONSE + " " + UPDATEERROR;
  }

  /**
   * Remove a tag from a GUID.
   *
   * @param guidInfo
   * @param tag
   * @return status result
   */
  public static String removeTag(GuidInfo guidInfo, String tag, GnsReconfigurable activeReplica, 
          InetSocketAddress lnsAddress) {
    guidInfo.removeTag(tag);
    guidInfo.noteUpdate();
    if (updateGuidInfo(guidInfo, activeReplica, lnsAddress)) {
      return OKRESPONSE;
    }
    return BADRESPONSE + " " + UPDATEERROR;
  }

  private static boolean updateAccountInfo(AccountInfo accountInfo, GnsReconfigurableInterface activeReplica,
          InetSocketAddress lnsAddress) {
    try {
      if (!LNSUpdateHandler.sendUpdate(accountInfo.getPrimaryGuid(), AccountAccess.ACCOUNT_INFO,
              accountInfo.toDBFormat(), UpdateOperation.SINGLE_FIELD_REPLACE_ALL, activeReplica,
              lnsAddress).isAnError()) {
        return true;
      }
    } catch (JSONException e) {
      GNS.getLogger().warning("Problem parsing account info:" + e);
    }
    return false;
  }

  private static boolean updateGuidInfo(GuidInfo guidInfo, GnsReconfigurable activeReplica,
          InetSocketAddress lnsAddress) {
    try {
      if (!LNSUpdateHandler.sendUpdate(guidInfo.getGuid(), AccountAccess.GUID_INFO,
              guidInfo.toDBFormat(), UpdateOperation.SINGLE_FIELD_REPLACE_ALL, activeReplica,
              lnsAddress).isAnError()) {
        return true;
      }
    } catch (JSONException e) {
      GNS.getLogger().warning("Problem parsing guid info:" + e);
    }
    return false;
  }
}
