/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.client;

import edu.umass.cs.gns.httpserver.Protocol;
import edu.umass.cs.gns.main.GNS;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import org.json.JSONException;

/**
 * Provides the basic interface to GNS accounts. 
 * <p>
 * See {@link AccountInfo} for more details about accounts.
 *
 * @author westy
 */
public class AccountAccess {

  public AccountAccess() {
  }

  // make it a singleton class
  public static AccountAccess getInstance() {
    return AccountAccessHolder.INSTANCE;
  }

  private static class AccountAccessHolder {

    private static final AccountAccess INSTANCE = new AccountAccess();
  }
  public static final String ACCOUNT_INFO = GNS.INTERNAL_PREFIX + "account_info";
  public static final String GUID = GNS.INTERNAL_PREFIX + "guid";
  public static final String PRIMARY_GUID = GNS.INTERNAL_PREFIX + "primary_guid";
  public static final String GUID_INFO = GNS.INTERNAL_PREFIX + "guid_info";

  /**
   * Obtains the account info record for the given GUID if that GUID
   * was used to create an account or is one of the GUIDs associated with
   * account.
   * <p>
   * Some of the internal records used to maintain account information are as follows:
   * <p>
   * GUID: "_GNS_ACCOUNT_INFO" -> {account} for primary guid<br>
   * GUID: "_GNS_GUID" -> GUID (primary) for secondary guid<br>
   * GUID: "_GNS_GUID_INFO" -> {guid info}<br>
   * HRN: "_GNS_GUID" -> GUID<br>
   * <p>
   * GUID = Globally Unique Identifier<br>
   * HRN = Human Readable Name<br>
   * 
   * @param guid
   * @return an {@link AccountInfo} instance
   */
  public AccountInfo lookupAccountInfoFromGuid(String guid) {
    Intercessor client = Intercessor.getInstance();
    ArrayList<String> accountResult = client.sendQuery(guid, ACCOUNT_INFO);
    if (accountResult == null) {
      guid = lookupPrimaryGuid(guid);
    }
    accountResult = client.sendQuery(guid, ACCOUNT_INFO);
    if (accountResult != null) {
      try {
        return new AccountInfo(accountResult);
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
  public String lookupPrimaryGuid(String guid) {
    Intercessor client = Intercessor.getInstance();
    ArrayList<String> guidResult = client.sendQuery(guid, PRIMARY_GUID);
    if (guidResult != null) {
      return guidResult.get(0);
    } else {
      return null;
    }
  }

  /**
   * Returns the GUID associated with name which is a HRN.
   * <p>
   * GUID = Globally Unique Identifier<br>
   * HRN = Human Readable Name<br>
   * 
   * @param name
   * @return a GUID
   */
  public String lookupGuid(String name) {
    Intercessor client = Intercessor.getInstance();
    ArrayList<String> guidResult = client.sendQuery(name, GUID);
    if (guidResult != null) {
      return guidResult.get(0);
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
  public GuidInfo lookupGuidInfo(String guid) {
    Intercessor client = Intercessor.getInstance();
    ArrayList<String> guidResult = client.sendQuery(guid, GUID_INFO);
    if (guidResult != null) {
      try {
        return new GuidInfo(guidResult);
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
  public AccountInfo lookupAccountInfoFromName(String name) {
    String guid = lookupGuid(name);
    if (guid != null) {
      return lookupAccountInfoFromGuid(guid);
    }
    return null;
  }

  /**
   * Create a new GNS user account.
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
  public String addAccount(String name, String guid, String publicKey, String password) {
    try {
      Intercessor client = Intercessor.getInstance();
      // do this first add to make sure this name isn't already registered
      if (client.sendAddRecordWithConfirmation(name, GUID, new ArrayList<String>(Arrays.asList(guid)))) {
        // if that's cool then add the entry that links the GUID to the username and public key
        // this one could fail if someone uses the same public key to register another one... that's a nono
        AccountInfo accountInfo = new AccountInfo(name, guid, password);
        if (client.sendAddRecordWithConfirmation(guid, ACCOUNT_INFO, accountInfo.toDBFormat())) {
          GuidInfo guidInfo = new GuidInfo(name, guid, publicKey);
          client.sendUpdateRecordWithConfirmation(guid, GUID_INFO, guidInfo.toDBFormat(), null, UpdateOperation.CREATE);
          return Protocol.OKRESPONSE;
        } else {
          // delete the record we added above
          // might be nice to have a notion of a transaction that we could roll back
          client.sendRemoveRecordWithConfirmation(name);
          return Protocol.BADRESPONSE + " " + Protocol.DUPLICATEGUID;
        }
      } else {
        return Protocol.BADRESPONSE + " " + Protocol.DUPLICATENAME;
      }
    } catch (JSONException e) {
      return Protocol.BADRESPONSE + " " + Protocol.JSONPARSEERROR;
    }
  }

  /**
   * Removes a GNS user account.
   * 
   * @param accountInfo
   * @return status result 
   */
  public String removeAccount(AccountInfo accountInfo) {
    Intercessor client = Intercessor.getInstance();
    // do this first add to make sure this account exists
    if (client.sendRemoveRecordWithConfirmation(accountInfo.getPrimaryName())) {
      client.sendRemoveRecordWithConfirmation(accountInfo.getPrimaryGuid());
      // remove all the alias reverse links
      for (String alias : accountInfo.getAliases()) {
        client.sendRemoveRecordWithConfirmation(alias);
      }
      for (String guid : accountInfo.getGuids()) {
        client.sendRemoveRecordWithConfirmation(guid);
      }
      return Protocol.OKRESPONSE;
    } else {
      return Protocol.BADRESPONSE + " " + Protocol.BADACCOUNT;
    }
  }

  /**
   * Adds a new GUID associated with and existing account.
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
  public String addGuid(AccountInfo accountInfo, String name, String guid, String publicKey) {
    try {
      // insure that the guis doesn't exist already
      if (lookupGuidInfo(guid) != null) {
        return Protocol.BADRESPONSE + " " + Protocol.DUPLICATEGUID;
      }
      // do this first so if there is an execption we don't have to back out of anything
      ArrayList<String> guidInfoFormatted = new GuidInfo(name, guid, publicKey).toDBFormat();

      accountInfo.addGuid(guid);
      accountInfo.noteUpdate();
      Intercessor client = Intercessor.getInstance();
      // insure that that name does not already exist
      if (client.sendAddRecordWithConfirmation(name, GUID, new ArrayList<String>(Arrays.asList(guid)))) {
        // update the account info
        if (updateAccountInfo(accountInfo)) {
          // add the GUID_INFO link
          client.sendAddRecordWithConfirmation(guid, GUID_INFO, guidInfoFormatted);
          // add a link the new GUID to primary GUID
          client.sendUpdateRecordWithConfirmation(guid, PRIMARY_GUID, new ArrayList<String>(Arrays.asList(accountInfo.getPrimaryGuid())), null, UpdateOperation.CREATE);
          return Protocol.OKRESPONSE;
        }
      }
      // otherwise roll it back
      accountInfo.removeGuid(guid);
      return Protocol.BADRESPONSE + " " + Protocol.DUPLICATENAME;
    } catch (JSONException e) {
      return Protocol.BADRESPONSE + " " + Protocol.JSONPARSEERROR;
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
  public String addAlias(AccountInfo accountInfo, String alias) {
    accountInfo.addAlias(alias);
    accountInfo.noteUpdate();
    Intercessor client = Intercessor.getInstance();
    // insure that that name does not already exist
    if (client.sendAddRecordWithConfirmation(alias, GUID, new ArrayList<String>(Arrays.asList(accountInfo.getPrimaryGuid())))) {
      if (updateAccountInfo(accountInfo)) {
        return Protocol.OKRESPONSE;
      } else {
        client.sendRemoveRecordWithConfirmation(alias);
        accountInfo.removeAlias(alias);
        return Protocol.BADRESPONSE + " " + Protocol.BADALIAS;
      }
    }
    // roll this back
    accountInfo.removeAlias(alias);
    return Protocol.BADRESPONSE + " " + Protocol.DUPLICATENAME;
  }

  /**
   * Remove an alias from an account.
   * 
   * @param accountInfo
   * @param alias
   * @return status result 
   */
  public String removeAlias(AccountInfo accountInfo, String alias) {
    Intercessor client = Intercessor.getInstance();
    if (accountInfo.containsAlias(alias)) {
      // remove the NAME -> GUID record
      client.sendRemoveRecordWithConfirmation(alias);
      accountInfo.removeAlias(alias);
      accountInfo.noteUpdate();
      if (updateAccountInfo(accountInfo)) {
        return Protocol.OKRESPONSE;
      }
    }
    return Protocol.BADRESPONSE + " " + Protocol.BADALIAS;
  }

  /**
   * Set the password of an account.
   * 
   * @param accountInfo
   * @param password
   * @return status result 
   */
  public String setPassword(AccountInfo accountInfo, String password) {
    accountInfo.setPassword(password);
    accountInfo.noteUpdate();
    Intercessor client = Intercessor.getInstance();
    if (updateAccountInfo(accountInfo)) {
      return Protocol.OKRESPONSE;
    }
    return Protocol.BADRESPONSE + " " + Protocol.UPDATEERROR;
  }

  /**
   * Add a tag to a GUID.
   * 
   * @param guidInfo
   * @param tag
   * @return status result 
   */
  public String addTag(GuidInfo guidInfo, String tag) {
    guidInfo.addTag(tag);
    guidInfo.noteUpdate();
    Intercessor client = Intercessor.getInstance();
    if (updateGuidInfo(guidInfo)) {
      return Protocol.OKRESPONSE;
    }
    guidInfo.removeTag(tag);
    return Protocol.BADRESPONSE + " " + Protocol.UPDATEERROR;
  }

  /**
   * Remove a tag from a GUID.
   * 
   * @param guidInfo
   * @param tag
   * @return status result 
   */
  public String removeTag(GuidInfo guidInfo, String tag) {
    guidInfo.removeTag(tag);
    guidInfo.noteUpdate();
    Intercessor client = Intercessor.getInstance();
    if (updateGuidInfo(guidInfo)) {
      return Protocol.OKRESPONSE;
    }
    return Protocol.BADRESPONSE + " " + Protocol.UPDATEERROR;
  }

  private boolean updateAccountInfo(AccountInfo accountInfo) {
    Intercessor client = Intercessor.getInstance();
    try {
      ArrayList<String> newvalue;
      newvalue = accountInfo.toDBFormat();
      if (client.sendUpdateRecordWithConfirmation(accountInfo.getPrimaryGuid(), ACCOUNT_INFO,
              newvalue, null, UpdateOperation.REPLACE_ALL)) {
        return true;
      }
    } catch (JSONException e) {
    }
    return false;
  }

  private boolean updateGuidInfo(GuidInfo guidInfo) {
    Intercessor client = Intercessor.getInstance();
    try {
      ArrayList<String> newvalue;
      newvalue = guidInfo.toDBFormat();
      if (client.sendUpdateRecordWithConfirmation(guidInfo.getGuid(), GUID_INFO,
              newvalue, null, UpdateOperation.REPLACE_ALL)) {
        return true;
      }
    } catch (JSONException e) {
    }
    return false;
  }
  public static String Version = "$Revision$";
}
