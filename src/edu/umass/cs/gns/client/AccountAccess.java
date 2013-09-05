package edu.umass.cs.gns.client;

import edu.umass.cs.gns.httpserver.Protocol;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.packet.UpdateOperation;
import org.json.JSONException;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;

//import edu.umass.cs.gns.packet.QueryResultValue;
//import edu.umass.cs.gns.packet.UpdateAddressPacket;

/**
 * Document once it settles down a bit.
 *
 * @author westy
 */
public class AccountAccess {

  public static String Version = "$Revision: 646 $";

  public AccountAccess() {
  }

  // make it a singleton class
  public static AccountAccess getInstance() {
    return AccountAccessHolder.INSTANCE;
  }

  private static class AccountAccessHolder {

    private static final AccountAccess INSTANCE = new AccountAccess();
  }
  public static final String ACCOUNT_INFO = Defs.INTERNAL_PREFIX + "account_info";
  public static final String GUID = Defs.INTERNAL_PREFIX + "guid";
  public static final String PRIMARY_GUID = Defs.INTERNAL_PREFIX + "primary_guid";
  public static final String GUID_INFO = Defs.INTERNAL_PREFIX + "guid_info";

  // GUID: "_GNS_ACCOUNT_INFO" -> {account} for primary guid
  // GUID: "_GNS_GUID" -> GUID (primary) for secondary guid
  // GUID: "_GNS_GUID_INFO" -> {guid info} 
  // NAME: "_GNS_GUID" -> GUID
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

  public String lookupPrimaryGuid(String guid) {
    Intercessor client = Intercessor.getInstance();
    ArrayList<String> guidResult = client.sendQuery(guid, PRIMARY_GUID);
    if (guidResult != null) {
      return guidResult.get(0);
    } else {
      return null;
    }
  }

  public String lookupGuid(String name) {
    Intercessor client = Intercessor.getInstance();
    ArrayList<String> guidResult = client.sendQuery(name, GUID);
    if (guidResult != null) {
      return guidResult.get(0);
    } else {
      return null;
    }
  }

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

  public AccountInfo lookupAccountInfoFromName(String name) {
    String guid = lookupGuid(name);
    if (guid != null) {
      return lookupAccountInfoFromGuid(guid);
    }
    return null;
  }

  /**
   * Create a new GNS user account.
   *
   * This adds three records to the GNS for the account. NAME: "_GNS_GUID" -> guid GUID: "_GNS_ACCOUNT_INFO" -> {account record - an
   * AccountInfo object stored as JSON} GUID: "_GNS_GUID_INFO" -> {guid record - a GuidInfo object stored as JSON}
   *
   * @param name
   * @param guid
   * @param publicKey
   * @param password
   * @return
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

  public String deleteAccount(AccountInfo accountInfo) {
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

  // GUID: "_GNS_PRIMARY_GUID" -> GUID (primary) for secondary guid
  // GUID: "_GNS_GUID_INFO" -> {guid info} 
  // ALIAS: "_GNS_GUID" -> GUID
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
      ArrayList<String> newAccountInfo;
      try {
        newAccountInfo = accountInfo.toDBFormat();
      } catch (JSONException e) {
        return Protocol.BADRESPONSE + " " + Protocol.JSONPARSEERROR;
      }
      Intercessor client = Intercessor.getInstance();
      // insure that that name does not already exist
      if (client.sendAddRecordWithConfirmation(name, GUID, new ArrayList<String>(Arrays.asList(guid)))) {
        // update the account info
        if (client.sendUpdateRecordWithConfirmation(accountInfo.getPrimaryGuid(), ACCOUNT_INFO,
                newAccountInfo, null, UpdateOperation.REPLACE_ALL)) {
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

  public String addAlias(AccountInfo accountInfo, String alias) {
    accountInfo.addAlias(alias);
    accountInfo.noteUpdate();
    ArrayList<String> newvalue;
    try {
      newvalue = accountInfo.toDBFormat();
    } catch (JSONException e) {
      return Protocol.BADRESPONSE + " " + Protocol.JSONPARSEERROR;
    }
    Intercessor client = Intercessor.getInstance();
    // insure that that name does not already exist
    if (client.sendAddRecordWithConfirmation(alias, GUID, new ArrayList<String>(Arrays.asList(accountInfo.getPrimaryGuid())))) {
      if (client.sendUpdateRecordWithConfirmation(accountInfo.getPrimaryGuid(), ACCOUNT_INFO,
              newvalue, null, UpdateOperation.REPLACE_ALL)) {
        return Protocol.OKRESPONSE;
      } else {
        client.sendRemoveRecordWithConfirmation(alias);
        accountInfo.removeAlias(alias);
        return Protocol.BADRESPONSE + " " + Protocol.BADGUID; // not really, but close enough
      }
    }
    // roll this back
    accountInfo.removeAlias(alias);
    return Protocol.BADRESPONSE + " " + Protocol.DUPLICATENAME;
  }

  public String setPassword(AccountInfo accountInfo, String password) {
    accountInfo.setPassword(password);
    accountInfo.noteUpdate();
    ArrayList<String> newvalue;
    try {
      newvalue = accountInfo.toDBFormat();
    } catch (JSONException e) {
      return Protocol.BADRESPONSE + " " + Protocol.JSONPARSEERROR;
    }
    Intercessor client = Intercessor.getInstance();
    if (client.sendUpdateRecordWithConfirmation(accountInfo.getPrimaryGuid(), ACCOUNT_INFO,
            newvalue, null, UpdateOperation.REPLACE_ALL)) {
      return Protocol.OKRESPONSE;
    }
    return Protocol.BADRESPONSE + " " + Protocol.GENERICEERROR;
  }

  public String addTag(GuidInfo guidInfo, String tag) {
    guidInfo.addTag(tag);
    guidInfo.noteUpdate();
    ArrayList<String> newvalue;
    try {
      newvalue = guidInfo.toDBFormat();
    } catch (JSONException e) {
      return Protocol.BADRESPONSE + " " + Protocol.JSONPARSEERROR;
    }
    Intercessor client = Intercessor.getInstance();
    if (client.sendUpdateRecordWithConfirmation(guidInfo.getGuid(), GUID_INFO,
            newvalue, null, UpdateOperation.REPLACE_ALL)) {
      return Protocol.OKRESPONSE;
    }
    guidInfo.removeTag(tag);
    return Protocol.BADRESPONSE + " " + Protocol.GENERICEERROR;
  }

  public String removeTag(GuidInfo guidInfo, String tag) {
    guidInfo.removeTag(tag);
    guidInfo.noteUpdate();
    ArrayList<String> newvalue;
    try {
      newvalue = guidInfo.toDBFormat();
    } catch (JSONException e) {
      return Protocol.BADRESPONSE + " " + Protocol.JSONPARSEERROR;
    }
    Intercessor client = Intercessor.getInstance();
    if (client.sendUpdateRecordWithConfirmation(guidInfo.getGuid(), GUID_INFO,
            newvalue, null, UpdateOperation.REPLACE_ALL)) {
      return Protocol.OKRESPONSE;
    }
    guidInfo.removeTag(tag);
    return Protocol.BADRESPONSE + " " + Protocol.GENERICEERROR;
  }
}
