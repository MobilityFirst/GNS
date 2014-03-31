/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.nameserver.clientsupport;

import edu.umass.cs.gns.clientsupport.AccountAccess;
import edu.umass.cs.gns.clientsupport.AccountInfo;
import edu.umass.cs.gns.clientsupport.GuidInfo;
import edu.umass.cs.gns.clientsupport.QueryResult;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.recordmap.NameRecord;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.util.ResultValue;
import org.json.JSONException;

import java.text.ParseException;

/**
 * @deprecated
 */
public class NSAccountAccess {

  public static AccountInfo lookupAccountInfoFromGuid(String guid) {
    ResultValue accountResult = null;
    try {
      accountResult = NameRecord.getNameRecordMultiField(NameServer.getRecordMap(), guid, null, AccountAccess.ACCOUNT_INFO).getKey(AccountAccess.ACCOUNT_INFO);
    } catch (FieldNotFoundException e) {
    } catch (RecordNotFoundException e) {
    }
    if (accountResult == null) {
      try {
        guid = lookupPrimaryGuid(guid);
        if (guid != null) {
          accountResult = NameRecord.getNameRecordMultiField(NameServer.getRecordMap(), guid, null, AccountAccess.ACCOUNT_INFO).getKey(AccountAccess.ACCOUNT_INFO);
        }
      } catch (FieldNotFoundException e) {
      } catch (RecordNotFoundException e) {
      }
    }
    if (accountResult != null) {
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
   * If GUID is associated with another account, returns the GUID of that account,
   * otherwise returns null.
   * <p>
   * GUID = Globally Unique Identifier
   * 
   * @param guid
   * @return a GUID
   */
  public static String lookupPrimaryGuid(String guid) {
    ResultValue guidResult = null;
    try {
      guidResult = NameRecord.getNameRecordMultiField(NameServer.getRecordMap(), guid, null, AccountAccess.PRIMARY_GUID).getKey(AccountAccess.PRIMARY_GUID);
    } catch (FieldNotFoundException e) {
    } catch (RecordNotFoundException e) {
    }
    if (guidResult != null) {
      return (String) guidResult.get(0);
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
    ResultValue guidResult = null;
    try {
      guidResult = NameRecord.getNameRecordMultiField(NameServer.getRecordMap(), name, null, AccountAccess.GUID).getKey(AccountAccess.GUID);
    } catch (FieldNotFoundException e) {
    } catch (RecordNotFoundException e) {
    }
    if (guidResult != null) {
      return (String) guidResult.get(0);
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
    return lookupGuidInfo(guid, false);
  }

  /**
   * Obtains the guid info record from the database for GUID given.
   * If allowSiteToSiteQuery is true and the record is not available locally 
   * a query will be sent another name server to find the record.
   * 
   * @param guid
   * @param allowSiteToSiteQuery
   * @return 
   */
  public static GuidInfo lookupGuidInfo(String guid, boolean allowSiteToSiteQuery) {
    ResultValue guidResult = null;
    try {
      guidResult = NameRecord.getNameRecordMultiField(NameServer.getRecordMap(), guid, null, AccountAccess.GUID_INFO).getKey(AccountAccess.GUID_INFO);
    } catch (FieldNotFoundException e) {
    } catch (RecordNotFoundException e) {
    }
    // If we're allowed we go looking at other NameServers for the record in question.
    if (guidResult == null && allowSiteToSiteQuery) {
      guidResult = lookupGuidInfoSiteToSite(guid, AccountAccess.GUID_INFO);
    }
    if (guidResult != null) {
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

  public static ResultValue lookupGuidInfoSiteToSite(String guid, String key) {
    QueryResult queryResult = SiteToSiteQueryHandler.sendQuery(guid, key);
    if (!queryResult.isError()) {
      return queryResult.get(AccountAccess.GUID_INFO);
    } else {
      return null;
    }

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
}
