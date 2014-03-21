/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.nsdesign.clientsupport;

import edu.umass.cs.gns.clientsupport.AccountAccess;
import edu.umass.cs.gns.clientsupport.AccountInfo;
import edu.umass.cs.gns.clientsupport.GuidInfo;
import edu.umass.cs.gns.clientsupport.QueryResult;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.NameRecord;
import edu.umass.cs.gns.nameserver.ResultValue;
import edu.umass.cs.gns.nsdesign.activeReplica.ActiveReplica;
import org.json.JSONException;

import java.text.ParseException;

public class NSAccountAccess {

  public static AccountInfo lookupAccountInfoFromGuid(String guid, ActiveReplica activeReplica) {
    ResultValue accountResult = null;
    try {
      accountResult = NameRecord.getNameRecordMultiField(activeReplica.getNameRecordDB(), guid, null, AccountAccess.ACCOUNT_INFO).getKey(AccountAccess.ACCOUNT_INFO);
    } catch (FieldNotFoundException e) {
    } catch (RecordNotFoundException e) {
    }
    if (accountResult == null) {
      try {
        guid = lookupPrimaryGuid(guid, activeReplica);
        if (guid != null) {
          accountResult = NameRecord.getNameRecordMultiField(activeReplica.getNameRecordDB(), guid, null, AccountAccess.ACCOUNT_INFO).getKey(AccountAccess.ACCOUNT_INFO);
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
  public static String lookupPrimaryGuid(String guid, ActiveReplica activeReplica) {
    ResultValue guidResult = null;
    try {
      guidResult = NameRecord.getNameRecordMultiField(activeReplica.getNameRecordDB(), guid, null, AccountAccess.PRIMARY_GUID).getKey(AccountAccess.PRIMARY_GUID);
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
  public static String lookupGuid(String name, ActiveReplica activeReplica) {
    ResultValue guidResult = null;
    try {
      guidResult = NameRecord.getNameRecordMultiField(activeReplica.getNameRecordDB(), name, null, AccountAccess.GUID).getKey(AccountAccess.GUID);
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
   * @return an {@link edu.umass.cs.gns.clientsupport.GuidInfo} instance
   */
  public static GuidInfo lookupGuidInfo(String guid, ActiveReplica activeReplica) {
    return lookupGuidInfo(guid, false, activeReplica);
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
  public static GuidInfo lookupGuidInfo(String guid, boolean allowSiteToSiteQuery, ActiveReplica activeReplica) {
    ResultValue guidResult = null;
    try {
      guidResult = NameRecord.getNameRecordMultiField(activeReplica.getNameRecordDB(), guid, null, AccountAccess.GUID_INFO).getKey(AccountAccess.GUID_INFO);
    } catch (FieldNotFoundException e) {
    } catch (RecordNotFoundException e) {
    }
    // If we're allowed we go looking at other NameServers for the record in question.
    if (guidResult == null && allowSiteToSiteQuery) {
      guidResult = lookupGuidInfoSiteToSite(guid, AccountAccess.GUID_INFO, activeReplica);
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

  public static ResultValue lookupGuidInfoSiteToSite(String guid, String key,ActiveReplica activeReplica) {
    QueryResult queryResult = SiteToSiteQueryHandler.sendQuery(guid, key, activeReplica);
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
   * @return an {@link edu.umass.cs.gns.clientsupport.AccountInfo} instance
   */
  public static AccountInfo lookupAccountInfoFromName(String name, ActiveReplica activeReplica) {
    String guid = lookupGuid(name, activeReplica);
    if (guid != null) {
      return lookupAccountInfoFromGuid(guid, activeReplica);
    }
    return null;
  }
}
