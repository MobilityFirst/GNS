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
import edu.umass.cs.gns.nsdesign.recordmap.NameRecord;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurable;
import org.json.JSONException;

import java.text.ParseException;

/*** DO NOT not use any class in package edu.umass.cs.gns.nsdesign ***/

public class NSAccountAccess {

  public static AccountInfo lookupAccountInfoFromGuid(String guid, GnsReconfigurable activeReplica) {
    ResultValue accountResult = null;
    try {
      accountResult = NameRecord.getNameRecordMultiField(activeReplica.getDB(), guid, null, AccountAccess.ACCOUNT_INFO).getKey(AccountAccess.ACCOUNT_INFO);
    } catch (FieldNotFoundException e) {
    } catch (RecordNotFoundException e) {
    }
    if (accountResult == null) {
      try {
        guid = lookupPrimaryGuid(guid, activeReplica);
        if (guid != null) {
          accountResult = NameRecord.getNameRecordMultiField(activeReplica.getDB(), guid, null, AccountAccess.ACCOUNT_INFO).getKey(AccountAccess.ACCOUNT_INFO);
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
  public static String lookupPrimaryGuid(String guid, GnsReconfigurable activeReplica) {
    ResultValue guidResult = null;
    try {
      guidResult = NameRecord.getNameRecordMultiField(activeReplica.getDB(), guid, null, AccountAccess.PRIMARY_GUID).getKey(AccountAccess.PRIMARY_GUID);
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
  public static String lookupGuid(String name, GnsReconfigurable activeReplica) {
    ResultValue guidResult = null;
    try {
      guidResult = NameRecord.getNameRecordMultiField(activeReplica.getDB(), name, null, AccountAccess.GUID).getKey(AccountAccess.GUID);
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
  public static GuidInfo lookupGuidInfo(String guid, GnsReconfigurable activeReplica) {
    return NSAccountAccess.lookupGuidInfo(guid, false, activeReplica);
  }

  /**
   * Obtains the guid info record from the database for GUID given.
   * If allowSiteToSiteQuery is true and the record is not available locally
   * a query will be sent another name server to find the record.
   *
   * @param guid
   * @param allowQueryToOtherNSs
   * @return
   */
  public static GuidInfo lookupGuidInfo(String guid, boolean allowQueryToOtherNSs, GnsReconfigurable activeReplica) {
    ResultValue guidResult = NSFieldAccess.lookupField(guid, AccountAccess.GUID_INFO, allowQueryToOtherNSs, activeReplica);
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
  public static AccountInfo lookupAccountInfoFromName(String name, GnsReconfigurable activeReplica) {
    String guid = lookupGuid(name, activeReplica);
    if (guid != null) {
      return lookupAccountInfoFromGuid(guid, activeReplica);
    }
    return null;
  }
}
