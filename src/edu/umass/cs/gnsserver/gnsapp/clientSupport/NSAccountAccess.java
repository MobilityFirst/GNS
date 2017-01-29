
package edu.umass.cs.gnsserver.gnsapp.clientSupport;

import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountAccess;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.GuidInfo;
import edu.umass.cs.gnsserver.gnsapp.deprecated.GNSApplicationInterface;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnsserver.utils.ValuesMap;

import org.json.JSONException;

import java.text.ParseException;
import java.util.logging.Level;

import org.json.JSONObject;


public class NSAccountAccess {


  public static GuidInfo lookupGuidInfoLocally(String guid,  GNSApplicationInterface<String> gnsApp) 
          throws FailedDBOperationException {
   ValuesMap valuesMap = NSFieldAccess.lookupJSONFieldLocally(null, guid, 
            AccountAccess.GUID_INFO, gnsApp);
    if (valuesMap.has(AccountAccess.GUID_INFO)) {
      try {
        // Something simpler here?
        return new GuidInfo(new JSONObject(valuesMap.get(AccountAccess.GUID_INFO).toString()));
      } catch (JSONException | ParseException e) {
        GNSConfig.getLogger().log(Level.SEVERE, "Problem parsing guidinfo: {0}", e);
      }
    }
    return null;
  }
  

  public static GuidInfo lookupGuidInfoAnywhere(InternalRequestHeader header, String guid, GNSApplicationInterface<String> gnsApp) 
          throws FailedDBOperationException {
    ValuesMap valuesMap = NSFieldAccess.lookupJSONFieldAnywhere(header, guid, 
            AccountAccess.GUID_INFO, gnsApp);
    if (valuesMap.has(AccountAccess.GUID_INFO)) {
      try {
        // Something simpler here?
        return new GuidInfo(new JSONObject(valuesMap.get(AccountAccess.GUID_INFO).toString()));
      } catch (JSONException | ParseException e) {
        GNSConfig.getLogger().log(Level.SEVERE, "Problem parsing guidinfo: {0}", e);
      }
    }
    return null;
  }
}
