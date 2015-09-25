/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.gnsApp.clientCommandProcessor.commands.account;

import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.AccountAccess;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.AccountInfo;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.CommandResponse;
import static edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.*;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commands.GnsCommand;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class LookupRandomGuids extends GnsCommand {

  /**
   * Creates a LookupAccountRecord instance.
   *
   * @param module
   */
  public LookupRandomGuids(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, GUIDCNT};
  }

  @Override
  public String getCommandName() {
    return LOOKUPACCOUNTRECORD;
  }

  @Override
  public CommandResponse<String> execute(JSONObject json, ClientRequestHandlerInterface handler) throws JSONException {
    String guid = json.getString(GUID);
    int count = json.getInt(GUIDCNT);
    AccountInfo acccountInfo;
    if ((acccountInfo = AccountAccess.lookupAccountInfoFromGuid(guid, handler)) == null) {
      return new CommandResponse<String>(BADRESPONSE + " " + BADACCOUNT + " " + guid);
    }
    if (acccountInfo != null) {
      List<String> guids = acccountInfo.getGuids();
      if (count >= guids.size()) {
        return new CommandResponse<>(new JSONArray(guids).toString());
      } else {
        Random rand = new Random();
        List<String> result = new ArrayList<>();
        for (int i = 0; i < count; i++) {
          result.add(guids.get(rand.nextInt(guids.size())));
        }
        return new CommandResponse<>(new JSONArray(result).toString());
      }
    } else {
      return new CommandResponse<>(BADRESPONSE + " " + BADGUID + " " + guid);
    }
    // }
  }

  @Override
  public String getCommandDescription() {
    return "Returns the account info associated with the given GUID. "
            + "Returns " + BADGUID + " if the GUID has not been registered.";

  }
}
