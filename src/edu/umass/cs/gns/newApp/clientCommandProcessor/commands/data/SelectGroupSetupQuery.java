/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data;

import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commands.GnsCommand;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.FieldAccess;
import static edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.*;
import edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import edu.umass.cs.gns.util.Base64;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Initializes a new group guid to automatically update and maintain all records that satisfy the query.
 * 
 * @author westy
 */
public class SelectGroupSetupQuery extends GnsCommand {

  public SelectGroupSetupQuery(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{ACCOUNT_GUID, QUERY};
  }

  @Override
  public String getCommandName() {
    return SELECTGROUP;
  }

  @Override
  public CommandResponse execute(JSONObject json, ClientRequestHandlerInterface handler) throws JSONException {
    String accountGuid = json.getString(ACCOUNT_GUID);
    String query = json.getString(QUERY);
    String publicKey = json.getString(PUBLICKEY);
    int interval = json.optInt(INTERVAL, -1);
    
    return FieldAccess.selectGroupSetupQuery(accountGuid, query, publicKey, interval, handler);
  }

  @Override
  public String getCommandDescription() {
    return "Initializes a new group guid to automatically update and maintain all records that satisfy the query."
            + "For details see http://gns.name/wiki/index.php/Query_Syntax "
            + "Values are returned as a JSON array of JSON Objects.";
  }
}
