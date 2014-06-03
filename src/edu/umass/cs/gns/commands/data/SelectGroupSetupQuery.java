/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.data;

import edu.umass.cs.gns.clientsupport.CommandResponse;
import edu.umass.cs.gns.commands.GnsCommand;
import edu.umass.cs.gns.commands.CommandModule;
import edu.umass.cs.gns.clientsupport.FieldAccess;
import static edu.umass.cs.gns.clientsupport.Defs.*;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class SelectGroupSetupQuery extends GnsCommand {

  public SelectGroupSetupQuery(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, QUERY};
  }

  @Override
  public String getCommandName() {
    return SELECT;
  }

  @Override
  public CommandResponse execute(JSONObject json) throws JSONException {
    String guid = json.getString(GUID);
    String query = json.getString(QUERY);
    return FieldAccess.selectGroupSetupQuery(query, guid);
  }

  @Override
  public String getCommandDescription() {
    return "Intializes the group guid to automatically update and maintain all records that satisfy the query."
            + "For details see http://mobilityfirst.cs.umass.edu/wiki/index.php/Query_Syntax "
            + "Values are returned as a JSON array of JSON Objects.";
  }
}
