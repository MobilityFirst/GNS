/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.data;

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
public class SelectQuery extends GnsCommand {

  public SelectQuery(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{QUERY};
  }

  @Override
  public String getCommandName() {
    return SELECT;
  }

  @Override
  public String execute(JSONObject json) throws JSONException {
    String query = json.getString(QUERY);
    return FieldAccess.selectQuery(query);
  }

  @Override
  public String getCommandDescription() {
    return "Returns all records that satisfy the query. "
            + "For details see http://mobilityfirst.cs.umass.edu/wiki/index.php/Query_Syntax "
            + "Values are returned as a JSON array of JSON Objects.";
  }
}
