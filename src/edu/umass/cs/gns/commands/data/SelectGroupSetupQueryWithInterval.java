/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.data;

import edu.umass.cs.gns.commands.CommandModule;
import static edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.Defs.*;

/**
 * Initializes a new group guid to automatically update and maintain all records that satisfy the query.
 * 
 * @author westy
 */
public class SelectGroupSetupQueryWithInterval extends SelectGroupSetupQuery {

  public SelectGroupSetupQueryWithInterval(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{QUERY, INTERVAL};
  }

  @Override
  public String getCommandDescription() {
    return "Initializes a new group guid to automatically update and maintain all records that satisfy the query."
            + "Interval is the minimum refresh interval of the query - lookups happening more quickly than this"
            + "interval will retrieve a stale value." 
            + "For details see http://gns.name/wiki/index.php/Query_Syntax "
            + "Values are returned as a JSON array of JSON Objects.";
  }
}
