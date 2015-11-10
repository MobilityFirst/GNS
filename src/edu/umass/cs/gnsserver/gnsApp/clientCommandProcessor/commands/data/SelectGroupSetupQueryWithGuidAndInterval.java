/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data;

import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.CommandModule;
import static edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.*;

/**
 * Initializes the group guid to automatically update and maintain all records that satisfy the query.
 * 
 * @author westy
 */
public class SelectGroupSetupQueryWithGuidAndInterval extends SelectGroupSetupQuery {

  /**
   *
   * @param module
   */
  public SelectGroupSetupQueryWithGuidAndInterval(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, QUERY, INTERVAL};
  }

  @Override
  public String getCommandDescription() {
    return "Initializes the group guid to automatically update and maintain all records that satisfy the query."
            + "Interval is the minimum refresh interval of the query - lookups happening more quickly than this"
            + "interval will retrieve a stale value." 
            + "For details see http://gns.name/wiki/index.php/Query_Syntax "
            + "Values are returned as a JSON array of JSON Objects.";
  }
}
