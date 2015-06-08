/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data;

import edu.umass.cs.gns.newApp.clientCommandProcessor.commands.CommandModule;
import static edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.*;

/**
 * Initializes the given group guid to automatically update and maintain all records that satisfy the query.
 *
 * @author westy
 */
public class SelectGroupSetupQueryWithGuid extends SelectGroupSetupQuery {

  public SelectGroupSetupQueryWithGuid(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{QUERY, GUID};
  }

  @Override
  public String getCommandDescription() {
    return "Initializes the given group guid to automatically update and maintain all records that satisfy the query."
            + "For details see http://gns.name/wiki/index.php/Query_Syntax "
            + "Values are returned as a JSON array of JSON Objects.";
  }
}
