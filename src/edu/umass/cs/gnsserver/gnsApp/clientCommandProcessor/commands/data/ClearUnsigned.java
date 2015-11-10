/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data;

import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.UpdateOperation;
import static edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.*;

/**
 *
 * @author westy
 */
public class ClearUnsigned extends AbstractUpdate {

  /**
   *
   * @param module
   */
  public ClearUnsigned(CommandModule module) {
    super(module);
  }

  /**
   * Return the update operation.
   * 
   * @return an {@link UpdateOperation}
   */
  @Override
  public UpdateOperation getUpdateOperation() {
    return UpdateOperation.SINGLE_FIELD_CLEAR;
  }

  @Override
  public String getCommandName() {
    return CLEAR;
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, FIELD, WRITER};
  }

  @Override
  public String getCommandDescription() {
    return "Clears the key value pair from the GNS for the given guid after "
            + "authenticating that GUID making request has access authority."
            + " Field must be world writeable as this command does not specify the writer and is not signed.";
  }
}
