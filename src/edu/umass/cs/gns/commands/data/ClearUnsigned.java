/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.data;

import edu.umass.cs.gns.commands.CommandModule;
import edu.umass.cs.gns.clientsupport.UpdateOperation;
import static edu.umass.cs.gns.clientsupport.Defs.*;

/**
 *
 * @author westy
 */
public class ClearUnsigned extends AbstractUpdate {

  public ClearUnsigned(CommandModule module) {
    super(module);
  }

  @Override
  public UpdateOperation getUpdateOperation() {
    return UpdateOperation.CLEAR;
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
