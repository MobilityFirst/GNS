/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.data;

import edu.umass.cs.gns.commands.CommandModule;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.UpdateOperation;
import static edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.Defs.*;

/**
 *
 * @author westy
 */
public class AppendUnsigned extends AbstractUpdate {

  public AppendUnsigned(CommandModule module) {
    super(module);
  }

  @Override
  public UpdateOperation getUpdateOperation() {
    return UpdateOperation.SINGLE_FIELD_APPEND;
  }

  @Override
  public String getCommandName() {
    return APPEND;
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, FIELD, VALUE};
  }

  @Override
  public String getCommandDescription() {
    return "Appends the value onto the key value pair for the given GUID. Treats the list as a set, removing duplicates"
            + " Field must be world writeable as this command does not specify the writer and is not signed.";
  }
}
