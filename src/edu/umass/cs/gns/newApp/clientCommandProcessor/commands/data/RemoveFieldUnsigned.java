/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data;

import edu.umass.cs.gns.newApp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.UpdateOperation;
import static edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.*;

/**
 *
 * @author westy
 */
public class RemoveFieldUnsigned extends AbstractUpdate {

  public RemoveFieldUnsigned(CommandModule module) {
    super(module);
  }

  @Override
  public UpdateOperation getUpdateOperation() {
    return UpdateOperation.SINGLE_FIELD_REMOVE_FIELD;
  }

  @Override
  public String getCommandName() {
    return REMOVEFIELD;
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, FIELD, WRITER};
  }

  @Override
  public String getCommandDescription() {
    return "Removes the key value pair from the GNS for the given guid."
            + " Field must be world writeable as this command does not specify the writer and is not signed.";
    
  }
}
