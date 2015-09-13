/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.gnsApp.clientCommandProcessor.commands.data;

import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commands.CommandModule;
import static edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.*;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.UpdateOperation;

/**
 *
 * @author westy
 */
public class ReplaceUnsigned extends AbstractUpdate {

  /**
   *
   * @param module
   */
  public ReplaceUnsigned(CommandModule module) {
    super(module);
  }

  /**
   *
   * @return
   */
  @Override
  public UpdateOperation getUpdateOperation() {
    return UpdateOperation.SINGLE_FIELD_REPLACE_ALL;
  }

  @Override
  public String getCommandName() {
    return REPLACE;
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, FIELD, VALUE};
  }

  @Override
  public String getCommandDescription() {
    return "Replaces the current value key value pair from the GNS for the given guid."
            + " Field must be world writeable as this command does not specify the writer and is not signed.";
  }
}
