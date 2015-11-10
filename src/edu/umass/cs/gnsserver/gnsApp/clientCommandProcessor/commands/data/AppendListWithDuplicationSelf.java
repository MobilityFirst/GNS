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
public class AppendListWithDuplicationSelf extends AbstractUpdateList {

  /**
   *
   * @param module
   */
  public AppendListWithDuplicationSelf(CommandModule module) {
    super(module);
  }

 /**
   * Return the update operation.
   * 
   * @return an {@link UpdateOperation}
   */
  @Override
  public UpdateOperation getUpdateOperation() {
    return UpdateOperation.SINGLE_FIELD_APPEND_WITH_DUPLICATION;
  }

  @Override
  public String getCommandName() {
    return APPENDLISTWITHDUPLICATION;
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, FIELD, VALUE, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandDescription() {
    return "Appends the values onto of this key value pair for the given GUID. Treats the list as a list, allows dupicate. "
            + "Value is a list of items formated as a JSON list.";
  }
}
