/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.data;

import edu.umass.cs.gns.clientsupport.UpdateOperation;
import static edu.umass.cs.gns.clientsupport.Defs.*;

/**
 *
 * @author westy
 */
public class AppendListWithDuplication extends AbstractUpdateList {

  public AppendListWithDuplication(CommandModule module) {
    super(module);
  }

  @Override
  public UpdateOperation getUpdateOperation() {
    return UpdateOperation.APPEND_WITH_DUPLICATION;
  }

  @Override
  public String getCommandName() {
    return APPENDLISTWITHDUPLICATION;
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, FIELD, VALUE, WRITER, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandDescription() {
    return  "Appends the values onto this key value pair for the given GUID. Treats the list as a list, allows dupicates. "
            + "Value is a list of items formated as a JSON list.";          
  }
}
