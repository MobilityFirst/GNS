/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.acl;

import static edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.Defs.*;
import edu.umass.cs.gns.commands.CommandModule;

/**
 *
 * @author westy
 */
public class AclAddSelf extends AclAdd {

  public AclAddSelf(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, FIELD, ACCESSER, ACLTYPE, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandName() {
    return ACLADD;
  }

  @Override
  public String getCommandDescription() {
    return "Updates the access control list of the given GUID's field to include the accesser guid. " + NEWLINE
            + "Accessor should a guid or group guid or " + EVERYONE + " which means anyone." + NEWLINE
            + "Field can be also be " + ALLFIELDS + " which means all fields can be read by the accessor." + NEWLINE
            + "See below for description of ACL type and signature.";
  }
}
