/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.data;

import edu.umass.cs.gns.commands.CommandModule;
import static edu.umass.cs.gns.clientsupport.Defs.*;
import edu.umass.cs.gns.clientsupport.FieldAccess;
import edu.umass.cs.gns.clientsupport.UpdateOperation;
import edu.umass.cs.gns.commands.GnsCommand;
import edu.umass.cs.gns.util.NSResponseCode;
import edu.umass.cs.gns.util.ResultValue;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class SetFieldNull extends AbstractUpdate {

  public SetFieldNull(CommandModule module) {
    super(module);
  }
  
  @Override
  public UpdateOperation getUpdateOperation() {
    return UpdateOperation.SETFIELDNULL;
  }

  @Override
  public String getCommandName() {
    return SETFIELDNULL;
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, FIELD, WRITER, SIGNATURE, SIGNATUREFULLMESSAGE};
  }


  @Override
  public String getCommandDescription() {
    return "Sets the field to contain a null value.";        
  }
}
