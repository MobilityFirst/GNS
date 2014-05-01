/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.nsdesign.commands.account;

import static edu.umass.cs.gns.clientsupport.Defs.*;
import edu.umass.cs.gns.nsdesign.clientsupport.NSAccountAccess;
import edu.umass.cs.gns.nsdesign.commands.NSCommand;
import edu.umass.cs.gns.nsdesign.commands.NSCommandModule;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurable;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class VerifyAccount extends NSCommand {
  
  public VerifyAccount(NSCommandModule module) {
    super(module);
  }
  
  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, CODE};
  }
  
  @Override
  public String getCommandName() {
    return VERIFYACCOUNT;
  }
  
  @Override
  public String execute(JSONObject json, GnsReconfigurable activeReplica) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
    String guid = json.getString(GUID);
    String code = json.getString(CODE);
    return NSAccountAccess.verifyAccount(guid, code, activeReplica);
  }
  
  @Override
  public String getCommandDescription() {
    return "Handles the completion of the verification process for a guid by supplying the correct code.";
  }
}
