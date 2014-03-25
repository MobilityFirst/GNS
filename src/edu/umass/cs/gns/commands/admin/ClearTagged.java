/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.admin;

import edu.umass.cs.gns.clientsupport.AccountAccess;
import edu.umass.cs.gns.clientsupport.AccountInfo;
import edu.umass.cs.gns.clientsupport.Admintercessor;
import static edu.umass.cs.gns.clientsupport.Defs.*;
import edu.umass.cs.gns.commands.CommandModule;
import edu.umass.cs.gns.commands.GnsCommand;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * 
 * Note: Currently doesn't handle subGuids that are tagged! Only deletes account GUIDs that are tagged.
 *
 * @author westy
 */
public class ClearTagged extends GnsCommand {

  public ClearTagged(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{NAME};
  }

  @Override
  public String getCommandName() {
    return CLEARTAGGED;
  }

  @Override
  public String execute(JSONObject json) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
    String tagName = json.getString(NAME);
    for (String guid : Admintercessor.collectTaggedGuids(tagName)) {
      AccountInfo accountInfo = AccountAccess.lookupAccountInfoFromGuid(guid);
      if (accountInfo != null) {
        AccountAccess.removeAccount(accountInfo);
      }
    }
    return OKRESPONSE;
  }

  @Override
  public String getCommandDescription() {
    return "Removes all guids that contain the tag.";
  }
}
