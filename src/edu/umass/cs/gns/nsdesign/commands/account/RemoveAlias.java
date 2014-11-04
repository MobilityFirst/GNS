/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.nsdesign.commands.account;

import edu.umass.cs.gns.clientsupport.AccountInfo;
import static edu.umass.cs.gns.clientsupport.Defs.*;
import edu.umass.cs.gns.clientsupport.GuidInfo;
import edu.umass.cs.gns.exceptions.FailedDBOperationException;
import edu.umass.cs.gns.nsdesign.clientsupport.NSAccessSupport;
import edu.umass.cs.gns.nsdesign.clientsupport.NSAccountAccess;
import edu.umass.cs.gns.nsdesign.commands.NSCommand;
import edu.umass.cs.gns.nsdesign.commands.NSCommandModule;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurableInterface;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
@Deprecated
public class RemoveAlias extends NSCommand {

  public RemoveAlias(NSCommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, NAME, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandName() {
    return REMOVEALIAS;
  }

  @Override
  public String execute(JSONObject json, GnsReconfigurableInterface activeReplica, InetSocketAddress lnsAddress) 
          throws InvalidKeyException, InvalidKeySpecException, JSONException, NoSuchAlgorithmException, SignatureException, 
          FailedDBOperationException, UnsupportedEncodingException {
    String guid = json.getString(GUID);
    String name = json.getString(NAME);
    String signature = json.getString(SIGNATURE);
    String message = json.getString(SIGNATUREFULLMESSAGE);
    GuidInfo guidInfo;
    if ((guidInfo = NSAccountAccess.lookupGuidInfo(guid, activeReplica, lnsAddress)) == null) {
      return BADRESPONSE + " " + BADGUID + " " + guid;
    }
    if (NSAccessSupport.verifySignature(guidInfo, signature, message)) {
      AccountInfo accountInfo = NSAccountAccess.lookupAccountInfoFromGuid(guid, activeReplica, lnsAddress);
      if (accountInfo == null) {
        return BADRESPONSE + " " + BADACCOUNT + " " + guid;
      }
      return NSAccountAccess.removeAlias(accountInfo, name, activeReplica, lnsAddress);
    } else {
      return BADRESPONSE + " " + BADSIGNATURE;
    }
  }

  @Override
  public String getCommandDescription() {
    return "Removes the alias from the account associated with the GUID. Must be signed by the guid. Returns "
            + BADGUID + " if the GUID has not been registered.";

  }
}
