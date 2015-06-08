/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.newApp.clientCommandProcessor.commands.acl;

import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.ClientUtils;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.CommandResponse;
import static edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.*;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.FieldMetaData;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.MetaDataTypeName;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commands.GnsCommand;
import edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class AclRetrieve extends GnsCommand {

  public AclRetrieve(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, FIELD, ACLTYPE, READER, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandName() {
    return ACLRETRIEVE;
  }

  @Override
  public CommandResponse execute(JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
    String guid = json.getString(GUID);
    String field = json.getString(FIELD);
    String accessType = json.getString(ACLTYPE);
    // allows someone other than guid to change the acl, defaults to guid
    String reader = json.optString(READER, guid);
    String signature = json.getString(SIGNATURE);
    String message = json.getString(SIGNATUREFULLMESSAGE);
    MetaDataTypeName access;
    if ((access = MetaDataTypeName.valueOf(accessType)) == null) {
      return new CommandResponse(BADRESPONSE + " " + BADACLTYPE + "Should be one of " + MetaDataTypeName.values().toString());
    }
    JSONArray guids = ClientUtils.convertPublicKeysToGuids(new JSONArray(FieldMetaData.lookup(access,
            guid, field, reader, signature, message, handler)));
    return new CommandResponse(guids.toString());
  }

  @Override
  public String getCommandDescription() {
    return "Returns the access control list for a guids's field.";

  }
}
