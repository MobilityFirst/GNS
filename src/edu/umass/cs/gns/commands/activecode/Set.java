package edu.umass.cs.gns.commands.activecode;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.clientsupport.AccessSupport;
import edu.umass.cs.gns.clientsupport.AccountAccess;
import edu.umass.cs.gns.clientsupport.AccountInfo;
import edu.umass.cs.gns.clientsupport.ActiveCode;
import edu.umass.cs.gns.clientsupport.ClientUtils;
import edu.umass.cs.gns.clientsupport.CommandResponse;
import edu.umass.cs.gns.clientsupport.FieldMetaData;
import edu.umass.cs.gns.clientsupport.GuidInfo;
import edu.umass.cs.gns.clientsupport.MetaDataTypeName;
import edu.umass.cs.gns.commands.CommandModule;
import edu.umass.cs.gns.commands.GnsCommand;
import edu.umass.cs.gns.localnameserver.ClientRequestHandlerInterface;
import edu.umass.cs.gns.util.NSResponseCode;
import static edu.umass.cs.gns.clientsupport.Defs.*;

public class Set extends GnsCommand {

	public Set(CommandModule module) {
		super(module);
	}

	@Override
	public String[] getCommandParameters() {
		// TODO Auto-generated method stub
		return new String[]{GUID, WRITER, ACACTION, ACCODE, SIGNATURE, SIGNATUREFULLMESSAGE};
	}

	@Override
	public String getCommandName() {
		return ACSET;
	}
	
	public CommandResponse execute(JSONObject json, 
			ClientRequestHandlerInterface handler) throws InvalidKeyException,
			InvalidKeySpecException, JSONException, NoSuchAlgorithmException,
			SignatureException {
		String accountGuid = json.getString(GUID);
		String writer = json.getString(WRITER);
		String action = json.getString(ACACTION);
		String code = json.getString(ACCODE);
		String signature = json.getString(SIGNATURE);
		String message = json.getString(SIGNATUREFULLMESSAGE);
	  
		NSResponseCode response = ActiveCode.setCode(accountGuid, action, code, writer, signature, message, handler);
		
		if(response.isAnError())
			return new CommandResponse(BADRESPONSE + " " + response.getProtocolCode());
		else
			return new CommandResponse(OKRESPONSE);
	}

	@Override
	public String getCommandDescription() {
		// TODO Auto-generated method stub
		return "Sets the given active code for the specified GUID and action," +
				"ensuring the writer has permission";
	}

}
