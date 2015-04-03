package edu.umass.cs.gns.commands.activecode;

import static edu.umass.cs.gns.clientsupport.Defs.*;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.clientsupport.CommandResponse;
import edu.umass.cs.gns.clientsupport.ActiveCode;
import edu.umass.cs.gns.commands.CommandModule;
import edu.umass.cs.gns.commands.GnsCommand;
import edu.umass.cs.gns.localnameserver.ClientRequestHandlerInterface;
import edu.umass.cs.gns.util.NSResponseCode;

public class Clear extends GnsCommand {

	public Clear(CommandModule module) {
		super(module);
	}

	@Override
	public String[] getCommandParameters() {
		return new String[]{GUID, WRITER, ACACTION, SIGNATURE, SIGNATUREFULLMESSAGE};
	}

	@Override
	public String getCommandName() {
		return ACCLEAR;
	}

	@Override
	public CommandResponse execute(JSONObject json, 
			ClientRequestHandlerInterface handler) throws InvalidKeyException,
			InvalidKeySpecException, JSONException, NoSuchAlgorithmException,
			SignatureException {
		String accountGuid = json.getString(GUID);
		String writer = json.getString(WRITER);
		String action = json.getString(ACACTION);
		String signature = json.getString(SIGNATURE);
		String message = json.getString(SIGNATUREFULLMESSAGE);
	  
		NSResponseCode response = ActiveCode.clearCode(accountGuid, action, writer, signature, message, handler);
		
		if(response.isAnError())
			return new CommandResponse(BADRESPONSE + " " + response.getProtocolCode());
		else
			return new CommandResponse(OKRESPONSE);
	}

	@Override
	public String getCommandDescription() {
		return "Clears the active code for the specified GUID and action," +
				"ensuring the writer has permission";
	}
}
