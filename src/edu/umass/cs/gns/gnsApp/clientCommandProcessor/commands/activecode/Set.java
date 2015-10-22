package edu.umass.cs.gns.gnsApp.clientCommandProcessor.commands.activecode;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.ActiveCode;
import edu.umass.cs.gns.gnsApp.NSResponseCode;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.CommandResponse;
import static edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.ACACTION;
import static edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.ACCODE;
import static edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.ACSET;
import static edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.BADRESPONSE;
import static edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.GUID;
import static edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.OKRESPONSE;
import static edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.SIGNATURE;
import static edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.SIGNATUREFULLMESSAGE;
import static edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.WRITER;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commands.GnsCommand;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;

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
	
	public CommandResponse<String> execute(JSONObject json, 
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
			return new CommandResponse<String>(BADRESPONSE + " " + response.getProtocolCode());
		else
			return new CommandResponse<String>(OKRESPONSE);
	}

	@Override
	public String getCommandDescription() {
		// TODO Auto-generated method stub
		return "Sets the given active code for the specified GUID and action," +
				"ensuring the writer has permission";
	}

}
