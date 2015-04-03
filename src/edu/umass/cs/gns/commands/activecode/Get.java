package edu.umass.cs.gns.commands.activecode;

import static edu.umass.cs.gns.clientsupport.Defs.ACACTION;
import static edu.umass.cs.gns.clientsupport.Defs.ACCODE;
import static edu.umass.cs.gns.clientsupport.Defs.BADRESPONSE;
import static edu.umass.cs.gns.clientsupport.Defs.BADSIGNATURE;
import static edu.umass.cs.gns.clientsupport.Defs.GUID;
import static edu.umass.cs.gns.clientsupport.Defs.OKRESPONSE;
import static edu.umass.cs.gns.clientsupport.Defs.SIGNATURE;
import static edu.umass.cs.gns.clientsupport.Defs.SIGNATUREFULLMESSAGE;
import static edu.umass.cs.gns.clientsupport.Defs.WRITER;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.clientsupport.ActiveCode;
import edu.umass.cs.gns.clientsupport.CommandResponse;
import edu.umass.cs.gns.clientsupport.FieldMetaData;
import edu.umass.cs.gns.clientsupport.MetaDataTypeName;
import edu.umass.cs.gns.commands.CommandModule;
import edu.umass.cs.gns.commands.GnsCommand;
import edu.umass.cs.gns.localnameserver.ClientRequestHandlerInterface;
import edu.umass.cs.gns.util.NSResponseCode;
import static edu.umass.cs.gns.clientsupport.Defs.*;

public class Get extends GnsCommand {

	public Get(CommandModule module) {
		super(module);
	}

	@Override
	public String[] getCommandParameters() {
		return new String[]{GUID, READER, ACACTION, SIGNATURE, SIGNATUREFULLMESSAGE};
	}

	@Override
	public String getCommandName() {
		return ACGET;
	}

	@Override
	public CommandResponse execute(JSONObject json,
			ClientRequestHandlerInterface handler) throws InvalidKeyException,
			InvalidKeySpecException, JSONException, NoSuchAlgorithmException,
			SignatureException {
		String accountGuid = json.getString(GUID);
		String reader = json.getString(READER);
		String action = json.getString(ACACTION);
		String signature = json.getString(SIGNATURE);
		String message = json.getString(SIGNATUREFULLMESSAGE);
		
		return new CommandResponse(
				new JSONArray(
						ActiveCode.getCode(accountGuid, action, reader, signature, message, handler)).toString());
	}

	@Override
	public String getCommandDescription() {
		return "Returns the active code for the specified action," +
				"ensuring the reader has permission";
	}
	

}
