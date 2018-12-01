package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.secured;

import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.SharedGuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.OperationNotSupportedException;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.FieldNotFoundException;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor
	.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport
	.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands
	.AbstractCommand;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands
	.CommandModule;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnsserver.utils.CertificateSigning;
import org.json.JSONException;
import org.json.JSONObject;
import sun.misc.BASE64Encoder;
import sun.security.provider.X509Factory;

import javax.xml.ws.Response;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.*;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.text.ParseException;

public class SignCertificate extends AbstractCommand {

	/**
	 * Creates a new <code>AbstractCommand</code> object
	 *
	 * @param module
	 */
	public SignCertificate(CommandModule module) {
		super(module);
	}

	@Override
	public CommandType getCommandType() {
		return CommandType.SignCertificate;
	}

	@Override
	public CommandResponse execute(InternalRequestHeader internalHeader,
								   CommandPacket commandPacket,
								   ClientRequestHandlerInterface handler)
		throws InvalidKeyException, InvalidKeySpecException, JSONException,
		NoSuchAlgorithmException, SignatureException,
		UnsupportedEncodingException, ParseException,
		InternalRequestException, OperationNotSupportedException,
		FailedDBOperationException, FieldNotFoundException {

		/* This is an admin command, so it is implicitly trusted.
		 */
		JSONObject json = commandPacket.getCommand();
		String name = json.getString(GNSProtocol.NAME.toString());
		String publicKey = json.getString(GNSProtocol.PUBLIC_KEY.toString());

		// generate signature
		Certificate cert = null;
		try {
			cert = CertificateSigning.generateCertificate(name,
				KeyFactory.getInstance("RSA").generatePublic(new
					X509EncodedKeySpec(Base64.decode(publicKey))));
		} catch (NoSuchProviderException | IOException | KeyStoreException |
			UnrecoverableKeyException | CertificateException e) {
			e.printStackTrace();
			return new CommandResponse(ResponseCode.IO_EXCEPTION, e.getMessage
				());
		}

		StringBuilder s = new StringBuilder();
		BASE64Encoder encoder = new BASE64Encoder();
		try {
			s.append(X509Factory.BEGIN_CERT).append(Base64.encodeToString(cert
				.getEncoded(), true)).append(X509Factory.END_CERT);
		} catch (CertificateEncodingException e) {
			e.printStackTrace();
			return new CommandResponse(ResponseCode.IO_EXCEPTION, e.getMessage
				());
		}

		// make response
		return new CommandResponse(ResponseCode.NO_ERROR,
			s.toString());

	}
}
