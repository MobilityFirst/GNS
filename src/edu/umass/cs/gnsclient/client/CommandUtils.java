/* Copyright (C) 2016 University of Massachusetts All Rights Reserved
 * 
 * Initial developer(s): Westy. */
package edu.umass.cs.gnsclient.client;

import edu.umass.cs.gnsclient.client.GNSClientConfig.GNSCC;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSCommandProtocol;
import edu.umass.cs.gnscommon.GNSResponseCode;
import edu.umass.cs.gnscommon.exceptions.client.EncryptionException;
import edu.umass.cs.gnscommon.exceptions.client.AclException;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.client.DuplicateNameException;
import edu.umass.cs.gnscommon.exceptions.client.FieldNotFoundException;
import edu.umass.cs.gnscommon.exceptions.client.InvalidFieldException;
import edu.umass.cs.gnscommon.exceptions.client.InvalidGuidException;
import edu.umass.cs.gnscommon.exceptions.client.VerificationException;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.packets.ResponsePacket;
import edu.umass.cs.gnscommon.utils.ByteUtils;
import edu.umass.cs.gnscommon.utils.CanonicalJSON;
import edu.umass.cs.gnscommon.utils.Format;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.OPERATION_NOT_SUPPORTED;
import edu.umass.cs.gnscommon.exceptions.client.OperationNotSupportedException;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.SessionKeys;
import edu.umass.cs.utils.SessionKeys.SecretKeyCertificate;
import edu.umass.cs.utils.Util;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.util.Date;
import java.util.Random;
import java.util.logging.Level;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy, arun
 */
public class CommandUtils {

	/* arun: at least as many instances as cores for parallelism. */
	private static Signature[] signatureInstances = new Signature[2 * Runtime
			.getRuntime().availableProcessors()];
	private static Random random;

	static {
		try {
			for (int i = 0; i < signatureInstances.length; i++) {
				signatureInstances[i] = Signature
						.getInstance(GNSCommandProtocol.SIGNATURE_ALGORITHM);
			}
			random = new Random();
		} catch (NoSuchAlgorithmException e) {
			GNSConfig.getLogger().log(Level.SEVERE,
					"Unable to initialize for authentication:{0}", e);
		}
	}

	private static int sigIndex = 0;

	private static synchronized Signature getSignatureInstance() {
		return signatureInstances[sigIndex++ % signatureInstances.length];
	}

	/**
	 * Creates a command object from the given action string and a variable
	 * number of key and value pairs.
	 * 
	 * @param commandType
	 *
	 * @param keysAndValues
	 * @return the query string
	 * @throws JSONException 
	 */
	public static JSONObject createCommand(CommandType commandType,
			Object... keysAndValues) throws JSONException {
		long startTime = System.currentTimeMillis();
			JSONObject result = new JSONObject();
			String key;
			Object value;
			result.put(GNSCommandProtocol.COMMAND_INT, commandType.getInt());
			for (int i = 0; i < keysAndValues.length; i = i + 2) {
				key = (String) keysAndValues[i];
				value = keysAndValues[i + 1];
				result.put(key, value);
			}

			DelayProfiler.updateDelay("createCommand", startTime);
			return result;
	}

	/**
	 * Only for backwards compatibility
	 * 
	 * @param response
	 * @return Single value string if response is JSONObject with a single
	 *         key-value piar.
	 */
	public static String specialCaseSingleField(String response) {
		if (JSONPacket.couldBeJSON(response) && response.startsWith("{"))
			try {
				JSONObject json = new JSONObject(response);
				String[] keys = JSONObject.getNames(json);
				return (keys.length == 1) ? json.getString(JSONObject
						.getNames(json)[0]) : response;
			} catch (JSONException e) {
				e.printStackTrace();
			}
		return response;
	}

	/**
	 * Disabled. 
	 * 
	 * @param commandType
	 * @param privateKey
	 * @param keysAndValues
	 * @return the query string
	 * @throws ClientException
	 */
	public static JSONObject createAndSignCommand(CommandType commandType,
			PrivateKey privateKey, Object... keysAndValues)
			throws ClientException {
		Util.suicide("This method is disabled: ");
		return null;
	}

	/**
	 * Signs a digest of a message using private key of the given guid.
	 *
	 * @param privateKey
	 * @param message
	 * @return a signed digest of the message string
	 * @throws InvalidKeyException
	 * @throws NoSuchAlgorithmException
	 * @throws SignatureException
	 * @throws java.io.UnsupportedEncodingException
	 *
	 *             arun: This method need to be synchronized over the signature
	 *             instance, otherwise it will result in corrupted signatures.
	 */
	public static String signDigestOfMessage(PrivateKey privateKey,
			String message) throws NoSuchAlgorithmException,
			InvalidKeyException, SignatureException,
			UnsupportedEncodingException {
		Signature signatureInstance = getSignatureInstance();
		synchronized (signatureInstance) {
			signatureInstance.initSign(privateKey);
			signatureInstance.update(message.getBytes("UTF-8"));
			byte[] signedString = signatureInstance.sign();
			// FIXME CHANGE THIS TO BASE64 (below) TO SAVE SOME SPACE ONCE THE
			// IOS CLIENT IS UPDATED AS WELL
			String result = ByteUtils.toHex(signedString);
			// String result = Base64.encodeToString(signedString, false);
			return result;
		}
	}

	private static final MessageDigest[] mds = new MessageDigest[Runtime
			.getRuntime().availableProcessors()];

	static {
		for (int i = 0; i < mds.length; i++) {
			try {
				mds[i] = MessageDigest
						.getInstance(GNSCommandProtocol.DIGEST_ALGORITHM);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}
	private static int mdIndex = 0;

	private static MessageDigest getMessageDigestInstance() {
		return mds[mdIndex++ % mds.length];
	}

	private static final Cipher[] ciphers = new Cipher[2 * Runtime.getRuntime()
			.availableProcessors()];

	static {
		for (int i = 0; i < ciphers.length; i++) {
			try {
				ciphers[i] = Cipher
						.getInstance(GNSCommandProtocol.SECRET_KEY_ALGORITHM);
			} catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}

	private static int cipherIndex = 0;

	private static Cipher getCipherInstance() {
		return ciphers[cipherIndex++ % ciphers.length];
	}

	/**
	 * @param privateKey
	 * @param publicKey
	 * @param message
	 * @return Signature as string
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidKeyException
	 * @throws SignatureException
	 * @throws UnsupportedEncodingException
	 * @throws IllegalBlockSizeException
	 * @throws BadPaddingException
	 * @throws NoSuchPaddingException
	 */
	public static String signDigestOfMessage(PrivateKey privateKey,
			PublicKey publicKey, String message)
			throws NoSuchAlgorithmException, InvalidKeyException,
			SignatureException, UnsupportedEncodingException,
			IllegalBlockSizeException, BadPaddingException,
			NoSuchPaddingException {
		SecretKey secretKey = SessionKeys.getOrGenerateSecretKey(publicKey,
				privateKey);
		MessageDigest md = getMessageDigestInstance();
		byte[] digest = null;
		byte[] body = message.getBytes(GNSCommandProtocol.CHARSET);
		synchronized (md) {
			digest = md.digest(body);
		}
		assert (digest != null);
		Cipher cipher = getCipherInstance();
		byte[] signature = null;
		synchronized (cipher) {
			cipher.init(Cipher.ENCRYPT_MODE, secretKey);
			signature = cipher.doFinal(digest);
		}

		SecretKeyCertificate skCert = SessionKeys
				.getSecretKeyCertificate(publicKey);
		byte[] encodedSKCert = skCert.getEncoded(false);

		/* arun: Combining them like this because the rest of the GNS code seems
		 * poorly organized to add more signature related fields in a systematic
		 * manner. */
		byte[] combined = new byte[Short.BYTES + signature.length + Short.BYTES
				+ encodedSKCert.length];
		ByteBuffer.wrap(combined)
		// signature
				.putShort((short) signature.length).put(signature)
				// certificate
				.putShort((short) encodedSKCert.length).put(encodedSKCert);

		return new String(combined, GNSCommandProtocol.CHARSET);
	}

	/**
	 * @param cvrp
	 * @return Response
	 * @throws ClientException
	 */
	public static ResponsePacket oldCheckResponse(ResponsePacket cvrp) throws ClientException {
		oldCheckResponse(cvrp.getReturnValue());
		return cvrp;
	}
	/**
	 * Checks the response from a command request for proper syntax as well as
	 * converting error responses into the appropriate thrown GNS exceptions.
	 *
	 * In the original protocol the string response was modeled after other
	 * simple string-based response protocols. Responses were either:<br>
	 * 1) a return value whose format was interpreted by the caller - this is
	 * the nominal case<br>
	 * 2) "+OK+" - which was used to indicate a nominal result for commands that
	 * don't return a value<br>
	 * 2) "+NULL+" - another nominal which meant we should return null as the
	 * value<br>
	 * 3) "+NO+"{space}{error code string}{{space}{additional info string}}+<br>
	 * Later a special case 4 was added for ACTIVE_REPLICA_EXCEPTION.<br>
	 *
	 * For case 3 the additional info strings (could be any number) were
	 * interpreted by the error handlers and generally used to help provide
	 * additional info to indicate error causes.
	 *
	 * Also note that:<br>
	 * GNSCommandProtocol.OK_RESPONSE = "+OK+"<br>
	 * GNSCommandProtocol.BAD_RESPONSE = "+NO+"<br>
	 * GNSCommandProtocol.NULL_RESPONSE = "+NULL+"<br>
	 *
	 * @param response
	 * @return Response as string.
	 * @throws ClientException
	 */
	public static String oldCheckResponse(String response) throws ClientException {
		// System.out.println("response:" + response);
		if (response.startsWith(GNSCommandProtocol.BAD_RESPONSE)) {
			String[] results = response.split(" ");
			// System.out.println("results length:" + results.length);
			if (results.length < 2) {
				throw new ClientException("Invalid bad response indicator: "
						+ response);
			} else if (results.length >= 2) {
				// System.out.println("results[0]:" + results[0]);
				// System.out.println("results[1]:" + results[1]);
				String error = results[1];
				// deal with the rest
				StringBuilder parts = new StringBuilder();
				for (int i = 2; i < results.length; i++) {
					parts.append(" ");
					parts.append(results[i]);
				}
				String rest = parts.toString();
				if (error.startsWith(GNSCommandProtocol.BAD_SIGNATURE)) {
					throw new EncryptionException(error);
				}
				if (error.startsWith(GNSCommandProtocol.BAD_GUID)
						|| error.startsWith(GNSCommandProtocol.BAD_ACCESSOR_GUID)
						// why not with DUPLICATE_NAME?
						|| error.startsWith(GNSCommandProtocol.DUPLICATE_GUID)
						|| error.startsWith(GNSCommandProtocol.BAD_ACCOUNT)) {
					throw new InvalidGuidException(error + rest);
				}
				if (error.startsWith(GNSCommandProtocol.DUPLICATE_FIELD)) {
					throw new InvalidFieldException(error + rest);
				}
				if (error.startsWith(GNSCommandProtocol.FIELD_NOT_FOUND)) {
					throw new FieldNotFoundException(error + rest);
				}
				if (error.startsWith(GNSCommandProtocol.ACCESS_DENIED)) {
					throw new AclException(error + rest);
				}
				if (error.startsWith(GNSCommandProtocol.DUPLICATE_NAME)) {
					throw new DuplicateNameException(error + rest);
				}
				if (error.startsWith(GNSCommandProtocol.VERIFICATION_ERROR)) {
					throw new VerificationException(error + rest);
				}
				if (error
						.startsWith(GNSCommandProtocol.ALREADY_VERIFIED_EXCEPTION)) {
					throw new VerificationException(error + rest);
				}
				if (error.startsWith(OPERATION_NOT_SUPPORTED)) {
					throw new OperationNotSupportedException(error + rest);
				}
				throw new ClientException("General command failure: " + error
						+ rest);
			}
		}
		if (response.startsWith(GNSCommandProtocol.NULL_RESPONSE)) {
			return null;
		} else if (response
				.startsWith(GNSCommandProtocol.ACTIVE_REPLICA_EXCEPTION
						.toString())) {
			throw new InvalidGuidException(response);
		} else {
			return response;
		}
	}

	/**
	 * @param response
	 * @return Response as String
	 * @throws ClientException
	 */
	public static String checkResponse(ResponsePacket response)
			throws ClientException {
		return checkResponse(response, null).getReturnValue();
	}

	private static final boolean USE_OLD_CHECK_RESPONSE = false;

	/**
	 * arun: This checkResponse method will replace the old one. There is no
	 * reason to not directly use the received CommandValueReturnPacket.
	 * 
	 * @param command
	 *
	 * @param responsePacket
	 * @return Response as a string.
	 * @throws ClientException
	 */
	public static ResponsePacket checkResponse(
			ResponsePacket responsePacket, CommandPacket command) throws ClientException {
		if (USE_OLD_CHECK_RESPONSE) 
			return oldCheckResponse(responsePacket);

		GNSResponseCode code = responsePacket.getErrorCode();
		String returnValue = responsePacket.getReturnValue();
		GNSConfig.getLogger().log(Level.FINE, "New check response: {0} {1}",
				new Object[] { code, responsePacket.getSummary() });
		// If the code isn't an error or exception we're just returning the
		// return value. Also handle the special case where the command
		// wants to return a null value.
		if (code.isOKResult()) {
			return (returnValue.startsWith(GNSCommandProtocol.NULL_RESPONSE)) ? null
					: responsePacket;//returnValue;
		}
		// else error
		String errorSummary = code
				+ ": "
				+ returnValue
				//+ ": " + responsePacket.getSummary()
				+ (command != null ? " for command " + command.getSummary()
						: "");
		switch (code) {
		case SIGNATURE_ERROR:
			throw new EncryptionException(code, errorSummary);

		case BAD_GUID_ERROR:
		case BAD_ACCESSOR_ERROR:
		case BAD_ACCOUNT_ERROR:
			throw new InvalidGuidException(code, errorSummary);

		case FIELD_NOT_FOUND_ERROR:
			throw new FieldNotFoundException(code, errorSummary);
		case ACCESS_ERROR:
			throw new AclException(code, errorSummary);
		case VERIFICATION_ERROR:
			throw new VerificationException(code, errorSummary);
		case ALREADY_VERIFIED_EXCEPTION:
			throw new VerificationException(code, errorSummary);
		case DUPLICATE_ID_EXCEPTION:
		case DUPLICATE_GUID_EXCEPTION:
		case DUPLICATE_NAME_EXCEPTION:
			throw new DuplicateNameException(code, errorSummary);
		case DUPLICATE_FIELD_EXCEPTION:
			throw new InvalidFieldException(code, errorSummary);

		case ACTIVE_REPLICA_EXCEPTION:
			throw new InvalidGuidException(code, errorSummary);
		case NONEXISTENT_NAME_EXCEPTION:
			throw new InvalidGuidException(code, errorSummary);
		case TIMEOUT:
			throw new ClientException(code, errorSummary);

		default:
			throw new ClientException(code,
					"Error received with an unknown response code: "
							+ errorSummary);
		}
	}

	/**
	 * @return Random long.
	 */
	public static String getRandomRequestNonce() {
		return (random.nextLong()+"").toString();
	}

	/**
	 * @param commandType
	 * @param privateKey
	 * @param publicKey
	 * @param keysAndValues
	 * @return Signed command.
	 * @throws ClientException
	 */
	public static JSONObject createAndSignCommand(CommandType commandType,
			PrivateKey privateKey, PublicKey publicKey, Object... keysAndValues)
			throws ClientException {
		try {
			JSONObject result = createCommand(commandType, keysAndValues);
			result.put(GNSCommandProtocol.TIMESTAMP,
					Format.formatDateISO8601UTC(new Date()));
			result.put(GNSCommandProtocol.NONCE, getRandomRequestNonce());

			String canonicalJSON = CanonicalJSON.getCanonicalForm(result);
			String signatureString = null;
			long t = System.nanoTime();
			if (!Config.getGlobalBoolean(GNSCC.ENABLE_SECRET_KEY)) {
				signatureString = signDigestOfMessage(privateKey, canonicalJSON);
			} else {
				signatureString = signDigestOfMessage(privateKey, publicKey,
						canonicalJSON);
			}
			result.put(GNSCommandProtocol.SIGNATURE, signatureString);
			if (edu.umass.cs.utils.Util.oneIn(10)) {
				DelayProfiler.updateDelayNano("signature", t);
			}
			return result;
		} catch (JSONException | NoSuchAlgorithmException | InvalidKeyException
				| SignatureException | UnsupportedEncodingException
				| IllegalBlockSizeException | BadPaddingException
				| NoSuchPaddingException e) {
			throw new ClientException("Error encoding message", e);
		}
	}

	/**
	 * @param commandType
	 * @param querier
	 * @param keysAndValues
	 * @return JSONObject command
	 * @throws ClientException
	 */
	public static JSONObject createAndSignCommand(CommandType commandType,
			GuidEntry querier, Object... keysAndValues) throws ClientException {
		try {
			return querier != null ? createAndSignCommand(commandType,
					querier.getPrivateKey(), querier.getPublicKey(), keysAndValues)
					: createCommand(commandType, keysAndValues);
		} catch (JSONException e) {
			throw new ClientException("Error encoding message", e);
		}
	}

}
