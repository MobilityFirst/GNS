package edu.umass.cs.gnsserver.utils;

import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.SharedGuidUtils;
import edu.umass.cs.gnsserver.main.GNSConfig;
import org.json.JSONException;
import org.json.JSONObject;
import sun.security.tools.keytool.CertAndKeyGen;
import sun.security.x509.X500Name;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.*;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Properties;

/**
 * A class to generate signed certificates for pre-approved account names and
 * keys. An account GUID is required in order to do anything except
 * world-accessible operations with the GNS.
 *
 * @author arun
 */
public class CertificateSigning {

	public static final int CERT_BITLENGTH = 2048;

	/**
	 * Generates a certificate from the supplied [name, publickey] tuple.
	 * Suffices to self-sign because the only place where this certificate is
	 * checked is at servers.
	 *
	 * @param hrName
	 * @param publicKey
	 * @return
	 * @throws NoSuchProviderException
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidKeyException
	 * @throws IOException
	 * @throws CertificateException
	 * @throws SignatureException
	 * @throws JSONException
	 * @throws UnrecoverableKeyException
	 * @throws KeyStoreException
	 */
	public static Certificate generateCertificate(String hrName, PublicKey
		publicKey) throws NoSuchProviderException, NoSuchAlgorithmException,
		InvalidKeyException, IOException, CertificateException,
		SignatureException, JSONException, UnrecoverableKeyException,
		KeyStoreException {
		// read private key from keyStore
		PrivateKey caPrivateKey = GNSConfig.getPrivateKey();

		CertAndKeyGen certGen = new CertAndKeyGen("RSA", "SHA256WithRSA",
			null);
		certGen.generate(CERT_BITLENGTH);
		long validSecs = (long) 365 * 24 * 60 * 60; // one year

		String certInfo = genCertInfoString(hrName, publicKey);

		X509Certificate cert = certGen.getSelfCertificate(
			// enter your details according to your application
			new X500Name(certInfo), validSecs);
		return cert;
	}

	private static String genCertInfoString(String hrName, PublicKey
		publicKey) throws JSONException {
		return new JSONObject().put(GNSProtocol.NAME.toString(), hrName).put
			(GNSProtocol.PUBLIC_KEY.toString(), SharedGuidUtils
				.getPublicKeyString(publicKey)).toString();
	}

}
