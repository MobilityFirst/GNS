package edu.umass.cs.gnsclient.client;

import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.SessionKeys;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.xml.bind.DatatypeConverter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.security.*;
import java.security.cert.CertificateException;
import java.util.logging.Level;


public class CryptoUtils {



    static final MessageDigest[] mds = new MessageDigest[Runtime
            .getRuntime().availableProcessors()];
    static final Cipher[] ciphers = new Cipher[2 * Runtime.getRuntime()
                    .availableProcessors()];
    /* arun: at least as many instances as cores for parallelism. */
    static Signature[] signatureInstances = new Signature[2 * Runtime
            .getRuntime().availableProcessors()];

    private static int sigIndex = 0;
    private static int mdIndex = 0;
    private static int cipherIndex = 0;

    static {
        try {
            for (int i = 0; i < signatureInstances.length; i++) {
                signatureInstances[i] = Signature
                        .getInstance(GNSProtocol.SIGNATURE_ALGORITHM.toString());
            }
        } catch (NoSuchAlgorithmException e) {
            GNSConfig.getLogger().log(Level.SEVERE,
                    "Unable to initialize for authentication:{0}", e);
        }
    }

    static {
        for (int i = 0; i < mds.length; i++) {
            try {
                mds[i] = MessageDigest
                        .getInstance(GNSProtocol.DIGEST_ALGORITHM.toString());
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }

    static {
        for (int i = 0; i < ciphers.length; i++) {
            try {
                ciphers[i] = Cipher
                        .getInstance(GNSProtocol.SECRET_KEY_ALGORITHM.toString());
            } catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }
    private static synchronized Signature getSignatureInstance() {
      return signatureInstances[sigIndex++ % signatureInstances.length];
    }

    /**
     * Signs a digest of a message using private key of the given guid.
     *
     * @param guidEntry
     * @param message
     * @return a signed digest of the message string encoded as a hex string
     * @throws InvalidKeyException
     * @throws NoSuchAlgorithmException
     * @throws SignatureException
     * @throws java.io.UnsupportedEncodingException
     *
     * arun: This method need to be synchronized over the signature
     * instance, otherwise it will result in corrupted signatures.
     */
    public static String signDigestOfMessage(GuidEntry guidEntry,
            String message) throws ClientException {
        try {
            Signature signatureInstance = getSignatureInstance();
            synchronized (signatureInstance) {
                signatureInstance.initSign(guidEntry.getPrivateKey());
                // iOS client uses UTF-8 - should switch to ISO-8859-1 to be consistent with
                // secret key version
                signatureInstance.update(message.getBytes("UTF-8"));
                byte[] signedString = signatureInstance.sign();
                // We used to encode this as a hex so we could send it with the html without
                // encoding. Not really necessary anymore for the socket based client,
                // but the iOS client does as well so we need to keep it like this.
                // Also note that the secret based method doesn't do this - it just returns a string
                // using the ISO-8859-1 charset.
                String result = DatatypeConverter.printHexBinary(signedString);
                //String result = ByteUtils.toHex(signedString);
                return result;
            }
        } catch (InvalidKeyException | UnsupportedEncodingException | SignatureException e) {
            throw new ClientException ("Error encoding message", e);
        }
    }

    private static MessageDigest getMessageDigestInstance() {
      return mds[mdIndex++ % mds.length];
    }

    private static Cipher getCipherInstance() {
      return ciphers[cipherIndex++ % ciphers.length];
    }

    /**
     * @param guidEntry
     * @param message
     * @return Signature encoded as a hex string
     * @throws NoSuchAlgorithmException
     * @throws InvalidKeyException
     * @throws SignatureException
     * @throws UnsupportedEncodingException
     * @throws IllegalBlockSizeException
     * @throws BadPaddingException
     * @throws NoSuchPaddingException
     */
    public static String signDigestOfMessageSecretKey(GuidEntry guidEntry, String message)
            throws ClientException {
        try {
            SecretKey secretKey = SessionKeys.getOrGenerateSecretKey(guidEntry.getPublicKey(),
                    guidEntry.getPrivateKey());
            MessageDigest md = getMessageDigestInstance();
            byte[] digest;
            // FIXME: The reason why we use CHARSET should be more throughly documented here.
            // This might be important for folks writing clients in other languages.
            byte[] body = message.getBytes(GNSProtocol.CHARSET.toString());
            synchronized (md) {
                digest = md.digest(body);
            }
            assert (digest != null);
            Cipher cipher = getCipherInstance();
            byte[] signature;
            synchronized (cipher) {
                cipher.init(Cipher.ENCRYPT_MODE, secretKey);
                signature = cipher.doFinal(digest);
            }

            SessionKeys.SecretKeyCertificate skCert = SessionKeys
                    .getSecretKeyCertificate(guidEntry.getPublicKey());
            byte[] encodedSKCert = skCert.getEncoded(false);

            // arun: Combining them like this because the rest of the GNS code seems
            // poorly organized to add more signature related fields in a systematic
            // manner.
            byte[] combined = new byte[Short.BYTES + signature.length + Short.BYTES
                    + encodedSKCert.length];
            ByteBuffer.wrap(combined)
                    // signature
                    .putShort((short) signature.length).put(signature)
                    // certificate
                    .putShort((short) encodedSKCert.length).put(encodedSKCert);

            // FIXME: The reason why we use CHARSET should be more throughly documented here.
            return new String(combined, GNSProtocol.CHARSET.toString());
        } catch (NoSuchAlgorithmException | InvalidKeyException | SignatureException | NoSuchPaddingException | BadPaddingException | UnsupportedEncodingException | IllegalBlockSizeException e ) {
            throw new ClientException("Error encoding message message (using secretkey)", e);
        }
    }

	public static final PrivateKey getPrivateKey() throws KeyStoreException,
		NoSuchAlgorithmException, CertificateException, IOException,
		UnrecoverableKeyException {
		String keyStoreFile = System.getProperty("javax.net.ssl.keyStore");
		String keyStorePassword = System
			.getProperty("javax.net.ssl.keyStorePassword");
		FileInputStream is = new FileInputStream(keyStoreFile);

		KeyStore keystore = KeyStore.getInstance(KeyStore.getDefaultType());
		keystore.load(is, keyStorePassword.toCharArray());
		String alias = Config.getGlobalString(GNSConfig.GNSC.PRIVATE_KEY_ALIAS);
		Key key = keystore.getKey(alias, keyStorePassword.toCharArray());
		if (key instanceof PrivateKey) {
			return (PrivateKey) key;
		}
		return null;
	}

}
