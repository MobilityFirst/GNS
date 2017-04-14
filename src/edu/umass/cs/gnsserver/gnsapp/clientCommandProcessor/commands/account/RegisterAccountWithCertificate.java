/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account;

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountAccess;
import edu.umass.cs.gnscommon.SharedGuidUtils;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnsserver.gnsapp.clientSupport.NSAccessSupport;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.*;
import java.security.cert.*;
import java.security.spec.InvalidKeySpecException;

import edu.umass.cs.reconfiguration.Reconfigurator;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.utils.Config;

import java.util.*;
import java.security.cert.X509Certificate;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author tramteja
 */
public class RegisterAccountWithCertificate extends AbstractCommand {

    /**
     * Creates a RegisterAccount instance.
     *
     * @param module
     */
    public static final Logger LOG = Logger.getLogger(Reconfigurator.class.getName());

    public RegisterAccountWithCertificate(CommandModule module) {
        super(module);
    }

    /**
     *
     * @return the command type
     */
    @Override
    public CommandType getCommandType() {
        return CommandType.RegisterAccountWithCertificate;
    }

    /**
     * check if certificate is self signed
     *
     * @param cert candidate certificate for checking
     * @return true/false
     * @throws GeneralSecurityException general security exception
     */

    private Boolean checkSelfSigned(X509Certificate cert) throws GeneralSecurityException {
        try {
            // Try to verify certificate signature with its own public key
            PublicKey key = cert.getPublicKey();
            cert.verify(key);
            return true;
        } catch (SignatureException | InvalidKeyException e) {
            // Invalid signature/key --> not self-signed
            return false;
        }
    }

    /**
     * Helper function to load certificates from trust store
     * @return trustedCerts
     * @throws IOException if trust file is not found
     * @throws GeneralSecurityException general security exception
     */

    private Set<X509Certificate> loadCertificatesFromTrustStore() throws IOException, GeneralSecurityException {
        char[] trustStorePassword = System.getProperty("javax.net.ssl.trustStorePassword").toCharArray();
        FileInputStream tsInputStream = new FileInputStream(System.getProperty("javax.net.ssl.trustStore"));

        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(tsInputStream, trustStorePassword);

        //Retrieve trusted CAs from the trust store
        PKIXParameters params = new PKIXParameters(trustStore);

        Iterator<TrustAnchor> it = params.getTrustAnchors().iterator();
        Set<X509Certificate> trustedCerts = new HashSet<X509Certificate>();
        while (it.hasNext()) {
            TrustAnchor ta = it.next();
            trustedCerts.add(ta.getTrustedCert());
        }

        return trustedCerts;
    }

    /**
     * helper function to construct the chain using trsuter certs and intermediate
     * return true if we are able to construct path else returns false
     *
     * @param cert candidate for building path
     * @param trustedRootCerts set of trusted roots
     * @param intermediateCerts set of intermediate roots
     * @return 0/1
     * @throws GeneralSecurityException general security exception
     */

    private static Boolean verifyCertificatePath(X509Certificate cert, Set<X509Certificate> trustedRootCerts,
                                                 Set<X509Certificate> intermediateCerts) throws GeneralSecurityException {

        // Create the selector that specifies the starting certificate
        X509CertSelector selector = new X509CertSelector();
        selector.setCertificate(cert);

        // Create the trust anchors (set of root CA certificates)
        Set<TrustAnchor> trustAnchors = new HashSet<TrustAnchor>();
        for (X509Certificate trustedRootCert : trustedRootCerts) {
            trustAnchors.add(new TrustAnchor(trustedRootCert, null));
        }

        // Configure the PKIX certificate builder algorithm parameters
        PKIXBuilderParameters pkixParams = new PKIXBuilderParameters(trustAnchors, selector);

        // Disable CRL checks since certificate currently generated do not have crl distribution points
        // TODO: Add support for crls
        pkixParams.setRevocationEnabled(false);

        // Specify a list of intermediate certificates
        CertStore intermediateCertStore = CertStore.getInstance("Collection",
                new CollectionCertStoreParameters(intermediateCerts));
        pkixParams.addCertStore(intermediateCertStore);

        // Build and verify the certification chain
        CertPathBuilder builder = CertPathBuilder.getInstance("PKIX");

        try {
            PKIXCertPathBuilderResult result = (PKIXCertPathBuilderResult) builder.build(pkixParams);
            // since we are able to build  valid path return true
            return true;
        } catch (CertPathBuilderException | InvalidAlgorithmParameterException e) {
            // unable to build certificate path return false
            LOG.log(Level.FINEST, "Exception occurred while building the certificate chain " + e.toString());
            return false;
        }
    }

    /**
     * Function to verify trust parameters of the certificate
     *
     * @param cert candidate certificate for verification
     * @return 0/1
     * @throws IOException if keystore file is not found
     * @throws GeneralSecurityException general security sxception
     */
    private Boolean verify_certificate_trust(X509Certificate cert) throws IOException, GeneralSecurityException {
        // check if certificate is self signed if yes return false

        if (checkSelfSigned(cert)) {
            LOG.log(Level.FINEST, "Given certificate was self signed");
            return false;
        }

        // Get handle to current keystore
        // TODO: currently it reads truststore directly from file system, store truststore object and use it on demand

        Set<X509Certificate> trustedCerts = loadCertificatesFromTrustStore();
        // Prepare a set of trusted root CA certificates
        // and a set of intermediate certificates
        Set<X509Certificate> trustedRootCerts = new HashSet<X509Certificate>();
        Set<X509Certificate> intermediateCerts = new HashSet<X509Certificate>();

        for (X509Certificate additionalCert : trustedCerts) {
            // if a certificate is in trustStore and selfsigned then it is trusted root or else intermediate
            if (checkSelfSigned(additionalCert)) {
                trustedRootCerts.add(additionalCert);
            } else {
                intermediateCerts.add(additionalCert);
            }
        }

        // try to construct a valid certificate path, if unable to construct return false
        return verifyCertificatePath(cert, trustedRootCerts, intermediateCerts);

    }

    /**
     *  Function to verify validity of the certificate
     *
     *  @param cert candidate certificate for verification
     *
     *  @return 0/1
     */

    private Boolean verify_certificate_validity(X509Certificate cert) {
        try {
            cert.checkValidity();
            return true;
        } catch (CertificateExpiredException | CertificateNotYetValidException e) {
            return false;
        }
    }

    /**
     * Function to verify name given  with common name in the certificate
     *
     * @param cert
     * @param name
     *
     * @return 0/1
     */
    private Boolean verify_certificate_name(String name, X509Certificate cert) {
        String name_in_certificate = SharedGuidUtils.getNameFromCertificate(cert);
        return name_in_certificate.equals(name);
    }

    @Override
    public CommandResponse execute(InternalRequestHeader header, CommandPacket commandPacket,
                                   ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException, JSONException,
            NoSuchAlgorithmException, SignatureException, UnsupportedEncodingException, InternalRequestException {

        LOG.log(Level.FINEST, "Command received for certificate based account registration");
        JSONObject json = commandPacket.getCommand();
        String name = json.getString(GNSProtocol.NAME.toString());
        String certificate_encoded = json.getString(GNSProtocol.CERTIFICATE.toString());
        String password = json.getString(GNSProtocol.PASSWORD.toString());
        String signature = json.getString(GNSProtocol.SIGNATURE.toString());
        String message = json.getString(GNSProtocol.SIGNATUREFULLMESSAGE.toString());

        try {

            // Make  certificate object from stream obtained
            X509Certificate cert = SharedGuidUtils.getCertificateFromString(certificate_encoded);
            PublicKey publicKey = SharedGuidUtils.getPublicKeyFromCertificate(cert);
            String publicKeyString = SharedGuidUtils.getPublicKeyString(publicKey);

            String guid = SharedGuidUtils.createGuidStringFromBase64PublicKey(publicKeyString);

            if (!NSAccessSupport.verifySignature(publicKeyString, signature, message)) {
                return new CommandResponse(ResponseCode.SIGNATURE_ERROR,
                        GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.BAD_SIGNATURE.toString());
            }

            if (!verify_certificate_name(name, cert)) {
                LOG.log(Level.FINEST, "Name mismatch in the certificate");
                return new CommandResponse(ResponseCode.NAME_MISMATCH_CERTIFICATE, GNSProtocol.NAME_MISMATCH_ERROR.toString());
            }

            if (!verify_certificate_trust(cert)) {
                LOG.log(Level.FINEST, "Unable to build certificate chain");
                return new CommandResponse(ResponseCode.TRUST_INVALID_CERTIFICATE, GNSProtocol.TRUST_INVALID_ERROR.toString());
            }

            if (!verify_certificate_validity(cert)) {
                LOG.log(Level.FINEST, "Certificate has been expired or not yet valid");
                return new CommandResponse(ResponseCode.TIME_INVALID_CERTIFICATE, GNSProtocol.TIME_INVALID_ERROR.toString());
            }

            CommandResponse result = AccountAccess.addAccount(header, commandPacket, handler.getHttpServerHostPortString(),
                    name, guid, publicKeyString, password, Config.getGlobalBoolean(GNSConfig.GNSC.ENABLE_EMAIL_VERIFICATION),
                    handler);
            if (result.getExceptionOrErrorCode().isOKResult()) {
                // Everything is hunkey dorey so return the new guid
                return new CommandResponse(ResponseCode.NO_ERROR, guid);
            } else {
                assert (result.getExceptionOrErrorCode() != null);
                // Otherwise return the error response.
                return result;
            }
        } catch (ClientException | IOException | GeneralSecurityException e) {
            return new CommandResponse(ResponseCode.UNSPECIFIED_ERROR,
                    GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.UNSPECIFIED_ERROR.toString() + " " + e.getMessage());
        }
    }

}
