/*
 *
 *  Copyright (c) 2017 University of Massachusetts
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
 *  Initial developer(s): tramteja
 *
 */
package edu.umass.cs.gnsclient.client.singletests.simple;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.utils.Utils;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.IOException;

/**
 * Basic test for the GNS using the UniversalTcpClient.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CreateAccountWithCertificate extends DefaultGNSTest {

    private static GNSClient gnsClient;
    private static final String TEST_ACCOUNT_ALIAS = "firsttest";
    private static final String TEST_ACCOUNT_PASSWORD = "password";
    private static GuidEntry testGuid;
    private static GuidEntry myAccountGUID = null;

    /**
     *
     */
    public CreateAccountWithCertificate() {
        if (gnsClient == null) {
            try {
                gnsClient = new GNSClient();
                gnsClient.setForceCoordinatedReads(true);
            } catch (IOException e) {
                Utils.failWithStackTrace("Exception creating client: " + e);
            }
        }
    }

    /**
     *
     */
    @Test
    public void test_01_CreateAccount() {
        try {
            String certificateFileName = "/home/tramteja/work/gns_source/GNS/certificate_firsttest.crt";
            String privateKeyFileName = "/home/tramteja/work/gns_source/GNS/final_key.pem";
            testGuid = GuidUtils.lookupOrCreateAccountGuidWithCertificate(gnsClient, certificateFileName,
                    privateKeyFileName, "password", true);

            KeyPairUtils.removeKeyPair(GNSClient.getGNSProvider(), "firsttest");

            testGuid = GuidUtils.lookupOrCreateAccountGuidWithCertificate(gnsClient, certificateFileName,
                    privateKeyFileName, "password", true);
            //testGuid = GuidUtils.accountGuidCreateWithCertificate(gnsClient, TEST_ACCOUNT_PASSWORD,certificateFileName, privateKeyFileName );
        } catch (Exception e) {
            Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
        }
    }

    /**
     *
     */
    @Test
    public void test_02_CheckAccount() {
        String guidString = null;
        try {
            myAccountGUID = GuidUtils.getGUIDKeys(TEST_ACCOUNT_ALIAS);
            Assert.assertEquals(testGuid,myAccountGUID);
        } catch (Exception e) {
            Utils.failWithStackTrace("Exception while looking up guid: " + e);
        }
    }

    /**
     *
     */
    @Test
    public void test_03_CheckAccountCleanup() {
        try {
            System.out.println("Removing my account GUID " + myAccountGUID);
            client.execute(GNSCommand.accountGuidRemove(myAccountGUID));
        } catch (ClientException | IOException e) {
            Utils.failWithStackTrace("Exception while removing test account guid: " + e);
        }
    }

}
