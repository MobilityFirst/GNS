package edu.umass.cs.gnsclient.examples;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.client.InvalidGuidException;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.FileWriter;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;

/**
 * Created by Udit on 3/27/17.
 */
public class ClientWebACLExample {
    private static final String CONFIG_FILE = "examples/acl/config.js";
    // replace with your account alias
    private static String ACCOUNT_NAME_USER = "user@gns.name";
    private static String READER_ALIAS = "reader@gns.name";
    private static GNSClient client;
    private static GuidEntry GUID_USER;
    private static GuidEntry reader;

    /**
     * @param args
     * @throws IOException
     * @throws InvalidKeySpecException
     * @throws NoSuchAlgorithmException
     * @throws ClientException
     * @throws InvalidKeyException
     * @throws SignatureException
     * @throws Exception
     */
    public static void main(String[] args) throws IOException,
            InvalidKeySpecException, NoSuchAlgorithmException, ClientException,
            InvalidKeyException, SignatureException, JSONException {

        client = new GNSClientCommands();
        System.out.println("[Client connected to GNS]\n");

        try {
            // deleting the entries, any if created earlier

            System.out.println("// User GUID creation\n"
                    + "client.execute(" + ACCOUNT_NAME_USER + ")");
            try {
                client.execute(GNSCommand.createAccount(ACCOUNT_NAME_USER));
            } catch (Exception e) {
                System.out.println("Exception during accountGuid creation: " + e.getMessage());
            }
            GUID_USER = GuidUtils.getGUIDKeys(ACCOUNT_NAME_USER);

            // Create reader
            // First we create an alias for the reader
            // Create a sub guid under our guid account
            GuidEntry readerGuidEntry = null;
            try {
                readerGuidEntry = GuidUtils.getGUIDKeys(READER_ALIAS);
                if (readerGuidEntry == null) {
                    client.execute(GNSCommand.createGUID(GUID_USER, READER_ALIAS));
                }
            } catch (Exception e) {
                System.out.println("Exception during reader creation: " + e.getMessage());
            }
            // Get the GuidEntry from the local database
            reader = GuidUtils.getGUIDKeys(READER_ALIAS);

            System.out.println("\n// Create reader\n"
                    + "client.createGuid(guid, readerAlias) // readerAlias="
                    + READER_ALIAS);
        } catch (Exception | Error e) {
            System.out.println("Exception during accountGuid creation: " + e);
            e.printStackTrace();
            System.exit(1);
        }

        // Create a JSON Object to initialize our guid record
        JSONObject json = new JSONObject(
                "{\"name\":\"John Doe\",\"location\":\"Amherst\",\"type\":\"USER\"," +
                        "\"status\":\"EMPLOYEE\"," + "\"contact\":\"0123456789\"," +
                        "\"id\":\"ES_JD_123\"}");

        // Write out the JSON Object
        client.execute(GNSCommand.update(GUID_USER, json));
        System.out.println("\n// Update guid record\n"
                + "client.update(guid, record) // record=" + json);

        // Remove default read access from guid
        /*client.execute(GNSCommand.aclRemove(AclAccessType.READ_WHITELIST, GUID_USER,
                GNSProtocol.ENTIRE_RECORD.toString(),
                GNSProtocol.ALL_GUIDS.toString()));
        System.out
                .println("\n// Remove default read access from guid\n"
                        + "client.aclRemove(READ_WHITELIST, guid, ALL_FIELDS, ALL_GUIDS)");*/

        // Give reader read access to fields in guid
        // If we had not removed the default read access from guid this step
        // would be unnecessary
        client.execute(GNSCommand.aclAdd(AclAccessType.READ_WHITELIST, GUID_USER,
                "name", reader.getGuid()));

        client.execute(GNSCommand.aclAdd(AclAccessType.READ_WHITELIST, GUID_USER,
                "ID", reader.getGuid()));

        client.execute(GNSCommand.aclAdd(AclAccessType.READ_WHITELIST, GUID_USER,
                "location", reader.getGuid()));

        client.execute(GNSCommand.aclAdd(AclAccessType.READ_WHITELIST, GUID_USER,
                "type", reader.getGuid()));

        client.execute(GNSCommand.aclAdd(AclAccessType.READ_WHITELIST, GUID_USER,
                "status", reader.getGuid()));

        client.execute(GNSCommand.aclAdd(AclAccessType.READ_WHITELIST, GUID_USER,
                "contact", reader.getGuid()));
        JSONObject result = client.execute(
                GNSCommand.read(GUID_USER.getGuid(), reader))
                .getResultJSONObject();
        System.out
                .println("\n// Give reader read access to the fields : 'name' and 'ID' in guid " +
                        "and read guid entry as reader\n"
                        + "client.aclAdd(READ_WHITELIST, guid, ALL_FIELDS, reader)\n"
                        + "client.read(guid, reader) -> " + result);

        // Remove reader from ACL
        client.execute(GNSCommand.aclRemove(AclAccessType.READ_WHITELIST, GUID_USER,
                "location", reader.getGuid()));
        client.execute(GNSCommand.aclRemove(AclAccessType.READ_WHITELIST, GUID_USER,
                "type", reader.getGuid()));
        client.execute(GNSCommand.aclRemove(AclAccessType.READ_WHITELIST, GUID_USER,
                "status", reader.getGuid()));
        client.execute(GNSCommand.aclRemove(AclAccessType.READ_WHITELIST, GUID_USER,
                "contact", reader.getGuid()));

        System.out
                .println("\n// Remove reader from guid's read whitelists for the fields: " +
                        "location, type, status, contact \n"
                        + "client.aclRemove(READ_WHITELIST, guid, ALL_FIELDS, reader))\n"
                        + "client.aclRemove(WRITE_WHITELIST, guid, \"location\", reader);");

        System.out.println("ACCOUNT GUID:" + GUID_USER.getGuid());
        System.out.println("ACCOUNT GUID:" + GUID_USER.getPrivateKey());
        System.out.println("ACCOUNT GUID(str):" + GUID_USER.getPrivateKey().toString());
        System.out.println("READER GUID:" + reader.getGuid());


        System.out
                .println("\n// Example complete, gracefully closing the client\n"
                        + "client.close()");
        client.close();

        writeToConfigFile();

    }

    private static void writeToConfigFile() {
        org.apache.commons.codec.binary.Base64 base64 = new org.apache.commons.codec.binary.Base64(64);
        String key_reader = base64.encodeAsString(reader.getPrivateKey().getEncoded());
        String replace_reader = key_reader.replace("\r\n", "\\\r\n");

        String finalKey_reader = "-----BEGIN PRIVATE KEY-----\\\r\n" + replace_reader +
                "\\\r\n-----END PRIVATE KEY-----";

        String key_account = base64.encodeAsString(GUID_USER.getPrivateKey().getEncoded());
        String replace_account = key_account.replace("\r\n", "\\\r\n");

        String finalKey_account = "-----BEGIN PRIVATE KEY-----\\\r\n" + replace_account +
                "\\\r\n-----END PRIVATE KEY-----";

        String guid = GUID_USER.getGuid();

        String readerGuid = reader.getGuid();


        try (FileWriter file = new FileWriter(CONFIG_FILE)) {

            file.write("var reader_key = '" + finalKey_reader + "';");
            file.write("\nvar account_key = '" + finalKey_account + "';");
            file.write("\nvar guid = '" + guid + "';");
            file.write("\nvar reader = '" + readerGuid + "';");
            file.flush();

        } catch (IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }
}
