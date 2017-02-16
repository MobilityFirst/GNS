/*
 * Copyright (C) 2017
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsclient.client.singletests.failingtests;

import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.SharedGuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.utils.CanonicalJSON;
import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Util;
import java.io.UnsupportedEncodingException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author westy
 */
public class ByteificationComparisonFail extends DefaultGNSTest {
  
  private static JSONObject testJson;
  private final int TEST_RUNS = 1000000;
  //private static RequestPacket packet128;
  //private static RequestPacket packet1024;
  
  /**
   *
   * @throws JSONException
   */
  @BeforeClass
  public static void setupBeforClass() throws JSONException {

    //packet128 = new RequestPacket(new String(Util.getRandomAlphanumericBytes(128)), false);
    //packet1024 = new RequestPacket(new String(Util.getRandomAlphanumericBytes(1024)), false);
    //json128=packet128.toJSONObject();
    //json1024=packet1024.toJSONObject();

    Collection<String> collection1 = new ArrayList<>();
    Collection<String> collection2 = new ArrayList<>();
    Collection<String> collection3 = new ArrayList<>();
    Collection<Collection<String>> recursiveCollection = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      collection1.add(new String(Util.getRandomAlphanumericBytes(64)));
    }
    for (int i = 0; i < 3; i++) {
      collection2.add(new String(Util.getRandomAlphanumericBytes(64)));
    }
    for (int i = 0; i < 7; i++) {
      collection3.add(new String(Util.getRandomAlphanumericBytes(32)));
    }
    recursiveCollection.add(collection1);
    recursiveCollection.add(collection2);
    testJson = new JSONObject();
    testJson.put("collection", recursiveCollection);
    testJson.put("otherCollection", collection3);

  }

  /**
   *
   * @param byteificationComparison
   * @throws UnsupportedEncodingException
   * @throws JSONException
   * @throws ClientException
   * @throws RequestParseException
   */
  // FIXME: THIS TEST IS FAILING at new CommandPacket(bytes)
  @Test
  public void test_19_CommandPacket_1024B(ByteificationComparisonFail byteificationComparison) throws UnsupportedEncodingException, JSONException, ClientException, RequestParseException {
    CommandPacket packet = GNSCommand.fieldRead(new String(Util.getRandomAlphanumericBytes(512)), new String(Util.getRandomAlphanumericBytes(512)), null);
    //CommandPacket packet = new CommandPacket(CommandUtils.createCommand(CommandType.ReadArrayOneUnsigned, "", GNSProtocol.GUID.toString(), new String(Util.getRandomAlphanumericBytes(512)), GNSProtocol.FIELD.toString(),new String(Util.getRandomAlphanumericBytes(512))));
    long startTime = System.nanoTime();
    for (int i = 0; i < TEST_RUNS; i++) {
      byte[] bytes = packet.toBytes();
      new CommandPacket(bytes);
    }
    long endTime = System.nanoTime();
    double avg = (endTime - startTime) / (TEST_RUNS);
    System.out.println("Average byteification time CommandPacket 1024B was " + avg + " nanoseconds.");
    byte[] bytes = packet.toBytes();
    CommandPacket outputPacket = new CommandPacket(bytes);
    assert (Arrays.equals(bytes, outputPacket.toBytes()));
    //assert(packet.toJSONObject().toString().equals(outputPacket.toJSONObject().toString()));
  }

  /**
   *
   * @param byteificationComparison
   * @throws UnsupportedEncodingException
   * @throws JSONException
   * @throws ClientException
   * @throws RequestParseException
   */
  // FIXME: THIS TEST IS FAILING at new CommandPacket(bytes)
  @Test
  public void test_18_CommandPacket_128B(ByteificationComparisonFail byteificationComparison) throws UnsupportedEncodingException, JSONException, ClientException, RequestParseException {
    CommandPacket packet = GNSCommand.fieldRead(new String(Util.getRandomAlphanumericBytes(64)), new String(Util.getRandomAlphanumericBytes(64)), null);
    //CommandPacket packet = new CommandPacket(CommandUtils.createCommand(CommandType.ReadArrayOneUnsigned, "", GNSProtocol.GUID.toString(), new String(Util.getRandomAlphanumericBytes(64)), GNSProtocol.FIELD.toString(),new String(Util.getRandomAlphanumericBytes(64))));
    long startTime = System.nanoTime();
    for (int i = 0; i < TEST_RUNS; i++) {
      byte[] bytes = packet.toBytes();
      new CommandPacket(bytes);
    }
    long endTime = System.nanoTime();
    double avg = (endTime - startTime) / (TEST_RUNS);
    System.out.println("Average byteification time CommandPacket 128B was " + avg + " nanoseconds.");
    byte[] bytes = packet.toBytes();
    CommandPacket outputPacket = new CommandPacket(bytes);
    //assert(packet.toJSONObject().toString().equals(outputPacket.toJSONObject().toString()));
    assert (Arrays.equals(bytes, outputPacket.toBytes()));
    //System.out.println(packet.toJSONObject().toString());
    //System.out.println(outputPacket.toJSONObject().toString());
  }

  /**
   *
   * @param byteificationComparison
   * @throws UnsupportedEncodingException
   * @throws JSONException
   * @throws ClientException
   * @throws NoSuchAlgorithmException
   * @throws RequestParseException
   */
  // FIXME: THIS TEST IS FAILING at new CommandPacket(bytes)
  @Test
  public void test_201_FromCommandPacket_128B_Signed(ByteificationComparisonFail byteificationComparison) throws UnsupportedEncodingException, JSONException, ClientException, NoSuchAlgorithmException, RequestParseException {
    KeyPair keyPair = KeyPairGenerator.getInstance(GNSProtocol.RSA_ALGORITHM.toString()).generateKeyPair();
    String guid = SharedGuidUtils.createGuidStringFromPublicKey(keyPair.getPublic().getEncoded());
    // Squirrel this away now just in case the call below times out.
    KeyPairUtils.saveKeyPair("gnsname", "alias", guid, keyPair);
    GuidEntry querier = new GuidEntry("alias", guid, keyPair.getPublic(), keyPair.getPrivate());
    CommandPacket packet = GNSCommand.fieldUpdate(querier, new String(Util.getRandomAlphanumericBytes(64)), new String(Util.getRandomAlphanumericBytes(64)));
    String jsonBefore = packet.toJSONObject().toString();
    byte[] bytes = packet.toBytes();
    assert (jsonBefore.equals(packet.toJSONObject().toString()));
    long startTime = System.nanoTime();
    for (int i = 0; i < TEST_RUNS; i++) {
      new CommandPacket(bytes);
    }
    long endTime = System.nanoTime();
    double avg = (endTime - startTime) / (TEST_RUNS);
    CommandPacket outputPacket = new CommandPacket(bytes);
    System.out.println("Average time CommandPacket from bytes 128B Signed was " + avg + " nanoseconds.");
    assert (Arrays.equals(bytes, outputPacket.toBytes()));
    String canonicalJSON = CanonicalJSON.getCanonicalForm(jsonBefore);
    String canonicalJSONOutput = CanonicalJSON.getCanonicalForm(outputPacket.toJSONObject());
    //System.out.println(canonicalJSON);
    //System.out.println(canonicalJSONOutput);
    assert (canonicalJSON.equals(canonicalJSONOutput));
    //CommandPacket outputPacket = CommandPacket.fromBytes(bytes);
    //assert(packet.toJSONObject().toString().equals(outputPacket.toJSONObject().toString()));
  }

  /**
   *
   * @param byteificationComparison
   * @throws UnsupportedEncodingException
   * @throws JSONException
   * @throws ClientException
   * @throws NoSuchAlgorithmException
   * @throws RequestParseException
   */
  // FIXME: THIS TEST IS FAILING at new CommandPacket(bytes)
  @Test
  public void test_20_FromCommandPacket_128B(ByteificationComparisonFail byteificationComparison) throws UnsupportedEncodingException, JSONException, ClientException, NoSuchAlgorithmException, RequestParseException {
    GuidEntry querier = null;
    CommandPacket packet = GNSCommand.fieldRead(new String(Util.getRandomAlphanumericBytes(64)), new String(Util.getRandomAlphanumericBytes(64)), querier);
    //CommandPacket packet = new CommandPacket(CommandUtils.createCommand(CommandType.ReadArrayOneUnsigned, "", GNSProtocol.GUID.toString(), new String(Util.getRandomAlphanumericBytes(512)), GNSProtocol.FIELD.toString(),new String(Util.getRandomAlphanumericBytes(512))));
    String jsonBefore = packet.toJSONObject().toString();
    byte[] bytes = packet.toBytes();
    //System.out.println(jsonBefore + "\n\n" + packet.toJSONObject().toString());
    assert (jsonBefore.equals(packet.toJSONObject().toString()));
    long startTime = System.nanoTime();
    for (int i = 0; i < TEST_RUNS; i++) {
      new CommandPacket(bytes);
    }
    long endTime = System.nanoTime();
    double avg = (endTime - startTime) / (TEST_RUNS);
    CommandPacket outputPacket = new CommandPacket(bytes);
    System.out.println("Average time CommandPacket from bytes 128B unsigned was " + avg + " nanoseconds.");
    assert (Arrays.equals(bytes, outputPacket.toBytes()));
    String canonicalJSON = CanonicalJSON.getCanonicalForm(jsonBefore);
    String canonicalJSONOutput = CanonicalJSON.getCanonicalForm(outputPacket.toJSONObject());
    //System.out.println(canonicalJSON);
    //System.out.println(canonicalJSONOutput);
    assert (canonicalJSON.equals(canonicalJSONOutput));
    //CommandPacket outputPacket = CommandPacket.fromBytes(bytes);
    //assert(packet.toJSONObject().toString().equals(outputPacket.toJSONObject().toString()));
  }

  /**
   *
   * @param byteificationComparison
   * @throws UnsupportedEncodingException
   * @throws JSONException
   * @throws ClientException
   * @throws NoSuchAlgorithmException
   * @throws RequestParseException
   */
  // FIXME: THIS TEST IS FAILING at new CommandPacket(bytes)
  @Test
  public void test_21_FromCommandPacket_1024B(ByteificationComparisonFail byteificationComparison) throws UnsupportedEncodingException, JSONException, ClientException, NoSuchAlgorithmException, RequestParseException {
    CommandPacket packet = GNSCommand.fieldRead(new String(Util.getRandomAlphanumericBytes(512)), new String(Util.getRandomAlphanumericBytes(512)), null);
    //CommandPacket packet = new CommandPacket(CommandUtils.createCommand(CommandType.ReadArrayOneUnsigned, "", GNSProtocol.GUID.toString(), new String(Util.getRandomAlphanumericBytes(512)), GNSProtocol.FIELD.toString(),new String(Util.getRandomAlphanumericBytes(512))));
    byte[] bytes = packet.toBytes();
    long startTime = System.nanoTime();
    for (int i = 0; i < TEST_RUNS; i++) {
      new CommandPacket(bytes);
    }
    long endTime = System.nanoTime();
    double avg = (endTime - startTime) / (TEST_RUNS);
    CommandPacket outputPacket = new CommandPacket(bytes);
    System.out.println("Average time CommandPacket from bytes 1024B was " + avg + " nanoseconds.");
    assert (Arrays.equals(bytes, outputPacket.toBytes()));
    //CommandPacket outputPacket = CommandPacket.fromBytes(bytes);
    //assert(packet.toJSONObject().toString().equals(outputPacket.toJSONObject().toString()));
  }
  
}
