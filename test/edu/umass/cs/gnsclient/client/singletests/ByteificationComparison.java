package edu.umass.cs.gnsclient.client.singletests;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;

import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.packets.ResponsePacket;
import edu.umass.cs.gnscommon.utils.JSONByteConverter;
import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.utils.Util;

import org.junit.runners.MethodSorters;

/**
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ByteificationComparison extends DefaultGNSTest {

  private static JSONObject testJson;
  private final int TEST_RUNS = 1000000;
  private static RequestPacket packet128;
  private static RequestPacket packet1024;

  /**
   *
   * @throws JSONException
   */
  @BeforeClass
  public static void setupBeforClass() throws JSONException {

    packet128 = new RequestPacket(new String(Util.getRandomAlphanumericBytes(128)), false);
    packet1024 = new RequestPacket(new String(Util.getRandomAlphanumericBytes(1024)), false);
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
   * @throws JSONException
   */
  @Test
  public void test_01_JSON_Default() throws JSONException {
    long startTime = System.nanoTime();
    for (int i = 0; i < TEST_RUNS; i++) {
      byte[] bytes = testJson.toString().getBytes();
      new JSONObject(new String(bytes));
      //assert(testJson.equals(testJson2));
    }
    long endTime = System.nanoTime();
    double avg = (endTime - startTime) / (TEST_RUNS);
    System.out.println("Average byteification time JSON_Default Contrived was " + avg + " nanoseconds.");

    byte[] bytes = testJson.toString().getBytes();
    JSONObject testJson2 = new JSONObject(new String(bytes));
    assert (testJson.toString().equals(testJson2.toString()));
    //System.out.println("JSON1: \n" + testJson.toString());
    //System.out.println("JSON2: \n" + testJson2.toString());

  }

  /**
   *
   * @throws JSONException
   * @throws IOException
   */
  @Test
  public void test_02_hardcoded() throws JSONException, IOException {
    long startTime = System.nanoTime();
    for (int i = 0; i < TEST_RUNS; i++) {
      JSONByteConverter.toBytesHardcoded(testJson);
      //JSONObject testJson2 = JSONByteConverter.fromBytesHardcoded(bytes);
      //assert(testJson.equals(testJson2));
    }
    long endTime = System.nanoTime();
    double avg = (endTime - startTime) / (TEST_RUNS);
    System.out.println("Average byteification time HARDCODED Contrived was " + avg + " nanoseconds.");

    byte[] bytes = JSONByteConverter.toBytesHardcoded(testJson);
    JSONObject testJson2 = JSONByteConverter.fromBytesHardcoded(bytes);
    assert (testJson.toString().equals(testJson2.toString()));
    //System.out.println("JSON1: \n" + testJson.toString());
    //System.out.println("JSON2: \n" + testJson2.toString());
  }

  /**
   *
   * @throws IOException
   * @throws JSONException
   */
  @Test
  public void test_03_jackson() throws IOException, JSONException {
    long startTime = System.nanoTime();
    for (int i = 0; i < TEST_RUNS; i++) {
      byte[] bytes = JSONByteConverter.toBytesJackson(testJson);
      JSONByteConverter.fromBytesJackson(bytes);
      //assert(testJson.equals(testJson2));
    }
    long endTime = System.nanoTime();
    double avg = (endTime - startTime) / (TEST_RUNS);
    System.out.println("Average byteification time Jackson Contrived was " + avg + " nanoseconds.");
    byte[] bytes = JSONByteConverter.toBytesJackson(testJson);
    JSONByteConverter.fromBytesJackson(bytes);

    //System.out.println("JSON1: \n" + testJson.toString());
    //System.out.println("JSON2: \n" + testJson2.toString());
    //assert(testJson.toString().equals(testJson2.toString()));
  }

  /**
   *
   * @throws IOException
   * @throws JSONException
   */
  @Test
  public void test_04_msgpack() throws IOException, JSONException {
    long startTime = System.nanoTime();
    JSONObject testJson2;
    for (int i = 0; i < TEST_RUNS; i++) {
      byte[] bytes = JSONByteConverter.toBytesMsgpack(testJson);
      JSONByteConverter.fromBytesMsgpack(bytes);
      //assert(testJson.equals(testJson2));
    }
    long endTime = System.nanoTime();
    double avg = (endTime - startTime) / (TEST_RUNS);
    System.out.println("Average byteification time Msgpack Contrived was " + avg + " nanoseconds.");
    byte[] bytes = JSONByteConverter.toBytesMsgpack(testJson);
    testJson2 = JSONByteConverter.fromBytesMsgpack(bytes);

    //System.out.println("JSON1: \n" + testJson.toString());
    //System.out.println("JSON2: \n" + testJson2.toString());
    assert (testJson.toString().equals(testJson2.toString()));
  }

  /**
   *
   * @throws JSONException
   * @throws IOException
   */
  @Test
  public void test_05_hardcoded_request_128() throws JSONException, IOException {
    long startTime = System.nanoTime();
    for (int i = 0; i < TEST_RUNS; i++) {
      JSONObject json128 = packet128.toJSONObject();
      byte[] bytes = JSONByteConverter.toBytesHardcoded(json128);
      JSONByteConverter.fromBytesHardcoded(bytes);
      //assert(testJson.equals(testJson2));
    }
    long endTime = System.nanoTime();
    double avg = (endTime - startTime) / (TEST_RUNS);
    System.out.println("Average byteification time HARDCODED 128B was " + avg + " nanoseconds.");
    JSONObject json128 = packet128.toJSONObject();
    byte[] bytes = JSONByteConverter.toBytesHardcoded(json128);
    JSONObject testJson2 = JSONByteConverter.fromBytesHardcoded(bytes);
    assert (json128.toString().equals(testJson2.toString()));
    //System.out.println("JSON1: \n" + testJson.toString());
    //System.out.println("JSON2: \n" + testJson2.toString());
  }

  /**
   *
   * @throws JSONException
   * @throws IOException
   */
  @Test
  public void test_06_jackson_request_128() throws JSONException, IOException {
    long startTime = System.nanoTime();
    for (int i = 0; i < TEST_RUNS; i++) {
      JSONObject json128 = packet128.toJSONObject();
      byte[] bytes = JSONByteConverter.toBytesJackson(json128);
      JSONByteConverter.fromBytesJackson(bytes);
      //assert(testJson.equals(testJson2));
    }
    long endTime = System.nanoTime();
    double avg = (endTime - startTime) / (TEST_RUNS);
    System.out.println("Average byteification time Jackson 128B was " + avg + " nanoseconds.");
    JSONObject json128 = packet128.toJSONObject();
    byte[] bytes = JSONByteConverter.toBytesJackson(json128);
    JSONByteConverter.fromBytesJackson(bytes);
    //assert(json128.toString().equals(testJson2.toString()));
    //System.out.println("JSON1: \n" + testJson.toString());
    //System.out.println("JSON2: \n" + testJson2.toString());
  }

  /**
   *
   * @throws JSONException
   */
  @Test
  public void test_07_JSON_Default_128() throws JSONException {
    long startTime = System.nanoTime();
    for (int i = 0; i < TEST_RUNS; i++) {
      JSONObject json128 = packet128.toJSONObject();
      byte[] bytes = json128.toString().getBytes();
      new JSONObject(new String(bytes));
      //assert(testJson.equals(testJson2));
    }
    long endTime = System.nanoTime();
    double avg = (endTime - startTime) / (TEST_RUNS);
    System.out.println("Average byteification time JSON_Default 128B was " + avg + " nanoseconds.");
    JSONObject json128 = packet128.toJSONObject();
    byte[] bytes = json128.toString().getBytes();
    JSONObject testJson2 = new JSONObject(new String(bytes));
    assert (json128.toString().equals(testJson2.toString()));
    //System.out.println("JSON1: \n" + testJson.toString());
    //System.out.println("JSON2: \n" + testJson2.toString());

  }

  /**
   *
   * @throws JSONException
   * @throws IOException
   */
  @Test
  public void test_071_msgpack_request_128() throws JSONException, IOException {
    long startTime = System.nanoTime();
    for (int i = 0; i < TEST_RUNS; i++) {
      JSONObject json128 = packet128.toJSONObject();
      byte[] bytes = JSONByteConverter.toBytesMsgpack(json128);
      JSONByteConverter.fromBytesMsgpack(bytes);
      //assert(testJson.equals(testJson2));
    }
    long endTime = System.nanoTime();
    double avg = (endTime - startTime) / (TEST_RUNS);
    System.out.println("Average byteification time Msgpack 128B was " + avg + " nanoseconds.");
    JSONObject json128 = packet128.toJSONObject();
    byte[] bytes = JSONByteConverter.toBytesMsgpack(json128);
    JSONObject testJson2 = JSONByteConverter.fromBytesMsgpack(bytes);
    assert (json128.toString().equals(testJson2.toString()));
    //System.out.println("JSON1: \n" + testJson.toString());
    //System.out.println("JSON2: \n" + testJson2.toString());
  }

  /**
   *
   * @throws JSONException
   * @throws UnsupportedEncodingException
   * @throws UnknownHostException
   */
  @Test
  public void test_08_PacketToBytes_128() throws JSONException, UnsupportedEncodingException, UnknownHostException {
    long startTime = System.nanoTime();
    for (int i = 0; i < TEST_RUNS; i++) {
      byte[] bytes = packet128.toBytesInstrument();
      new RequestPacket(bytes);
      //assert(testJson.equals(testJson2));
    }
    long endTime = System.nanoTime();
    double avg = (endTime - startTime) / (TEST_RUNS);
    System.out.println("Average byteification time PacketToBytes 128B was " + avg + " nanoseconds.");
    byte[] bytes = packet128.toBytesInstrument();
    RequestPacket packet = new RequestPacket(bytes);
    JSONObject testJson2 = packet.toJSONObject();
    assert (packet128.toJSONObject().toString().equals(testJson2.toString()));
    //System.out.println("JSON1: \n" + testJson.toString());
    //System.out.println("JSON2: \n" + testJson2.toString());

  }

  /**
   *
   * @throws JSONException
   * @throws IOException
   */
  @Test
  public void test_09_hardcoded_request_1024() throws JSONException, IOException {
    long startTime = System.nanoTime();
    for (int i = 0; i < TEST_RUNS; i++) {
      JSONObject json1024 = packet1024.toJSONObject();
      byte[] bytes = JSONByteConverter.toBytesHardcoded(json1024);
      JSONByteConverter.fromBytesHardcoded(bytes);
      //assert(testJson.equals(testJson2));
    }
    long endTime = System.nanoTime();
    double avg = (endTime - startTime) / (TEST_RUNS);
    System.out.println("Average byteification time HARDCODED 1024B was " + avg + " nanoseconds.");
    JSONObject json1024 = packet1024.toJSONObject();
    byte[] bytes = JSONByteConverter.toBytesHardcoded(json1024);
    JSONObject testJson2 = JSONByteConverter.fromBytesHardcoded(bytes);
    assert (json1024.toString().equals(testJson2.toString()));
    //System.out.println("JSON1: \n" + testJson.toString());
    //System.out.println("JSON2: \n" + testJson2.toString());
  }

  /**
   *
   * @throws JSONException
   * @throws IOException
   */
  @Test
  public void test_10_jackson_request_1024() throws JSONException, IOException {
    long startTime = System.nanoTime();
    for (int i = 0; i < TEST_RUNS; i++) {
      JSONObject json1024 = packet1024.toJSONObject();
      byte[] bytes = JSONByteConverter.toBytesJackson(json1024);
      JSONByteConverter.fromBytesJackson(bytes);
      //assert(testJson.equals(testJson2));
    }
    long endTime = System.nanoTime();
    double avg = (endTime - startTime) / (TEST_RUNS);
    System.out.println("Average byteification time Jackson 1024B was " + avg + " nanoseconds.");
    JSONObject json1024 = packet1024.toJSONObject();
    byte[] bytes = JSONByteConverter.toBytesJackson(json1024);
    JSONByteConverter.fromBytesJackson(bytes);
    //assert(json1024.toString().equals(testJson2.toString()));
    //System.out.println("JSON1: \n" + testJson.toString());
    //System.out.println("JSON2: \n" + testJson2.toString());
  }

  /**
   *
   * @throws JSONException
   */
  @Test
  public void test_11_JSON_Default_1024() throws JSONException {
    long startTime = System.nanoTime();
    for (int i = 0; i < TEST_RUNS; i++) {
      JSONObject json1024 = packet1024.toJSONObject();
      byte[] bytes = json1024.toString().getBytes();
      new JSONObject(new String(bytes));
      //assert(testJson.equals(testJson2));
    }
    long endTime = System.nanoTime();
    double avg = (endTime - startTime) / (TEST_RUNS);
    System.out.println("Average byteification time JSON_Default 1024B was " + avg + " nanoseconds.");
    JSONObject json1024 = packet1024.toJSONObject();
    byte[] bytes = json1024.toString().getBytes();
    JSONObject testJson2 = new JSONObject(new String(bytes));
    assert (json1024.toString().equals(testJson2.toString()));
    //System.out.println("JSON1: \n" + testJson.toString());
    //System.out.println("JSON2: \n" + testJson2.toString());

  }

  /**
   *
   * @throws JSONException
   * @throws UnsupportedEncodingException
   * @throws UnknownHostException
   */
  @Test
  public void test_12_PacketToBytes_1024() throws JSONException, UnsupportedEncodingException, UnknownHostException {
    long startTime = System.nanoTime();
    for (int i = 0; i < TEST_RUNS; i++) {
      byte[] bytes = packet1024.toBytesInstrument();
      new RequestPacket(bytes);
      //assert(testJson.equals(testJson2));
    }
    long endTime = System.nanoTime();
    double avg = (endTime - startTime) / (TEST_RUNS);
    System.out.println("Average byteification time PacketToBytes 1024B was " + avg + " nanoseconds.");
    byte[] bytes = packet1024.toBytesInstrument();
    RequestPacket packet = new RequestPacket(bytes);
    JSONObject testJson2 = packet.toJSONObject();
    assert (packet1024.toJSONObject().toString().equals(testJson2.toString()));
    //System.out.println("JSON1: \n" + testJson.toString());
    //System.out.println("JSON2: \n" + testJson2.toString());

  }

  /**
   *
   * @throws JSONException
   * @throws IOException
   */
  @Test
  public void test_13_msgpack_request_1024() throws JSONException, IOException {
    long startTime = System.nanoTime();
    for (int i = 0; i < TEST_RUNS; i++) {
      JSONObject json1024 = packet1024.toJSONObject();
      byte[] bytes = JSONByteConverter.toBytesMsgpack(json1024);
      JSONByteConverter.fromBytesMsgpack(bytes);
      //assert(testJson.equals(testJson2));
    }
    long endTime = System.nanoTime();
    double avg = (endTime - startTime) / (TEST_RUNS);
    System.out.println("Average byteification time Msgpack 1024B was " + avg + " nanoseconds.");
    JSONObject json1024 = packet1024.toJSONObject();
    byte[] bytes = JSONByteConverter.toBytesMsgpack(json1024);
    JSONObject testJson2 = JSONByteConverter.fromBytesMsgpack(bytes);
    assert (json1024.toString().equals(testJson2.toString()));
    //System.out.println("JSON1: \n" + testJson.toString());
    //System.out.println("JSON2: \n" + testJson2.toString());
  }

  /**
   *
   * @throws UnsupportedEncodingException
   * @throws JSONException
   */
  @Test
  public void test_14_CommandValueReturnPacket_128B() throws UnsupportedEncodingException, JSONException {
    ResponsePacket packet = new ResponsePacket(1, ResponseCode.NO_ERROR.getCodeValue(), new String(Util.getRandomAlphanumericBytes(64)), new String(Util.getRandomAlphanumericBytes(64)));
    long startTime = System.nanoTime();
    for (int i = 0; i < TEST_RUNS; i++) {
      byte[] bytes = packet.toBytes();
      ResponsePacket.fromBytes(bytes);
    }
    long endTime = System.nanoTime();
    double avg = (endTime - startTime) / (TEST_RUNS);
    System.out.println("Average byteification time CommandValueReturnPacket 128B was " + avg + " nanoseconds.");
    byte[] bytes = packet.toBytes();
    ResponsePacket outputPacket = ResponsePacket.fromBytes(bytes);
    assert (packet.toJSONObject().toString().equals(outputPacket.toJSONObject().toString()));
  }

  /**
   *
   * @throws UnsupportedEncodingException
   * @throws JSONException
   */
  @Test
  public void test_15_CommandValueReturnPacket_1024B_Strings() throws UnsupportedEncodingException, JSONException {
    ResponsePacket packet = new ResponsePacket(1, ResponseCode.NO_ERROR.getCodeValue(), new String(Util.getRandomAlphanumericBytes(512)), new String(Util.getRandomAlphanumericBytes(512)));
    long startTime = System.nanoTime();
    for (int i = 0; i < TEST_RUNS; i++) {
      byte[] bytes = packet.toBytes();
      ResponsePacket.fromBytes(bytes);
    }
    long endTime = System.nanoTime();
    double avg = (endTime - startTime) / (TEST_RUNS);
    System.out.println("Average byteification time CommandValueReturnPacket 1024B was " + avg + " nanoseconds.");
    byte[] bytes = packet.toBytes();
    ResponsePacket outputPacket = ResponsePacket.fromBytes(bytes);
    assert (packet.toJSONObject().toString().equals(outputPacket.toJSONObject().toString()));
  }

  /**
   *
   * @throws UnsupportedEncodingException
   * @throws JSONException
   */
  @Test
  public void test_16_CommandValueReturnPacket_toBytes_128B() throws UnsupportedEncodingException, JSONException {
    ResponsePacket packet = new ResponsePacket(1, ResponseCode.NO_ERROR.getCodeValue(), new String(Util.getRandomAlphanumericBytes(64)), new String(Util.getRandomAlphanumericBytes(64)));
    long startTime = System.nanoTime();
    for (int i = 0; i < TEST_RUNS; i++) {
      packet.toBytes();
    }
    long endTime = System.nanoTime();
    double avg = (endTime - startTime) / (TEST_RUNS);
    System.out.println("Average byteification time CommandValueReturnPacket toBytes 128B was " + avg + " nanoseconds.");
    byte[] bytes = packet.toBytes();
    ResponsePacket outputPacket = ResponsePacket.fromBytes(bytes);
    assert (packet.toJSONObject().toString().equals(outputPacket.toJSONObject().toString()));
  }

  /**
   *
   * @throws UnsupportedEncodingException
   * @throws JSONException
   */
  @Test
  public void test_17_CommandValueReturnPacket_toBytes_1024B_Strings() throws UnsupportedEncodingException, JSONException {
    ResponsePacket packet
            = new ResponsePacket(1, ResponseCode.NO_ERROR.getCodeValue(),
                    new String(Util.getRandomAlphanumericBytes(512)), new String(Util.getRandomAlphanumericBytes(512)));
    long startTime = System.nanoTime();
    for (int i = 0; i < TEST_RUNS; i++) {
      packet.toBytes();
    }
    long endTime = System.nanoTime();
    double avg = (endTime - startTime) / (TEST_RUNS);
    System.out.println("Average byteification time CommandValueReturnPacket toBytes 1024B was " + avg + " nanoseconds.");
    byte[] bytes = packet.toBytes();
    ResponsePacket outputPacket = ResponsePacket.fromBytes(bytes);
    assert (packet.toJSONObject().toString().equals(outputPacket.toJSONObject().toString()));
  }

}
