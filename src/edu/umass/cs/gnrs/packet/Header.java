package edu.umass.cs.gnrs.packet;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * ************************************************************
 * This class represents the DNS packet header.
 *
 * @author Hardeep Uppal
 *
 ************************************************************
 */
public class Header {

  private static final String ID = "id";
  private static final String QR = "qr";
  private static final String RCODE = "rcode";
  
  /**
   * Unique ID for each query. Used by the requester to match up replies to outstanding queries *
   */
  private int id;
  /**
   * Specifies whether this message is a query (0), or a response (1) *
   */
  private int qr;
  /**
   * Response code *
   */
  private int rcode;

  /**
   * ***********************************************************
   * Constructs a Header for the resource record packet with the specified fields.
   *
   * @param id Unique ID for each query
   * @param qr Specifies whether this message is a query (0), or a response (1)
   * @param rcode Response code
	 ***********************************************************
   */
  public Header(int id, int qr, int rcode) {
    this.id = id;
    this.qr = qr;
    this.rcode = rcode;
  }

  /**
   * ***********************************************************
   * Constructs a Header for the resource record packet from a JSONObject that represents the packet header
   *
   * @param json JSONObject that represents the packet header
   * @throws JSONException
	 ***********************************************************
   */
  public Header(JSONObject json) throws JSONException {
    this.id = json.getInt(ID);
    this.qr = json.getInt(QR);
    this.rcode = json.getInt(RCODE);
  }

  /**
   * ***********************************************************
   * Converts the packet header to a JSONObject that represents the header.
   *
   * @return Return a JSONObject that represents the packet header
   * @throws JSONException
	 ***********************************************************
   */
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    json.put(ID, getId());
    json.put(QR, getQr());
    json.put(RCODE, getRcode());
    return json;
  }

  /**
   * ***********************************************************
   * Returns a string representing the packet header.
	 ***********************************************************
   */
  @Override
  public String toString() {
    try {
      return this.toJSONObject().toString();
    } catch (JSONException e) {
      e.printStackTrace();
      return null;
    }
  }

  /**
   * @return the id
   */
  public int getId() {
    return id;
  }

  /**
   * @param id the id to set
   */
  public void setId(int id) {
    this.id = id;
  }

  /**
   * @return the qr
   */
  public int getQr() {
    return qr;
  }

  /**
   * @param qr the qr to set
   */
  public void setQr(int qr) {
    this.qr = qr;
  }

  /**
   * @return the rcode
   */
  public int getRcode() {
    return rcode;
  }

  /**
   * @param rcode the rcode to set
   */
  public void setRcode(int rcode) {
    this.rcode = rcode;
  }
}
