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
 *  Initial developer(s): Abhigyan Sharma, Westy
 *
 */
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport;

import edu.umass.cs.gnsserver.utils.ResultValue;
import edu.umass.cs.gnsserver.utils.ResultValueString;
import edu.umass.cs.gnscommon.utils.Format;
import edu.umass.cs.gnsserver.utils.JSONUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 * Stores the guid and associated human readable name, public key and
 * other information for a guid.
 *
 * This class handles the conversion to and from JSON objects as well as 
 * conversion to the format which can be store in the database.
 *
 * @author westy
 */
public class GuidInfo {

  /**
   * The GUID.
   */
  private String guid;
  /**
   * The human readable name corresponding to this GUID.
   * There will be an entry in the GNS that reads <name> : _GNS_GUID -> <guid>
   */
  private String name;
  /**
   * A base64 encoded public key.
   */
  private String publicKey;
  // This is reserved for future use.
  private String type;
  /** 
   * Special uses.
   */
  private Set<String> tags;
  private Date created;
  private Date updated;

  /**
   * Creates a GuidInfo instance with a human readable name, guid and public key.
   *
   * @param userName
   * @param guid
   * @param publicKey 
   */
  public GuidInfo(String userName, String guid, String publicKey) {
    this.name = userName;
    this.guid = guid;
    this.type = "DEFAULT"; // huh? :-)
    this.publicKey = publicKey;
    this.created = new Date();
    this.updated = new Date();
    this.tags = new HashSet<String>();
  }

  /**
   Convert GuidInfo to and from the format which is used to store it in the DB. 
   Use a JSON Object which is put as the first element of an ArrayList
   @return a {@link ResultValue}
   @throws JSONException 
   */
  public ResultValue toDBFormat() throws JSONException {
    return new ResultValue(Arrays.asList(toJSONObject().toString()));
  }

  /**
   Creates a GuidInfo instance from a ResultValueString which is the format that
   is used to store the object in the database.
  
   @param queryResult
   @throws JSONException
   @throws ParseException 
   */
  public GuidInfo(ResultValueString queryResult) throws JSONException, ParseException {
    this(new JSONObject(queryResult.get(0)));
  }

  // JSON Conversion
  private static final String PUBLICKEY = "publickey";
  private static final String NAME = "name";
  private static final String GUID = "guid";
  private static final String TYPE = "type";
  private static final String CREATED = "created";
  private static final String UPDATED = "updated";
  private static final String TAGS = "tags";

  /**
   Creates a GuidInfo instance from a JSONObject.
  
   @param json
   @throws JSONException
   @throws ParseException 
   */
  public GuidInfo(JSONObject json) throws JSONException, ParseException {
    this.publicKey = json.getString(PUBLICKEY);
    this.name = json.getString(NAME);
    this.guid = json.getString(GUID);
    this.type = json.getString(TYPE);
    this.created = Format.parseDateUTC(json.getString(CREATED));
    this.updated = Format.parseDateUTC(json.getString(UPDATED));
    this.tags = JSONUtils.JSONArrayToHashSet(json.getJSONArray(TAGS));
  }

  /**
   * Converts this instance into a JSONObject.
   *
   * @return a JSONObject
   * @throws JSONException 
   */
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    json.put(PUBLICKEY, publicKey);
    json.put(NAME, name);
    json.put(GUID, guid);
    json.put(TYPE, type);
    json.put(CREATED, Format.formatDateUTC(created));
    json.put(UPDATED, Format.formatDateUTC(updated));
    json.put(TAGS, new JSONArray(tags));
    return json;
  }

  /**
   * Returns the human readable name.
   *
   * @return a string
   */
  public String getName() {
    return name;
  }

  /**
   * Returns the guid.
   *
   * @return a string
   */
  public String getGuid() {
    return guid;
  }

  /**
   * Returns the public key.
   *
   * @return a string
   */
  public String getPublicKey() {
    return publicKey;
  }

  /**
   * Sets the public key.
   * 
   * @param publicKey 
   */
  public void setPublicKey(String publicKey) {
    this.publicKey = publicKey;
  }

  /**
   * Returns the type. Currently an unused feature.
   *
   * @return a string
   */
  public String getType() {
    return type;
  }

  /**
   * Returns the creation date of the guid.
   *
   * @return a date
   */
  public Date getCreated() {
    return created;
  }

  /**
   * Returns the update date of the guid.
   *
   * @return a date
   */
  public Date getUpdated() {
    return updated;
  }

  /**
   * Updates the update date of the guid.
   *
   */
  public void noteUpdate() {
    this.updated = new Date();
  }

  /**
   * Returns true if this guid contains the tag.
   *
   * @param tag
   * @return true if the tag was found
   */
  public boolean containsTag(String tag) {
    return tags.contains(tag);
  }

  /**
   * Adds the tag to the guid.
   *
   * @param tag
   */
  public void addTag(String tag) {
    tags.add(tag);
  }

  /**
   * Removes the tag from the guid.
   *
   * @param tag
   */
  public void removeTag(String tag) {
    tags.remove(tag);
  }

  /**
   * Returns a informational string version of the instance. 
   *
   * @return the string
   */
  @Override
  public String toString() {
    return "GuidInfo{" + "guid=" + guid + ", name=" + name + ", publicKey=" + publicKey + ", type=" + type + ", tags=" + tags + ", created=" + created + ", updated=" + updated + '}';
  }

}
