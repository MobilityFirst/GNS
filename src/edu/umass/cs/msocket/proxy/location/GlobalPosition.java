/*******************************************************************************
 *
 * Mobility First - mSocket library
 * Copyright (C) 2013, 2014 - University of Massachusetts Amherst
 * Contact: arun@cs.umass.edu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 *
 * Initial developer(s): Arun Venkataramani, Aditya Yadav, Emmanuel Cecchet.
 * Contributor(s): ______________________.
 *
 *******************************************************************************/

package edu.umass.cs.msocket.proxy.location;

/**
 * <p>
 * Encapsulates a three dimensional location on a globe (GlobalCoordinates
 * combined with an elevation in meters above a reference ellipsoid).
 * </p>
 * <p>
 * See documentation for GlobalCoordinates for details on how latitude and
 * longitude measurements are canonicalized.
 * </p>
 */
public class GlobalPosition extends GlobalCoordinate
{

  /** Elevation, in meters, above the surface of the ellipsoid. */
  private double mElevation;

  /**
   * Creates a new instance of GlobalPosition.
   * 
   * @param latitude latitude in degrees
   * @param longitude longitude in degrees
   * @param elevation elevation, in meters, above the reference ellipsoid
   */
  public GlobalPosition(double latitude, double longitude, double elevation)
  {
    super(latitude, longitude);
    mElevation = elevation;
  }

  /**
   * Creates a new instance of GlobalPosition.
   * 
   * @param coords coordinates of the position
   * @param elevation elevation, in meters, above the reference ellipsoid
   */
  public GlobalPosition(GlobalCoordinate coords, double elevation)
  {
    this(coords.getLatitude(), coords.getLongitude(), elevation);
  }

  /**
   * Get elevation.
   * 
   * @return elevation about the ellipsoid in meters.
   */
  public double getElevation()
  {
    return mElevation;
  }

  // synonym
  public double getAltitude()
  {
    return getElevation();
  }

  // synonym
  public double getAlt()
  {
    return getElevation();
  }

  /**
   * Set the elevation.
   * 
   * @param elevation elevation about the ellipsoid in meters.
   */
  public void setElevation(double elevation)
  {
    mElevation = elevation;
  }

  public void setAltitude(double altitude)
  {
    setElevation(altitude);
  }

  /**
   * Compare this position to another. Western longitudes are less than eastern
   * logitudes. If longitudes are equal, then southern latitudes are less than
   * northern latitudes. If coordinates are equal, lower elevations are less
   * than higher elevations
   * 
   * @param other instance to compare to
   * @return -1, 0, or +1 as per Comparable contract
   */
  public int compareTo(GlobalPosition other)
  {
    int retval = super.compareTo(other);

    if (retval == 0)
    {
      if (mElevation < other.mElevation)
      {
        retval = -1;
      }
      else if (mElevation > other.mElevation)
      {
        retval = +1;
      }
    }

    return retval;
  }

  /**
   * Get a hash code for this position.
   * 
   * @return
   */
  @Override
  public int hashCode()
  {
    int hash = super.hashCode();

    if (mElevation != 0)
    {
      hash *= (int) mElevation;
    }

    return hash;
  }

  /**
   * Compare this position to another object for equality.
   * 
   * @param other
   * @return
   */
  @Override
  public boolean equals(Object obj)
  {
    if (!(obj instanceof GlobalPosition))
    {
      return false;
    }

    GlobalPosition other = (GlobalPosition) obj;

    return (mElevation == other.mElevation) && (super.equals(other));
  }

  /**
   * Get position as a string.
   */
  @Override
  public String toString()
  {
    StringBuilder result = new StringBuilder();

    result.append(super.toString());
    result.append("elevation=");
    result.append(Format.formatFloat(mElevation));
    result.append("m");

    return result.toString();
  }
}
