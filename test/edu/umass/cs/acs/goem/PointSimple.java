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
package edu.umass.cs.acs.goem;

import edu.umass.cs.acs.geodesy.GlobalPosition;
import java.awt.Color;
import java.awt.Graphics;
import static java.lang.Math.*;
import java.text.DecimalFormat;
import java.util.logging.Logger;


/**
 * A representation of a multidimensional point.
 * 
 * @author	Westy (westy@cs.umass.edu)
 */
public class PointSimple implements Cloneable {
    static Logger logger=Logger.getLogger(PointSimple.class.getName());

    private double pointArray[];
    
    public PointSimple(double x, double y) {
	this.pointArray = new double[2];
	this.pointArray[0] = x;
	this.pointArray[1] = y;
    }

    public PointSimple(double x, double y, double z) {
	this.pointArray = new double[3];
	this.pointArray[0] = x;
	this.pointArray[1] = y;
	this.pointArray[2] = z;
    }

    public PointSimple(double x, double y, double z, double w) {
	this.pointArray = new double[4];
	this.pointArray[0] = x;
	this.pointArray[1] = y;
	this.pointArray[2] = z;
	this.pointArray[3] = w;
    }

    public PointSimple(PointSimple point) {
	this.pointArray = new double[point.getDimensions()];
	for (int i=0; i < this.getDimensions(); i++) {
	    this.pointArray[i] = point.pointArray[i];
	}
    }

    public PointSimple(int dimensions) {
	this.pointArray = new double[dimensions];
	for (int i=0; i < this.getDimensions(); i++) {
	    this.pointArray[i] = 0.0;
	}
    }

    public PointSimple (int x, int y) {
	this((double)x, (double)y);
    }
	
    public void set(double x, double y) {
	setX(x); 
	setY(y); 
    }

    public void set(int x, int y) {
	setX((double)x); 
	setY((double)y); 
    }

    public void set(PointSimple point) {
	setX(point.getX()); 
	setY(point.getY()); 
    }
    
    public String toString() {
	DecimalFormat myFormatter = new DecimalFormat("#####.###");
	int dimensions = this.getDimensions();
	String result = "(";
	for (int i=0; i < dimensions; i++) {
	    result = result + myFormatter.format(this.pointArray[i]);
	    if (i < dimensions - 1) result = result + ",";
	}
	return result+")";
    }

    public Object clone ()
    {
	try {
	    PointSimple newObject = (PointSimple) super.clone();
	    // deep copy - is this needed?
	    newObject.pointArray = new double[this.getDimensions()];
	    for (int i=0; i < newObject.getDimensions(); i++) {
		newObject.pointArray[i] = this.pointArray[i];
	    }
	    return newObject;
	} catch (CloneNotSupportedException e) {
	    return null;
	}
    }

    public int getDimensions() {
	return this.pointArray.length;
    }

    public void setDimension(int d, double x) {
	this.pointArray[d] = x;
    }

    public double getDimension(int d) {
	return this.pointArray[d];
    }
    
    public void setX(double x) {
	this.pointArray[0] = x;
    }
    
    public void setY(double y) {
	this.pointArray[1] = y;
    }
    
    public void setZ(double z) {
	this.pointArray[2] = z;
    }

    public void setW(double w) {
	this.pointArray[3] = w;
    }
    
    public double getX() {
	return this.pointArray[0];
    }
    
    public double getY() {
	return this.pointArray[1];
    }

    public double getYCorrected() {
	return this.pointArray[1] * Math.cos(this.pointArray[0]/57.3);
    }
    
    public double getZ() {
	return this.pointArray[2];
    }

    public double getW() {
	return this.pointArray[3];
    }

    public boolean equals(PointSimple p) {
	int d1 = p.getDimensions();
	if (d1 != getDimensions()) return false;
	for (int i=0; i < d1; i++) {
	    if (pointArray[i]  != p.pointArray[i]) return false;
	}
	return true;
    }
    
    public static double euclideanDistanceSquared(PointSimple p1, PointSimple p2) throws PointSimple.PointDimensionException {
	int d1 = p1.getDimensions();
	if (d1 != p2.getDimensions()) {
	    throw new PointSimple.PointDimensionException();
	}
	double sum = 0.0;
	for (int i=0; i < d1; i++) {
	    sum = sum + ((p1.pointArray[i] - p2.pointArray[i]) * (p1.pointArray[i] - p2.pointArray[i]));
	}
	return sum;
    }

    public static double euclideanDistance(PointSimple p1, PointSimple p2) throws PointSimple.PointDimensionException {
	return Math.sqrt(euclideanDistanceSquared(p1,p2));
    }

    public static double euclideanDistanceSquaredWeighted(PointSimple p1, PointSimple p2, double weights[]) throws PointSimple.PointDimensionException {
	int d1 = p1.getDimensions();
	if (d1 != p2.getDimensions()) {
	    throw new PointSimple.PointDimensionException();
	}
	double sum = 0.0;
	for (int i=0; i < d1; i++) {
	    sum = sum + ((p1.pointArray[i] - p2.pointArray[i]) * (p1.pointArray[i] - p2.pointArray[i])) * weights[i];
	}
	return sum;
    }
    
    // Mean radius in KM
    final static double EARTH_RADIUS = 6371.0;

    // CONSTANTS USED ELSEWHERE
    public final static double KILOMETERS_TO_MILES = 0.621371192237334;

    /** Method to compute Great Circle distance between two points. 
     * Please note that this algorithm  
     * assumes the Earth to be a perfect sphere, whereas
     * in fact the equatorial radius is about 30Km 
     * greater than the Polar.
     *
     * @param p1 start point to compute distance from
     * @param p2 end point to compute distance to
     * @return The distance in Kilometres
     */
    public static double sphericalDistance(PointSimple p1, PointSimple p2) {
	double p1_lon = toRadians(p1.pointArray[0]);
	double p1_lat = toRadians(p1.pointArray[1]);
	double p2_lon = toRadians(p2.pointArray[0]);
	double p2_lat = toRadians(p2.pointArray[1]);
	double r1 = Math.cos(p1_lat)*Math.cos(p1_lon) 
	    *Math.cos(p2_lat)*Math.cos(p2_lon);
	double r2 = Math.cos(p1_lat)*Math.sin(p1_lon) 
	    *Math.cos(p2_lat)*Math.sin(p2_lon);
	double r3 = Math.sin(p1_lat)*Math.sin(p2_lat);

	return(Math.acos(r1+r2+r3)*EARTH_RADIUS);
    }

    /**
     * Returns the (initial) bearing from this From to To
     *   see http://williams.best.vwh.net/avform.htm#Crs
     *
     * @param   {LatLon} point: Latitude/longitude of destination point
     * @returns {Number} Initial bearing in radians from North
     */
    public static double bearing(PointSimple from, PointSimple to) {
	double lat1 = toRadians(from.pointArray[1]);
	double lat2 = toRadians(to.pointArray[1]);
	double dLon = toRadians(to.pointArray[0] - from.pointArray[0]);
	double y = Math.sin(dLon) * Math.cos(lat2);
	double x = Math.cos(lat1)*Math.sin(lat2) - Math.sin(lat1)*Math.cos(lat2)*Math.cos(dLon);
	return Math.atan2(y, x);
    }

    public static String compassDirection(PointSimple p1, PointSimple p2) {
	String[] bearings = {"NE", "E", "SE", "S", "SW", "W", "NW", "N"};
        //String[] bearings = {"SW", "W", "NW", "N", "NE", "E", "SE", "S"};

	double rindex = toDegrees(bearing(p1, p2)) - 22.5;
	rindex = Angle.normalizeDegrees(rindex);
	int index = (int)Math.floor(rindex / 45.0);

	return bearings[index];
    }

    public static PointSimple midpoint(PointSimple p1, PointSimple p2) throws PointSimple.PointDimensionException {
	int d1 = p1.getDimensions();
	if (d1 != p2.getDimensions()) {
	    throw new PointSimple.PointDimensionException();
	}
	PointSimple p = new PointSimple(0.0, 0.0);
	for (int i=0; i < d1; i++) {
	    p.pointArray[i] = (p1.pointArray[i] + p2.pointArray[i]) / 2.0;
	    // logger.fine("p1:"+(p1.pointArray[i]+" p2:"+p2.pointArray[i])+" = "+ p.pointArray[i]);
	}
	return p;
    }

    void draw(Graphics g, Color c) {
      g.setColor(c);
      g.drawRect( (int)(getX()+.5)-1, (int)(getY()+.5)-1, 3,3 );
    }
    
    void draw(Graphics g, Color c, String s) {
      draw(g,c);
      g.drawString( s, (int)getX()+5, (int)getY()-5 );
    }

    boolean lessThan(PointSimple p) {
      return (getX()<p.getX());// || (getY()<p.getY());
    }
    
    boolean whichSide(PointSimple p1, PointSimple p2) {
      double Sin_Angle = (p2.getY()-p1.getY())*(p1.getX()-getX())- (p2.getX()-p1.getX())*(p1.getY()-getY());
      return (Sin_Angle>0);
    }

    // Subtracts a point from this one 
    public PointSimple subtract(PointSimple point) { 
	setX(getX() - point.getX());
	setY(getY() - point.getY());
	return this;
    } 

    // Adds a point to this one 
    public PointSimple add(PointSimple point) 
    { 
	setX(getX() + point.getX());
	setY(getY() + point.getY());
	return this;
    } 
    
    public double dot(PointSimple point) 
    { 
	return point.getX()*getX() + point.getY()*getY(); 
    } 
    
    public double length() 
    { 
	double dot = dot(this); 
	return Math.sqrt(dot);
    } 
    
    public void normalise() 
    { 
	double length = length(); 
	setX(getX()/length); 
	setY(getY()/length);
    } 
 
    public double angle(PointSimple point) 
    { 
	double angle = (this.dot(point))/(this.length()*point.length() ); 
	
	return Math.acos(angle); 
    } 
 
    public double distanceTo(PointSimple point) 
    { 
	double dx = point.getX() - getX(); 
	double dy = point.getY() - getY(); 

	// Find length of dx,dy 
	return Math.sqrt(dx*dx + dy*dy); 
    } 
    
    public static PointSimple toPoint(GlobalPosition gp) {
	return new PointSimple(gp.getLongitude(), gp.getLatitude(), gp.getAltitude());
    }
    
    static class GeneralPointException extends RuntimeException 
    {
	public GeneralPointException() { } 
    }
    
    static class PointDimensionException extends GeneralPointException
    {
	public PointDimensionException() { } 
    }

}
