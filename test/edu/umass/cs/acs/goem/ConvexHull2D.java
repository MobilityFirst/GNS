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

import edu.umass.cs.acs.geodesy.GlobalCoordinate;
import java.util.Collection;
import java.util.ArrayList;

/**
 * A convex hull implementation.
 *
 * @author	Westy (westy@cs.umass.edu)
 */
public class ConvexHull2D {

  // Points is filled with points to test, then stripped down to minimal set when hull calcualted 

  private ArrayList<PointSimple> points;

  public ConvexHull2D() {
    points = new ArrayList<PointSimple>();
  }
  public ConvexHull2D(Collection<GlobalCoordinate> points) {
    this();
    addAll(points);
    calculateHull();
  }

  public void addPoint(double x, double y) {
    PointSimple newPoint = new PointSimple(x, y);
    points.add(newPoint);
  }
  
  public void addPoint(GlobalCoordinate coord) {
    addPoint(coord.getLongitude(), coord.getLatitude());
  }
  
  public void addAll(Collection<GlobalCoordinate> points) {
    for (GlobalCoordinate item : points) {
      addPoint(item);
    }
  }

  public void clear() {
    points.clear();
  }

  public ArrayList<PointSimple> getPoints() {
    return this.points;
  }

  public void calculateHull() {
    if (points.size() > 0) {
      // Holds the points of the calculated hull 
      ArrayList hullPoints = new ArrayList();

		 // First find an extreme point guranteed to be on the hull 
      // Start from the first point and compare all others for minimal y coord 
      PointSimple startPoint = (PointSimple) points.get(0);
      int startIndex = 0;

      for (int i = 0; i < points.size(); i++) {
        PointSimple testPoint = (PointSimple) points.get(i);

        // Find lowest y, and lowest x if equal y values. 
        if (testPoint.getY() < startPoint.getY()) {
          startPoint = testPoint;
          startIndex = i;
        } else if (testPoint.getY() == startPoint.getY()) {
          if (testPoint.getX() < startPoint.getX()) {
            startPoint = testPoint;
            startIndex = i;
          }
        }
      }

		 // Check this... can't remove them because of test below...
      //points.remove(startIndex);
      // Add the start point 
      hullPoints.add(startPoint);

      PointSimple currentPoint = startPoint;
      PointSimple currentDirection = new PointSimple(1.0f, 0.0f);
      PointSimple nextPoint = null;

      int debug = 0;

      // March around the edge. Finish when we get back to where we started 
      while (true) {
        // Find next point with largest right turn relative to current 
        double currentAngle = 181f;
        int chosenIndex = -1;
        for (int i = 0; i < points.size(); i++) {
          PointSimple testPoint = (PointSimple) points.get(i);

          // Find angle between test and current points 
          PointSimple testDirection = new PointSimple(testPoint);
          testDirection.subtract(currentPoint);
          testDirection.normalise();

          double testAngle = currentDirection.angle(testDirection);

			 // FeatureRepo.logger.info("testPoint is "+testPoint+" testAngle = "+testAngle+" currentPoint is "+currentPoint+" currentAngle = "+currentAngle);
          // Update next point with test if smaller angle 
          if (testAngle < currentAngle) {
            currentAngle = testAngle;
            nextPoint = testPoint;
            chosenIndex = i;
          } else if (testAngle == currentAngle) {
            // take point furthest away from current 
            if (currentPoint.distanceTo(testPoint) > currentPoint.distanceTo(nextPoint)) {
              nextPoint = testPoint;
              chosenIndex = i;
            }
          }
        }

		     // might make it faster, but is this Kosher
        // if (chosenIndex != -1)
        // points.remove(chosenIndex);
        // nextPoint can be null if all the points are nearly the same
        if (nextPoint == null) {
          //FeatureRepo.logger.info("nextPoint is NULL! number of points is "+points.size()+" currentAngle = "+currentAngle+" startIndex ="+startIndex+" chosenIndex = "+chosenIndex);
        }

		     // Exit? 
        // nextPoint can be null if all the points are nearly the same
        if (nextPoint == null || nextPoint == startPoint || debug > 1000) {
          break;
        }

        // Add and advance 
        hullPoints.add(nextPoint);

        currentDirection.set(nextPoint);
        currentDirection.subtract(currentPoint);

        currentPoint = nextPoint;
        nextPoint = null;

        debug++;
      }
      points = hullPoints;
    }
  }
}
