package org.apache.hadoop.examples.mst;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.StringTokenizer;

//import javax.media.j3d.PointSound;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.Text;

public class Point implements WritableComparable<Point> {
	
  private int myDimension;
  private ArrayList<Integer> myCoordinates;
  private Text myInfo;
  private long myColor;

  private static StringBuffer sb = new StringBuffer();
  private static Random RandomGenerator = new Random();  
  
  public static final double INF = 1E100;

  public static long getRandomLong() {
	  return (((long) RandomGenerator.nextInt()) << 32) + RandomGenerator.nextInt();
  }
  
  public Point(int dimension) {
    myDimension = dimension;
    myCoordinates = new ArrayList<Integer>(dimension);
    for (int i = 0; i < dimension; i++) {
      myCoordinates.add(0);
    }
    myColor = getRandomLong();
  }

  public Point(int dimension, Text info) {
    // TODO Auto-generated constructor stub
    myDimension = dimension;
    myCoordinates = new ArrayList<Integer>(dimension);
    for (int i = 0; i < dimension; i++) {
      myCoordinates.add(0);
    }
    myInfo = info;
    myColor = getRandomLong();
  }

  public Point(ArrayList<Integer> coordinates, Text info) {
    myDimension = coordinates.size();
    myCoordinates = coordinates;
    myInfo = info;
    myColor = getRandomLong();
  }
  
  public Point(int dimension, ArrayList<Integer> coords, long color, Text info) {
	  myDimension = dimension;
	  myCoordinates = coords;
	  myColor = color;
	  myInfo = info;
  }
  
  public long getColor() {
	  return myColor;
  }
  
  public void setColor(long color) {
	  myColor = color;
  }

  public static double dist(Point p1, Point p2) {
    double ret = 0;
    for (int i = 0; i < p1.getDimension(); i++) {
      ret +=
          (p2.getCoordinates().get(i) - p1.getCoordinates().get(i))
              * (p2.getCoordinates().get(i) - p1.getCoordinates().get(i));
    }
    return Math.sqrt(ret);
  }

  // Basic Prim
  
  public static double Prim(ArrayList<Point> points) {
	    double length = 0;
	    int n = points.size();

	    if (n == 0) {
	      return 0;
	    }

	    double[] dist = new double[n];
	    boolean[] done = new boolean[n];

	    for (int i = 0; i < n; i++) {
	      done[i] = false;
	      dist[i] = INF;
	    }
	    dist[0] = 0;
	    for (int i = 0; i < n; i++) {
	      int best = -1;
	      double bestdist = INF;
	      for (int j = 0; j < n; j++) {
	        if (!done[j] && dist[j] < bestdist) {
	          best = j;
	          bestdist = dist[j];
	        }
	      }
	      length += bestdist;
	      done[best] = true;
	      for (int j = 0; j < n; j++) {
	        if (!done[j]
	 && dist[j] > Point.dist(points.get(best), points.get(j))) {
	          dist[j] = Point.dist(points.get(best), points.get(j));
	        }
	      }
	    }
	    return length;
	  }

  
  //Prim's algorithm, using only edges of length at most maxLength 
  
  public static double PrimShort(ArrayList<Point> points, double maxLength){
    System.out.println("Entering PrimShort...");
	  
    
   // for (Point p : points) {
    //	System.out.println(p.print());
   // }
    
	double length = 0;
    int n = points.size();

    if (n == 0) {
        System.out.println("Terminating Prim since n = 0.");
    	return 0;
    }

    HashSet<Long> conn = new HashSet<Long>();
    double[] dist = new double[n];
    
    
    for (int it = 0; it < n; it++) {
    	long curcolor = points.get(it).myColor;
    	if (!conn.contains(curcolor)) {
    		conn.add(curcolor);
    	} else {
    		continue;
    	}
    	
    	for (int i = 0; i < n; i++) {
    	      dist[i] = INF;
    	}
    	
    	dist[it] = 0;
    	int best = it;
    	
    	System.out.println("Entering loop...");
    	
    	while (true) {	
	        Point bestPoint = points.get(best);
	        long oldColor = bestPoint.getColor();
	        for (int i = 0; i < n; i++) {
	      	  Point curPoint = points.get(i);
	      	  if (curPoint.getColor() == oldColor) {
	      		  curPoint.setColor(curcolor);
	      		  for (int j = 0; j < n; j++) {
	      			  if (dist[j] > Point.dist(curPoint, points.get(j))) { 
	      				  dist[j] = Point.dist(curPoint, points.get(j));
	      			  }  
	      		  }
	      	  }        	 
	        }
    	
	      System.out.println("Old color = " + oldColor);  
	        
          best = -1;
          double bestdist = INF;
          for (int i = 0; i < n; i++) {
        	  Point next = points.get(i); 
        	  if (curcolor != next.getColor() && dist[i] < bestdist && dist[i] <= maxLength) {
        		  best = i;
        		  bestdist = dist[i];
        	  }
          }	
          System.out.println("Best = " + best);
          System.out.println("Best dist = " + bestdist);
          if (best == -1) {
        	  break;
          }
          length += bestdist;
        }      
    	
    	System.out.println("Exiting loop...");
    }
    System.out.println("Exiting PrimShort...");
    
 //   for (Point p : points) {
 //  	System.out.println(p.print());
  //  }
    
    return length;
  }

  public static ArrayList<Point> epsNet(ArrayList<Point> points, double distance) {
//	  System.out.println("Entering epsNet...");
	  
	  int n = points.size();
	  double [] dist = new double[n];
	  for (int i = 0; i < n; i++) {
		  dist[i] = INF;
	  }
	  ArrayList<Point> result = new ArrayList<Point>();
	  while (true) {
		  double maxdist = 0;
		  int furthest = 0;
		  for (int i = 0; i < n; i++) {
			  if (dist[i] > maxdist) {
				  maxdist = dist[i];
				  furthest = i;
			  }
		  }
		  if (maxdist <= distance) {
			  break;
		  }
		  result.add(points.get(furthest));
		  for (int i = 0; i < n; i++) {
			  dist[i] = Math.min(dist[i], Point.dist(points.get(furthest), points.get(i)));
		  }
	  }
//	  System.out.println("Exiting epsNet...");
	  return result;
  }
  
  
  
  /**
   * Getters and setters
   */
  public void set(int pos, int value) {
    myCoordinates.set(pos, value);
  }

  public int get(int pos) {
    return myCoordinates.get(pos);
  }

  public void setInfo(Text info) {
    myInfo = info;
  }

  public int getDimension() {
    return myDimension;
  }

  public ArrayList<Integer> getCoordinates() {
    return myCoordinates;
  }

  public Text getInfo() {
    return myInfo;
  }

  /**
   * Read / write
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    for (int i = 0; i < myDimension; i++) {
      myCoordinates.set(i, in.readInt());
    }
    myInfo.set(in.readLine());
  }

  @Override
  public void write(DataOutput out) throws IOException {
    for (int i = 0; i < myDimension; i++) {
      out.writeInt(myCoordinates.get(i));
    }
  }

  @Override
  public int hashCode() {
    return myCoordinates.hashCode();
  }

  @Override
  public boolean equals(Object right) {
    if (right instanceof Point) {
      Point r = (Point) right;
      return myCoordinates.equals(r.getCoordinates());
    } else {
      return false;
    }
  }

  /** A Comparator that compares serialized Point. */
  public static class Comparator extends WritableComparator {
    public Comparator() {
      super(Point.class);
    }

    @Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      return compareBytes(b1, s1, l1, b2, s2, l2);
    }
  }

  static { // register this comparator
    WritableComparator.define(Point.class, new Comparator());
  }

  @Override
  public int compareTo(Point o) {
    for (int i = 0; i < myDimension; i++) {
      if (myCoordinates.get(i) < o.get(i)) {
        return -1;
      } else if (myCoordinates.get(i) > o.get(i)) {
        return 1;
      }
    }
    return 0;
  }

  public static Point read(StringTokenizer st) {
	  int dimension = Integer.parseInt(st.nextToken());
	  ArrayList<Integer> coords = new ArrayList<Integer>();
	  for (int i = 0; i < dimension; i++) {
		  coords.add(Integer.parseInt(st.nextToken()));
	  }
	  Long color = Long.parseLong(st.nextToken());
	  return new Point(dimension, coords, color, new Text(" "));
  }
  
  public static String print(Point p) {
    sb.delete(0, sb.length());
    sb.append(p.myDimension + " ");
    for (int i = 0; i < p.myDimension; i++) {
      sb.append(p.myCoordinates.get(i));
      sb.append(" ");
    }
    sb.append(p.myColor);
    while (sb.length() < MSTInputFormat.KEY_LENGTH) {
      sb.append(" ");
    }
    return sb.toString();
  }

  public String printInfo() {
    StringBuilder sb = new StringBuilder();
    sb.append(myInfo);
    while (sb.length() != MSTInputFormat.VALUE_LENGTH) {
      sb.append(" ");
    }
    return sb.toString();
  }
}
