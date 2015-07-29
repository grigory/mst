package org.apache.hadoop.examples.mst;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;


public class MSTCompute extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(MSTCompute.class);
  private static int dimension;
  private static int blocksize;
  private static int shortEdge;
  private static int epsNetDistance;
  
  public MSTCompute() {
    // TODO Auto-generated constructor stub
  }

  static class ComputeMapper extends Mapper<Text, Text, Text, Text> {
    private ArrayList<Point> points = new ArrayList<Point>();

    @Override
	public void map(Text key, Text value, Context context) throws IOException,
        InterruptedException {
    	points.add(Point.read(new StringTokenizer(key.toString(), " ")));
    	System.out.println("Key = " + key);
    	System.out.println("Value = " + value);
    }

    @Override
	public void cleanup(Context context) throws IOException,
        InterruptedException {
      
    /*	System.out.println();
    	System.out.println();
    	System.out.println();
    	
    	System.out.println("HERE!!!!!!");
    	
    	System.out.println();
    	System.out.println();
    	System.out.println();
    	*/
    	
    	if (points.size() > 0) {
	        double length = Point.PrimShort(points, shortEdge);
	        ArrayList<Point> reps = Point.epsNet(points, epsNetDistance);
	       // Point rep = points.get(RandomGenerator.nextInt(points.size()));
	    //    System.out.println("Length = " + length);
	        StringBuffer buf = new StringBuffer(String.valueOf(length) + " " + String.valueOf(reps.size())); 
	        for (Point p : reps) {
		        buf.append(" " + Point.print(p));
	        }
	        System.out.println("Mapper buffer = " + buf);
	        context.write(new Text("Points"), new Text(buf.toString()));
    	}
    	
    ///	System.out.println(); 
    //	System.out.println();
    //	System.out.println();
    }
  }
  
  static class ComputeReducer extends Reducer<Text, Text, Text, Text> {

    @Override
	public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      
    	
    	System.out.println();
    	System.out.println();
    	System.out.println("Entering the reduction phase...");
    	System.out.println();
    	System.out.println();
    	
    	
    	System.out.println("Key = " + key);
    /*	for (Text it : values) {
    		System.out.println("Printing values first: " + it);
    	}
    	*/
    	
    	double result = 0;
    	ArrayList<Point> points = new ArrayList<Point>();
    	System.out.println("Before the second loop");
    	for (Text it : values) {
    		System.out.println("Printing values second: " + it);
    		StringTokenizer st = new StringTokenizer(it.toString(), " ");
    		double curlength = Double.parseDouble(st.nextToken());
    		System.out.println("Current length = " + curlength);
    		result += curlength;
   //     System.out.println("Length = "+ curlength);
    		System.out.println("Intermediate result = " + result);
    		int numpoints = Integer.parseInt(st.nextToken());
   //     System.out.println();
     //   System.out.println("Number of points = " + numpoints);
     //   System.out.println();
    		for (int j = 0; j < numpoints; j++) {
    			points.add(Point.read(st));
    		}
    	}
    	System.out.println("Number of points before the second Prim = " + points.size());
    	result += Point.PrimShort(points, Point.INF);
    	System.out.println("Final length = " + result);
    	context.write(new Text("Result = "), new Text((Double.valueOf(result)).toString()));
    }
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    // Configuring log4j
    BasicConfigurator.configure();

    int res = ToolRunner.run(new Configuration(), new MSTCompute(), args);
    System.exit(res);
  }

  @Override
public int run(String[] args) throws Exception {
    LOG.info("starting");
    
    dimension = Integer.parseInt(args[0]);
    shortEdge = Integer.parseInt(args[1]);
    epsNetDistance = Integer.parseInt(args[2]);
    
    
    
    Configuration conf = getConf();

    conf.setInt("dimension", dimension);
    conf.setInt("shortEdge", shortEdge);
    conf.setInt("epsNetDistance", epsNetDistance);
    
    
    Job job = Job.getInstance(conf);


    Path inputDir = new Path(args[3]);
    Path outputDir = new Path(args[4]);

    FileInputFormat.setInputPaths(job, inputDir);
    FileOutputFormat.setOutputPath(job, outputDir);

    // Set number of mappers to be a perfect square
    int maps = job.getConfiguration().getInt(MRJobConfig.NUM_MAPS, 1);

    blocksize = (int) (Math.pow(maps, 1.0 / dimension) + 0.000001);
 
    job.getConfiguration().setInt("blocksize", blocksize);
    
    maps = 1;
    for (int i = 0; i < dimension; i++) {
      maps *= blocksize;
    }
    
    job.getConfiguration().setInt(MRJobConfig.NUM_MAPS, maps);
    System.err.println(job.getConfiguration()
        .getInt(MRJobConfig.NUM_REDUCES, 1));

    job.setJobName("MSTCompute");
    job.setJarByClass(MSTCompute.class);

    job.setMapperClass(ComputeMapper.class);
    job.setReducerClass(ComputeReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass(MSTInputFormat.class);
    job.setOutputFormatClass(MSTOutputFormat.class);
  

    MSTOutputFormat.setFinalSync(job, true);
    int ret = job.waitForCompletion(true) ? 0 : 1;
    LOG.info("done");
    return ret;
  }

  
}
