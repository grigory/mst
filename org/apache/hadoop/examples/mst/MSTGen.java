/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.examples.mst;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

public class MSTGen extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(MSTSort.class);
    //private static final Log LOG = LogFactory.getLog(MSTSort.class);

  public static final String NUM_ROWS = "mapreduce.terasort.num-rows";

  public static int dimension;
  public static int maxcoordinate;


  /**
   * An input format that assigns ranges of longs to each mapper.
   */
  static class RangeInputFormat 
      extends InputFormat<LongWritable, NullWritable> {
    
    /**
     * An input split consisting of a range on numbers.
     */
    static class RangeInputSplit extends InputSplit implements Writable {
      long firstRow;
      long rowCount;

      public RangeInputSplit() { }

      public RangeInputSplit(long offset, long length) {
        firstRow = offset;
        rowCount = length;
      }

      @Override
	public long getLength() throws IOException {
        return 0;
      }

      @Override
	public String[] getLocations() throws IOException {
        return new String[]{};
      }

      @Override
	public void readFields(DataInput in) throws IOException {
        firstRow = WritableUtils.readVLong(in);
        rowCount = WritableUtils.readVLong(in);
      }

      @Override
	public void write(DataOutput out) throws IOException {
        WritableUtils.writeVLong(out, firstRow);
        WritableUtils.writeVLong(out, rowCount);
      }
    }
    
    /**
     * A record reader that will generate a range of numbers.
     */
    static class RangeRecordReader 
        extends RecordReader<LongWritable, NullWritable> {
      long startRow;
      long finishedRows;
      long totalRows;
      LongWritable key = null;

      public RangeRecordReader() {
      }
      
      @Override
	public void initialize(InputSplit split, TaskAttemptContext context) 
          throws IOException, InterruptedException {
        startRow = ((RangeInputSplit)split).firstRow;
        finishedRows = 0;
        totalRows = ((RangeInputSplit)split).rowCount;
      }

      @Override
	public void close() throws IOException {
        // NOTHING
      }

      @Override
	public LongWritable getCurrentKey() {
        return key;
      }

      @Override
	public NullWritable getCurrentValue() {
        return NullWritable.get();
      }

      @Override
	public float getProgress() throws IOException {
        return finishedRows / (float) totalRows;
      }

      @Override
	public boolean nextKeyValue() {
        if (key == null) {
          key = new LongWritable();
        }
        if (finishedRows < totalRows) {
          key.set(startRow + finishedRows);
          finishedRows += 1;
          return true;
        } else {
          return false;
        }
      }
      
    }

    @Override
	public RecordReader<LongWritable, NullWritable> 
        createRecordReader(InputSplit split, TaskAttemptContext context) 
        throws IOException {
      return new RangeRecordReader();
    }

    /**
     * Create the desired number of splits, dividing the number of rows
     * between the mappers.
     */
    @Override
	public List<InputSplit> getSplits(JobContext job) {
      long totalRows = getNumberOfRows(job);
      int numSplits = job.getConfiguration().getInt(MRJobConfig.NUM_MAPS, 1);
      LOG.info("Generating " + totalRows + " using " + numSplits);
      List<InputSplit> splits = new ArrayList<InputSplit>();
      long currentRow = 0;
      for(int split = 0; split < numSplits; ++split) {
        long goal = 
          (long) Math.ceil(totalRows * (double)(split + 1) / numSplits);
        splits.add(new RangeInputSplit(currentRow, goal - currentRow));
        currentRow = goal;
      }
      return splits;
    }

  }
  
  static long getNumberOfRows(JobContext job) {
    return job.getConfiguration().getLong(NUM_ROWS, 0);
  }
  
  static void setNumberOfRows(Job job, long numRows) {
    job.getConfiguration().setLong(NUM_ROWS, numRows);
  }

  /**
   * The Mapper class that given a row number, will generate the appropriate 
   * output line.
   */
  public static class SortGenMapper 
      extends Mapper<LongWritable, NullWritable, Text, Text> {

    private Text key = new Text();
    private Text value = new Text();
    private long rowId = -1;

    private Random RandomGenerator = new Random(System.currentTimeMillis());	
    
    @Override
	public void map(LongWritable row, NullWritable ignored,
        Context context) throws IOException, InterruptedException {
      
    	
    	
    	
    if (rowId == -1) {
        rowId = row.get();
      }

      dimension = context.getConfiguration().getInt("dimension", 1);
      maxcoordinate = context.getConfiguration().getInt("maxcoordinate", 1);
      
      
      Point point = new Point(dimension);
      for (int i = 0; i < dimension; i++) {
        point.set(i, RandomGenerator.nextInt(maxcoordinate));
      }

      Text temp = new Text("Label " + rowId);
      point.setInfo(temp);

      key.set(Point.print(point));
      value.set(point.printInfo());

      context.write(key, value);
      rowId++;
    }
  }

  private static void usage() throws IOException {
    System.err.println("mstgen <num rows> <dimension> <range> <output dir>");
  }

  /**
   * Parse a number that optionally has a postfix that denotes a base.
   * @param str an string integer with an option base {k,m,b,t}.
   * @return the expanded value
   */
  private static long parseHumanLong(String str) {
    char tail = str.charAt(str.length() - 1);
    long base = 1;
    switch (tail) {
    case 't':
      base *= 1000 * 1000 * 1000 * 1000;
      break;
    case 'b':
      base *= 1000 * 1000 * 1000;
      break;
    case 'm':
      base *= 1000 * 1000;
      break;
    case 'k':
      base *= 1000;
      break;
    default:
    }
    if (base != 1) {
      str = str.substring(0, str.length() - 1);
    }
    return Long.parseLong(str) * base;
  }
  
  /**
   * @param args the cli arguments
   */
  @Override
public int run(String[] args) 
      throws IOException, InterruptedException, ClassNotFoundException {


	Configuration conf = getConf();
	
	dimension = Integer.parseInt(args[1]);
	maxcoordinate = Integer.parseInt(args[2]);
	
	conf.setInt("dimension", dimension);
	conf.setInt("maxcoordinate", maxcoordinate);
	
    Job job = Job.getInstance(conf);
    if (args.length != 4) {
      usage();
      return 2;
    }

    setNumberOfRows(job, parseHumanLong(args[0]));
     
    Path outputDir = new Path(args[3]);
    /*
     * if (outputDir.getFileSystem(getConf()).exists(outputDir)) { throw new
     * IOException("Output directory " + outputDir + " already exists."); }
     */

    

    FileOutputFormat.setOutputPath(job, outputDir);
    job.setJobName("MSTGen");
    job.setJarByClass(MSTGen.class);
    job.setMapperClass(SortGenMapper.class);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass(RangeInputFormat.class);
    job.setOutputFormatClass(MSTOutputFormat.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    // Configuring log4j

      //BasicConfigurator.configure();
    int res = ToolRunner.run(new Configuration(), new MSTGen(), args);
    System.exit(res);
  }
}
