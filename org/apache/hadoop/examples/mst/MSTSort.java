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

import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

/** 
 * Generates the sampled split points, launches the job, and waits for it to
 * finish. 
 * <p>
 * To run the program: 
 * <b>bin/hadoop jar hadoop-*-examples.jar terasort in-dir out-dir</b>
 */
public class MSTSort extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(MSTSort.class);

  public static int dimension;
  public static int blocksize;
  public static int maxcoordinate;

  static String SIMPLE_PARTITIONER = "mapreduce.terasort.simplepartitioner";
  static String OUTPUT_REPLICATION = "mapreduce.terasort.output.replication";

    /**
   * A total order partitioner that assigns keys based on their first 
   * PREFIX_LENGTH bytes, assuming a flat distribution.
   */
  public static class SimplePartitioner extends Partitioner<Text, Text>
      implements Configurable {
    private Configuration conf = null;
    

    @Override
	public void setConf(Configuration conf) {
      this.conf = conf;
    }
    
    @Override
	public Configuration getConf() {
      return conf;
    }
    
    @Override
    public int getPartition(Text key, Text value, int numPartitions) {      	
      StringTokenizer st = new StringTokenizer(key.toString(), " ");
      st.nextToken();
      int ret = 0;
      
      blocksize = conf.getInt("blocksize", 1);
      maxcoordinate = conf.getInt("maxcoordinate", 1);
      dimension = conf.getInt("dimension", 1);
      
      System.err.println("blocksize = " + blocksize);
      System.err.println("maxcoordinate = " + maxcoordinate);
      System.err.println("dimension = " + dimension);
      
      int blocklength = maxcoordinate / blocksize;
      for (int i = 0; i < dimension; i++) {
        ret *= blocksize;
        int tmp = Integer.parseInt(st.nextToken());
        ret += tmp / blocklength;
      }
      return ret;
    }
  }

  public static boolean getUseSimplePartitioner(JobContext job) {
    return job.getConfiguration().getBoolean(SIMPLE_PARTITIONER, true);
  }

  public static void setUseSimplePartitioner(Job job, boolean value) {
	  job.getConfiguration().setBoolean(SIMPLE_PARTITIONER, value);
  }

  public static int getOutputReplication(JobContext job) {
    return job.getConfiguration().getInt(OUTPUT_REPLICATION, 1);
  }

  public static void setOutputReplication(Job job, int value) {
    job.getConfiguration().setInt(OUTPUT_REPLICATION, value);
  }

  @Override
public int run(String[] args) throws Exception {
    LOG.info("starting");
    Job job = Job.getInstance(getConf());

    dimension = Integer.parseInt(args[0]);
    maxcoordinate = Integer.parseInt(args[1]);
    Path inputDir = new Path(args[2]);
    Path outputDir = new Path(args[3]);
    
    FileInputFormat.setInputPaths(job, inputDir);
    FileOutputFormat.setOutputPath(job, outputDir);

    // Set number of reducers to be a perfect square

    int reducers = job.getConfiguration().getInt(MRJobConfig.NUM_REDUCES, 1);
    blocksize = (int) Math.pow(reducers, 1.0 / dimension);
    reducers = 1;
    for (int i = 0; i < dimension; i++) {
      reducers *= blocksize;
    }
    job.getConfiguration().setInt(MRJobConfig.NUM_REDUCES, reducers);

    job.getConfiguration().setInt("blocksize", blocksize);
    job.getConfiguration().setInt("maxcoordinate", maxcoordinate);
    job.getConfiguration().setInt("dimension", dimension);
    
    setUseSimplePartitioner(job, true);

    
    
    System.err.println("Block size = " + blocksize);
    System.err.println("Reducers = " + reducers);
    

    
    System.err.println(job.getConfiguration()
        .getInt(MRJobConfig.NUM_REDUCES, 1));

    job.setJobName("MSTSort");
    job.setJarByClass(MSTSort.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass(MSTInputFormat.class);
    job.setOutputFormatClass(MSTOutputFormat.class);
    
    job.setPartitionerClass(SimplePartitioner.class);

    
    
    job.getConfiguration().setInt("dfs.replication", getOutputReplication(job));
    MSTOutputFormat.setFinalSync(job, true);
    int ret = job.waitForCompletion(true) ? 0 : 1;
    LOG.info("done");
    return ret;
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    // Configuring log4j
    BasicConfigurator.configure();

    int res = ToolRunner.run(new Configuration(), new MSTSort(), args);
    System.exit(res);
  }

}
