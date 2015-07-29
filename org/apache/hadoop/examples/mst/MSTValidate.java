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

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Generate 1 mapper per a file that checks to make sure the keys
 * are sorted within each file. The mapper also generates 
 * "$file:begin", first key and "$file:end", last key. The reduce verifies that
 * all of the start/end items are in order.
 * Any output from the reduce is problem report.
 * <p>
 * To run the program: 
 * <b>bin/hadoop jar hadoop-*-examples.jar teravalidate out-dir report-dir</b>
 * <p>
 * If there is any output, something is wrong and the output of the reduce
 * will have the problem report.
 */
public class MSTValidate extends Configured implements Tool {
  private static final Text ERROR = new Text("error");

  
  private static String textifyBytes(Text t) {
    BytesWritable b = new BytesWritable();
    b.set(t.getBytes(), 0, t.getLength());
    return b.toString();
  }

  static class ValidateMapper extends Mapper<Text,Text,Text,Text> {
    private Text lastKey;
    private String filename;

    /**
     * Get the final part of the input name
     * @param split the input split
     * @return the "part-r-00000" for the input
     */
    private String getFilename(FileSplit split) {
      return split.getPath().getName();
    }

    @Override
	public void map(Text key, Text value, Context context) 
        throws IOException, InterruptedException {
      if (lastKey == null) {
        FileSplit fs = (FileSplit) context.getInputSplit();
        filename = getFilename(fs);
        context.write(new Text(filename + ":begin"), key);
        lastKey = new Text();
      } else {
        if (key.compareTo(lastKey) < 0) {
          context.write(ERROR, new Text("misorder in " + filename + 
                                         " between " + textifyBytes(lastKey) + 
                                         " and " + textifyBytes(key)));
        }
      }
      lastKey.set(key);
    }
    
    @Override
	public void cleanup(Context context) 
        throws IOException, InterruptedException  {
      if (lastKey != null) {
        context.write(new Text(filename + ":end"), lastKey);
      }
    }
  }

  /**
   * Check the boundaries between the output files by making sure that the
   * boundary keys are always increasing.
   * Also passes any error reports along intact.
   */
  static class ValidateReducer extends Reducer<Text,Text,Text,Text> {
    private boolean firstKey = true;
    private Text lastKey = new Text();
    private Text lastValue = new Text();
    @Override
	public void reduce(Text key, Iterable<Text> values,
        Context context) throws IOException, InterruptedException  {
      if (ERROR.equals(key)) {
        for (Text val : values) {
          context.write(key, val);
        }
      } else {
        Text value = values.iterator().next();
        if (firstKey) {
          firstKey = false;
        } else {
          if (value.compareTo(lastValue) < 0) {
            context.write(ERROR, 
                           new Text("bad key partitioning:\n  file " + 
                                    lastKey + " key " + 
                                    textifyBytes(lastValue) +
                                    "\n  file " + key + " key " + 
                                    textifyBytes(value)));
          }
        }
        lastKey.set(key);
        lastValue.set(value);
      }
    }
    
  }

  private static void usage() throws IOException {
    System.err.println("teravalidate <out-dir> <report-dir>");
  }

  @Override
public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf());
    if (args.length != 2) {
      usage();
      return 1;
    }
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setJobName("MSTValidate");
    job.setJarByClass(MSTValidate.class);
    job.setMapperClass(ValidateMapper.class);
    job.setReducerClass(ValidateReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    // force a single reducer
    job.setNumReduceTasks(1);
    // force a single split 
    FileInputFormat.setMinInputSplitSize(job, Long.MAX_VALUE);
    job.setInputFormatClass(MSTInputFormat.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new MSTValidate(), args);
    System.exit(res);
  }

}
