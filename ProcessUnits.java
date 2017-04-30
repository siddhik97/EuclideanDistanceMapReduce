import java.util.*;
import java.io.IOException;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class ProcessUnits
{
   //Mapper class
   public static class E_EMapper extends MapReduceBase implements
   Mapper<LongWritable,  /*Input key Type */
   Text,                   /*Input value Type*/
   Text,                   /*Output key Type*/
   IntWritable>            /*Output value Type*/
   {
      //Map function
      public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
      {
         String line = value.toString();
         String lasttoken = null;
         StringTokenizer s = new StringTokenizer(line,",");
         String id = s.nextToken();
         String value1 = s.nextToken();
         String value2 = s.nextToken();

     int intval1 = Integer.parseInt(value1);
     int intval2 = Integer.parseInt(value2);

     int difference = intval1-intval2;
     int square = (difference*difference);
         
         output.collect(new Text(id), new IntWritable(square));
      }
   }
   
   //Reducer class
    
   public static class E_EReduce extends MapReduceBase implements
   Reducer< Text, IntWritable, Text, IntWritable >
   {
      //Reduce function
      public void reduce(Text key, Iterator <IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
      {
        int sum = 0;
               while (values.hasNext()) {
             sum += values.next().get();
               }
        Double root = Math.sqrt(sum);
        System.out.println(root);
        int op = root.intValue();
        System.out.println(op);
        output.collect(key,new IntWritable(op));
      }
   }
    
   //Main function
    
   public static void main(String args[])throws Exception
   {
      JobConf conf = new JobConf(ProcessUnits.class);
        
      conf.setJobName("max_eletricityunits");
        
      conf.setOutputKeyClass(Text.class);
      conf.setOutputValueClass(IntWritable.class);
        
      conf.setMapperClass(E_EMapper.class);
      conf.setCombinerClass(E_EReduce.class);
      conf.setReducerClass(E_EReduce.class);
        
      conf.setInputFormat(TextInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);
        
      FileInputFormat.setInputPaths(conf, new Path(args[0]));
      FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        
      JobClient.runJob(conf);
   }
}
