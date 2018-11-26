import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MovieName{

  public static void main(String[] args) throws Exception{
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "MovieName");
    job.setJarByClass(MovieName.class);

    job.setMapperClass(GetTitlesMap.class);
    job.setCombinerClass(DistinctTitleReduce.class);
    job.setReducerClass(CountTitlesReduce.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1] + "/temp"));

    job.waitForCompletion(true);
  }

  public static class GetTitlesMap extends Mapper<LongWritable, Text, Text, Text>{
    private Text title = new Text();

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
      String line = value.toString();
      String[] fields = line.split(",");
      String titleField = fields[1];

      title.set(titleField);
      context.write(title, title);
    }
  }

  public static class DistinctTitleReduce extends Reducer<Text, Text, Text, Text>{
    private Text distinctOutput = new Text();
    private Text sameKey = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
      HashSet<String> titles = new HashSet<>();
      for(Text value : values){
        titles.add(value.toString());
      }

      String tempKey = "result";
      sameKey.set(tempKey);

      Iterator itr = titles.iterator();
      while(itr.hasNext()){
        distinctOutput.set(itr.next().toString());
        context.write(sameKey, distinctOutput);
      }
    }
  }


  public static class CountTitlesReduce extends Reducer <Text, Text, Text, Text>{
    private Text result = new Text();
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
      int sum = 0;
      for(Text value : values){
        sum += 1;
      }
      String resultString = new Integer(sum).toString();
      result.set(resultString);
      context.write(key, result);
    }
  }
}





















//
