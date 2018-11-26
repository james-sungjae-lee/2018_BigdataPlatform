import java.io.IOException;

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
    job.setJarByClass(MovieNam.class);

    job.setMapperClass(GetTitlesMap.class);
    job.setReducerClass(CountTitlesReduce.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);
  }

  public static class GetTitlesMap extends Mapper<LongWritable, Text, Text, IntWritable>{
    private final static IntWritable one = new IntWritable(1);
    private Text title = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
      String line = value.toString();
      String[] fields = line.split(",");
      String titleField = field[1];

      title.set(titleField);
      context.write(title, one);
    }
  }

  public static class CountTitlesReduce extends Reducer <Text, IntWritable, Text, IntWritable>{
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
      int sum = 0;
      for(IntWritable value : values){
        sum = value.get();
      }
      context.write(key, new IntWritable(sum));
    }
  }
}





















//
