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

public class GenreCount{

  public static void main(String[] args) throws Exception{
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "GenreCount");
    job.setNumReduceTasks(1);
    job.setJarByClass(MovieName.class);

    job.setMapperClass(GetGenreMap.class);
    job.setReducerClass(CountReduce.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);
  }

  public static class GetGenreMap extends Mapper<LongWritable, Text, Text, Text>{
    private Text tempKey = new Text();

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
      String line = value.toString();
      String[] fields = line.split(",");
      String lastField = fields[fields.length - 1];
      String[] genres = lastField.split("\\|");
      String filmnoir = new String("Film-Noir");

      tempKey.set(genre);

      for (String genre : genres){
        if(genre == filmnoir){
          context.write(tempKey, tempKey);
        }
      }

      title.set(titleField);
      context.write(title, title);
    }
  }


  public static class CountReduce extends Reducer <Text, Text, Text, Text>{
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
