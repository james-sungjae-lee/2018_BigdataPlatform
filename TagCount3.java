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

public class TagCount {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        /*
         * Job 1: Count the number of Tags appeared
         */
        Job job1 = Job.getInstance(conf, "TagCount");
        job1.setJarByClass(TagCount.class);

        job1.setMapperClass(TagCounterMap.class);
        job1.setReducerClass(TagCounterReduce.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/temp"));

        job1.waitForCompletion(true);

        /*
         * Job 2: Sort based on the number of occurences
         */
        Job job2 = Job.getInstance(conf, "SortByCountValue");

        job2.setNumReduceTasks(1);

        job2.setJarByClass(TagCount.class);

        job2.setMapperClass(SortByValueMap.class);
        job2.setReducerClass(SortByValueReduce.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job2, new Path(args[1] + "/temp"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/final"));

        job2.waitForCompletion(true);
    }

    public static class TagCounterMap extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            String lastField = fields[2];
            String[] tags = lastField.split("\\|");

            for (String tag : tags) {
                word.set(tag);
                context.write(word, one);
            }
        }
    }

    public static class TagCounterReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class SortByValueMap extends Mapper<IntWritable, Text, Text, Text> {
        private Text frequency = new Text();
        private Text tempKey = new Text();

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
          String tempString = key.toString() + "," + value.toString();
          frequency.set(tempString);
          tempKey.set("tempKey");
          context.write(tempKey, frequency);
        }
    }

    public static class SortByValueReduce extends Reducer<Text, Text, Text, IntWritable> {
      private Text mostTagOut = new Text();
      IntWritable frequencyOut = new IntWritable();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

          String mostTag = new String();
          int max = 0;

          for(Text value : values){

            String line = value.toString();
            String[] fields = line.split(",");
            String tag = fields[0];
            int frequency = Integer.parseInt(fields[1]);

            if(max < frequency){
              max = frequency;
              mostTag = tag;
            }
          }
          frequencyOut.set(max);
          mostTagOut.set(mostTag);
          context.write(mostTagOut, frequencyOut);
        }
    }
}
