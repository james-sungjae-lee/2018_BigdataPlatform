package kr.ac.kookmin.cs.bigdata;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Create a new job
        Job job = Job.getInstance(conf, "wordcount");

        // Use the WordCount.class file to point to the job jar
        job.setJarByClass(WordCount.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setNumReduceTasks(1);
        // Setting the input and output locations
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Submit the job and wait for it's completion
        job.waitForCompletion(true);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            String ans[] = new String[6];
            /* JSON parsing */
            for(int i = 0; i<fields.length;i++)
            {
                if(fields[i].contains("\"reviewerID\""))
                {
                    String[] tmp = fields[i].split(":");
                    ans[0] = tmp[1];
                }
                else if(fields[i].contains("\"asin\""))
                {
                    String[] tmp = fields[i].split(":");
                    ans[1] = tmp[1];
                }
                else if(fields[i].contains("\"overall\""))
                {
                    String[] tmp = fields[i].split(":");
                    ans[2] = tmp[1];
                }
            }

            String result = "";
            String dot = ",";
            for(int i =0; i<2; i++)
            {
                result = result.concat(ans[i]);
                result = result.concat(dot);
            }
            /* Get Rev2 OverAll Value */
            double overall = Double.parseDouble(ans[2]);
            double rev2_overall = ((overall-1.0)/2.0) - 1;

            word.set(result);
            context.write(word,new DoubleWritable(rev2_overall));
        }
    }

    public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            int s = 0;

            // Sum all the occurrences of the word (key)
            for (DoubleWritable value : values) {
                sum += value.get();
                s += 1;
            }
            /* 한 유저가 동일한 상품에 대해서 여러개의 평점을 남긴경우, 그 유저의 평가는 무시합니다. */
            if (s == 1)
                context.write(key, new DoubleWritable(sum));
        }
    }
}
