import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;

import com.mongodb.hadoop.MongoInputFormat
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;

public class UniqueMovieNames{
  public static void main(String[] args) throws Exception{
    Configuration conf = new Configuration();

    MongoConfigUtil.setInputURI(conf, "mongodb://" + arg[0]);

    Job job = Job.getInstance(conf, "MovieNameAggregation");

    job.setJarByClass(UniqueMovieNames.class);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setInputFormatClass(MongoInputFormat.class);
    job.setOutputFormatClass(Text.class);

    job.waitForCompletion(true);
  }

  public static class Map extends Mapper<ObjectId, BSONObject, Text, Text>{
    private final Text movieIdOutput = new Text();
    private final Text titleOutput = new Text();

    public void map(ObjectId key, BSONObject value, Context context) throws IOException, InterruptedException{
      String movieId = value.get("movieId").toString();
      String title = value.get("title").toString();

      movieIdOutput.set(movieId);
      titleOutput.set(title);
      context.write(movieIdOutput, titleOutput);
    }
  }

  public static class Reduce extends Reducer<Text, Text, Text, Text>{
    private final Text reduceResult = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
      HashSet<String> titles = new HashSet<>();
      for(Text value : values){
        titles.add(value.toString());
      }

      Int result = 0;

      Iterator itr = titles.iterator();
      while(itr.hasNext()){
        reduceResult.set(itr.next().toString());
        context.write(key, reduceResult);
        result = result + 1;
      }
      System.out.print("result is : " + result);
    }
  }
}
