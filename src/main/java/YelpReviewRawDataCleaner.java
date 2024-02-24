import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.*;

public class YelpReviewRawDataCleaner {



    public static class CSVMapper
            extends Mapper<Object, Text, Text, Text>{


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            StringBuilder sb = new StringBuilder();
            try {
                Object obj = new JSONParser().parse(line);
                JSONObject jsonObject = (JSONObject) obj;

                String review_id = (String) jsonObject.get("review_id");
                String user_id = (String) jsonObject.get("user_id");
                long useful = (long) jsonObject.get("useful");
                long funny = (long) jsonObject.get("funny");
                long cool = (long) jsonObject.get("cool");
                String text = (String) jsonObject.get("text");

                text = text.replaceAll("\r\n", "");
                text = text.replaceAll("\n", "");
                text = text.replaceAll("\r", "");

                sb.append(review_id).append(",");
                sb.append(user_id).append(",");
                sb.append(useful).append(",");
                sb.append(funny).append(",");
                sb.append(cool).append(",");
                sb.append(text).append(",");
                context.write(new Text(review_id), new Text(sb.toString()));

            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    public static class CSVReducer extends Reducer<Text,Text,Text,NullWritable> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            Iterator<Text> itr = values.iterator();
            while (itr.hasNext()) {
                Text val = itr.next();
                context.write(val, NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "yelp_user");
        job.setJarByClass(YelpReviewRawDataCleaner.class);
        job.setMapperClass(CSVMapper.class);
        job.setReducerClass(CSVReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        System.gc();
    }
}