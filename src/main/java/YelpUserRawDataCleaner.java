import java.io.IOException;
import java.util.Iterator;

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

public class YelpUserRawDataCleaner {

    public static class CSVMapper
            extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            StringBuilder sb = new StringBuilder();
            try {
                Object obj = new JSONParser().parse(line);
                JSONObject jsonObject = (JSONObject) obj;

                String user_id = (String) jsonObject.get("user_id");
                String name = (String) jsonObject.get("name");
                long review_count = (long) jsonObject.get("review_count");
                long useful = (long) jsonObject.get("useful");
                long funny = (long) jsonObject.get("funny");
                long cool = (long) jsonObject.get("cool");

                sb.append(user_id).append(",");
                sb.append(name).append(",");
                sb.append(review_count).append(",");
                sb.append(useful).append(",");
                sb.append(funny).append(",");
                sb.append(cool);

                context.write(new Text(user_id), new Text(sb.toString()));

            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    public static class CSVReducer extends Reducer<Text,Text,Text,NullWritable> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            NullWritable nw = NullWritable.get();
            Iterator<Text> itr = values.iterator();
            while (itr.hasNext()) {
                Text val = itr.next();
                context.write(val,nw);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "yelp_user");
        job.setJarByClass(YelpUserRawDataCleaner.class);
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