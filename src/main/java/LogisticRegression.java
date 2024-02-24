import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;

public class LogisticRegression extends Mapper<Object, Text, Text, Text> {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "logistic regression");
        job.setJarByClass(LogisticRegression.class);
        job.setMapperClass(CSVMapper.class);
        job.setReducerClass(CSVReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        System.gc();
    }


    public static class CSVMapper
            extends Mapper<Object, Text, Text, Text>{

        public static class Instance {
            public int label;
            public int[] x;

            public Instance(int label, int[] x) {
                this.label = label;
                this.x = x;
            }
        }

        public static class Logistic {
            public double rate;
            public double[] weights;
            private static final int ITERATIONS = 100;

            private static double sigmoid(double z) {
                return 1.0 / (1.0 + Math.exp(-z));
            }

        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            

        }
    }

    public static class CSVReducer extends Reducer<Text,Text,Text, NullWritable> {
        @Override
        protected void setup(Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
            NullWritable nw = NullWritable.get();
            context.write(new Text("user_id,name,review_count,useful,funny,cool"), nw);
        }

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




}
