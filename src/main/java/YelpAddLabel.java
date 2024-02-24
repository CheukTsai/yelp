import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.checkerframework.checker.units.qual.A;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.*;

public class YelpAddLabel {

	public static class CSVMapper
			extends Mapper<Object, Text, Text, Text> {

		List<Node> list;

		@Override
		protected void setup(Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			super.setup(context);
			list = new ArrayList<>();
		}

		public void map(Object key, Text value, Context context
		) throws IOException, InterruptedException {
			String line = value.toString();
			String[] lineArray = line.split("\t");
			if (lineArray.length < 12) return;
			double val = calculateLabel(lineArray);
			list.add(new Node(lineArray, val));
		}

		private double calculateLabel(String[] array) {
			double label = 0;

			label += (double) Long.parseLong(array[Constant.USER_REVIEW_COUNT_INDEX]) * Constant.USER_REVIEW_COUNT_FACTOR;
			label += (double) Long.parseLong(array[Constant.USER_USEFUL_INDEX]) * Constant.USER_USEFUL_FACTOR;
			label += (double) Long.parseLong(array[Constant.USER_FUNNY_INDEX]) * Constant.USER_FUNNY_FACTOR;
			label += (double) Long.parseLong(array[Constant.USER_COOL_INDEX]) * Constant.USER_COOL_FACTOR;
			label += (double) Long.parseLong(array[Constant.REVIEW_USEFUL_INDEX]) * Constant.REVIEW_USEFUL_FACTOR;
			label += (double) Long.parseLong(array[Constant.REVIEW_FUNNY_INDEX]) * Constant.REVIEW_FUNNY_FACTOR;
			label += (double) Long.parseLong(array[Constant.REVIEW_COOL_INDEX]) * Constant.REVIEW_COOL_FACTOR;
			label += Double.parseDouble(array[Constant.REVIEW_TEXT_LEN_INDEX]) * Constant.REVIEW_TEXT_LEN;
			label += Double.parseDouble(array[Constant.REVIEW_TEXT_SENTIMENT_INDEX]) * Constant.REVIEW_TEXT_SENTIMENT_FACTOR;

			return label;
		}

		@Override
		protected void cleanup(Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			Collections.sort(list, Comparator.comparingDouble(a -> a.val));
			for (int i = 0; i < list.size() / 2; i++) {
				Node node = list.get(i);
				context.write(new Text(String.join("\t", node.arr)), new Text("0"));
			}
			for (int i = list.size() / 2; i < list.size(); i++) {
				Node node = list.get(i);
				context.write(new Text(String.join("\t", node.arr)), new Text("1"));
			}
		}

		class Node {
			String[] arr;
			double val;

			public Node(String[] arr, double val) {
				this.arr = arr;
				this.val = val;
			}
		}
	}

	public static class CSVReducer extends Reducer<Text,Text,Text, Text> {
		public void reduce(Text key, Iterable<Text> values,
						   Context context
		) throws IOException, InterruptedException {
			Iterator<Text> itr = values.iterator();
			while (itr.hasNext()) {
				Text val = itr.next();
				context.write(key, val);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "yelp_user");
		job.setJarByClass(YelpAddLabel.class);
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