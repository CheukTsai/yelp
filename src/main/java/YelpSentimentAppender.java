
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

public class YelpSentimentAppender {

    public static class CSVMapper
            extends Mapper<Object, Text, Text, Text>{

        private Set<String> positiveWords;
        private Set<String> negativeWords;

        @Override
        protected void setup(Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            super.setup(context);

            positiveWords = new HashSet<>();
            negativeWords = new HashSet<>();

            URI[] cacheFiles = DistributedCache.getCacheFiles(context.getConfiguration());

            // Retrieve positive-words.txt from DistributedCache
            positiveWords = loadWordsFromDistributedCache(context, cacheFiles, "positive-words.txt");

            // Retrieve negative-words.txt from DistributedCache
            negativeWords = loadWordsFromDistributedCache(context, cacheFiles, "negative-words.txt");
        }

        private Set<String> loadWordsFromDistributedCache(Context context, URI[] cacheFiles, String fileName) throws IOException {
            Set<String> words = new HashSet<>();

            if (cacheFiles != null) {
                for (URI cacheFile : cacheFiles) {
                    if (cacheFile.getPath().endsWith(fileName)) {
                        try (BufferedReader br = new BufferedReader(new FileReader(cacheFile.getPath()))) {
                            String line;
                            while ((line = br.readLine()) != null) {
                                words.add(line.trim());
                            }
                        }
                    }
                }
            }

            return words;
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tmp = line.split("\t");


            List<String> outputVal = new ArrayList<>(Arrays.asList(tmp));
            String text = tmp[tmp.length - 1];
            outputVal.add(2, text);
            outputVal.remove(outputVal.size() - 1);

            text = text.replaceAll("[^\\sa-zA-Z0-9]", "");
            String[] words = text.split(" ");
            int sum = 0, cnt = 0;

            for (String word : words) {
                if (word.isEmpty()) continue;
                cnt++;
                word = word.trim();
                if (positiveWords.contains(word)) sum++;
                if (negativeWords.contains(word)) sum--;
            }

            double avg = cnt == 0 ? 0.0 : (double) sum / cnt;
            outputVal.add(String.valueOf(cnt));
            context.write(new Text(String.join("\t", outputVal)), new Text(String.valueOf(avg)));
        }
    }

    public static class CSVReducer extends Reducer<Text,Text,Text,Text> {
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

    public static class CustomPartitioner extends Partitioner<Text, Text> {
        private double partitionRatio = 0.2; // 20% in one partition, 80% in another

        public int getPartition(Text key, Text value, int numPartitions) {
            // Use random number to determine partition
            if (Math.random() < partitionRatio) {
                return 0; // 20% in partition 0
            } else {
                return 1; // 80% in partition 1
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "yelp_user");
        // Add positive-words.txt to DistributedCache
        DistributedCache.addCacheFile(new URI("positive-words.txt"), job.getConfiguration());

        // Add negative-words.txt to DistributedCache
        DistributedCache.addCacheFile(new URI("negative-words.txt"), job.getConfiguration());

        job.setJarByClass(YelpSentimentAppender.class);
        job.setMapperClass(CSVMapper.class);
        job.setReducerClass(CSVReducer.class);
        job.setPartitionerClass(CustomPartitioner.class); // Set custom partitioner
        job.setNumReduceTasks(2); // Set the number of reduce tasks (partitions)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}