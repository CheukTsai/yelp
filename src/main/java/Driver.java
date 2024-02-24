import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
    public static int num_features; // needs to be set
    public static float alpha; // needs to be set

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //args[0] is the number of features each input has.
        num_features = Integer.parseInt(args[0]);
        ++num_features;
        //args[1] is the value of alpha that you want to use.
        alpha = Float.parseFloat(args[1]);
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.newInstance(conf);
        double[] weights = new double[num_features];
        //args[2] is the number of times you want to iterate over your training set.
        for (int i = 0; i < Integer.parseInt(args[2]); i++) {
            conf.setInt("iteration", i);
            //for the first run
            if (i == 0) {
                for (int i1 = 0; i1 < num_features; i1++) {
                    weights[i1] = 0.0;
                }
            }
            //for the second run
            else {
                int iter = 0;
                //args[4] is the output path for storing the theta values.
                BufferedReader br1 = new BufferedReader(new InputStreamReader(hdfs.open(new Path(args[4] + "/part-r-00000"))));
                String line1 = null;
                boolean firstLine = true;
                while ((line1 = br1.readLine()) != null) {
                    if (firstLine) {
                        firstLine = false;
                        continue;
                    }
                    String[] theta_line = line1.split("\t");
                    weights[iter] = Double.parseDouble(theta_line[1]);
                    iter++;
                }
                br1.close();
            }
            if (hdfs.exists(new Path(args[4]))) {
                hdfs.delete(new Path(args[4]), true);
            }
            conf.setFloat("alpha", alpha);
            //Theta Value Initialisation
            for (int j = 0; j < num_features; j++) {
                conf.setDouble("weight".concat(String.valueOf(j)), weights[j]);
            }
            Job job = new Job(conf, "Calculation of Theta");
            job.setJarByClass(Driver.class);
            FileInputFormat.setInputPaths(job, new Path(args[3]));
            FileOutputFormat.setOutputPath(job, new Path(args[4]));
            job.setMapperClass(thetaMAP.class);
            job.setReducerClass(thetaREDUCE.class);
            job.setNumReduceTasks(1); // Set the number of reduce tasks (partitions)
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.waitForCompletion(true);
        }

        // predict phase

        int iter = 0;
        //args[4] is the output path for storing the theta values.
        BufferedReader br1 = new BufferedReader(new InputStreamReader(hdfs.open(new Path(args[4] + "/part-r-00000"))));
        String line1 = null;
        boolean firstLine = true;
        while ((line1 = br1.readLine()) != null) {
            if (firstLine) {
                firstLine = false;
                continue;
            }
            String[] theta_line = line1.split("\t");
            weights[iter] = Double.parseDouble(theta_line[1]);
            iter++;
        }
        br1.close();

        for (int j = 0; j < num_features; j++) {
            conf.setDouble("weight".concat(String.valueOf(j)), weights[j]);
        }
        Job job = new Job(conf, "Predict");
        job.setJarByClass(Driver.class);
        //args[3] is the input path.
        FileInputFormat.setInputPaths(job, new Path(args[5]));
        FileOutputFormat.setOutputPath(job, new Path(args[6]));
        job.setMapperClass(predictMAP.class);
        job.setReducerClass(predictREDUCE.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.waitForCompletion(true);

        hdfs.close();
    }
}