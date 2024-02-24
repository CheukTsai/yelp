import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class thetaMAP extends Mapper<Object, Text, Text, Text> {
    public static int count = 0;
    public static double rate = 0.00001;
    public static int skippedColumns = 3;
    private double[] weights;
    private List<Instance> instances;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        instances = new ArrayList<>();
        weights = new double[9];
        for(int i = 0;i < 9;i++){
            weights[i] = context.getConfiguration().getDouble("weight".concat(String.valueOf(i)),0);
        }
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columns = line.split("\t");
        if (columns.length != 9 + skippedColumns + 1) return;
        ++count;
        // skip first column and last column is the label
        int skip = 3;
        int i = skip;
        double[] data = new double[columns.length - skip - 1];
        for (; i <columns.length-1; i++) {
            data[i-skip] = Double.parseDouble(columns[i]);
        }
        int label = (int) Double.parseDouble(columns[columns.length - 1]);
        Instance instance = new Instance(label, data);
        instances.add(instance);
    }

    private double classify(double[] x) {
        double logit = .0;
        for (int i=0; i<weights.length;i++)  {
            logit += weights[i] * x[i];
        }
        return sigmoid(logit);
    }
    private static double sigmoid(double z) {
        return 1.0 / (1.0 + Math.exp(-z));
    }

    public static class Instance {
        public int label;
        public double[] x;

        public Instance(int label, double[] x) {
            this.label = label;
            this.x = x;
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for (int i=0; i < instances.size(); i++) {
            double[] x = instances.get(i).x;
            double predicted = classify(x);
            int label = instances.get(i).label;
            for (int j=0; j<weights.length; j++) {
                weights[j] = weights[j] + rate * (label - predicted) * x[j];
            }
        }
        for (int i = 0; i < weights.length; i++) {
            double weight = weights[i];
            context.write(new Text("weight" + i), new Text(String.valueOf(weight)));
        }
    }
}