import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class predictMAP extends Mapper<Object, Text, Text, Text> {
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
        if (columns.length != 9 + skippedColumns) return;
        ++count;
        // skip first column and last column is the label
        int skip = 3;
        int i = skip;
        double[] data = new double[columns.length - skip];
        for (; i <columns.length; i++) {
            data[i - skip] = Double.parseDouble(columns[i]);
        }
        Instance instance = new Instance(data, line);
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
        public double[] x;
        public String line;

        public Instance(double[] x, String line) {
            this.x = x;
            this.line = line;
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for (int i=0; i < instances.size(); i++) {
            Instance instance = instances.get(i);
            double[] x = instances.get(i).x;
            double predicted = classify(x);
            if (predicted > 1 || predicted < 0) continue;
            context.write(new Text(String.valueOf(predicted)), new Text(instance.line));
        }
    }
}