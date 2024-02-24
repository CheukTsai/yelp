import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class predictREDUCE extends Reducer<Text, Text, Text, Text> {

    private int total;
    private int positive;
    private int negative;

    @Override
    protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        total = 0;
        positive = 0;
        negative = 0;
    }

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> itr = values.iterator();
        double prob = Double.parseDouble(key.toString());
        while (itr.hasNext()) {
            total++;
            Text val = itr.next();
            String line = val.toString();
            if (prob < 0.5) {
                negative++;
                context.write(new Text(String.valueOf(prob)), new Text(line));
            } else {
                positive++;
                context.write(new Text(String.valueOf(prob)), new Text(line));
            }
        }
    }

    @Override
    protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        for (int i = 0; i < 3; i++) {
            context.write(new Text(""), new Text(""));
        }
        context.write(new Text(""), new Text("=====================  final results  ====================="));
        context.write(new Text("Total"), new Text(String.valueOf(total)));
        context.write(new Text("Positive"), new Text(String.valueOf(positive)));
        context.write(new Text("Negative"), new Text(String.valueOf(negative)));
        int half = total / 2;
        int diff = Math.abs(positive - half);
        double accuracy = (double) Math.abs(diff - half) / half;
        context.write(new Text("Accuracy"), new Text(String.valueOf(accuracy * 100) + "%"));
        for (int i = 0; i < 3; i++) {
            context.write(new Text(""), new Text(""));
        }
        context.write(new Text(""), new Text("=====================  model weights  ====================="));
        for(int i = 0;i < 9;i++){
            double k = context.getConfiguration().getDouble("weight".concat(String.valueOf(i)),0);
            context.write(new Text("weight" + i), new Text(String.valueOf(k)));
        }
    }
}