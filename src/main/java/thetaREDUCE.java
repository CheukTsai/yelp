import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class thetaREDUCE extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        int iteration = context.getConfiguration().getInt("iteration",0);
        context.write(new Text("iteration"), new Text(String.valueOf(iteration)));
    }

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> itr = values.iterator();
        double sum = 0.0;
        int count = 0;
        while (itr.hasNext()) {
            Text val = itr.next();
            double d = Double.parseDouble(val.toString());
            sum += d;
            count++;
        }
        context.write(key, new Text(String.valueOf(sum / count)));
    }
}