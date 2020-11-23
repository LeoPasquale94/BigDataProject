package map_reduce.job2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AverageReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private double sum = 0.0;
    private int count = 0;

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        for (DoubleWritable val : values) {
            sum += val.get();
            count++;
        }
        DoubleWritable avg = new DoubleWritable(sum / count);
        context.write(key, avg);
    }
}