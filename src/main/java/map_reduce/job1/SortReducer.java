package map_reduce.job1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

    public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text idVG : values) {
            context.write(idVG, key);
        }
    }
}
