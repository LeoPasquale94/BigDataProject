package map_reduce.job1;

import map_reduce.Utils.Avg;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AvgBrandReducer extends Reducer<Text, Avg, Text, DoubleWritable> {



    public void reduce(Text brand, Iterable<Avg> avgs, Context context) throws IOException, InterruptedException {
        Avg totalAvg = new Avg();
        DoubleWritable avg= new DoubleWritable();

        for ( Avg partialAvg: avgs){
            totalAvg.mergeAvg(partialAvg);
        }
        avg.set(totalAvg.computeAvg());
        context.write(brand, avg);

    }
}
