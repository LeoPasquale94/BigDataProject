package map_reduce.job1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<Object, Text, DoubleWritable, Text> {

    private final Text idVG = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] pairsKV = value.toString().split("\t");
        idVG.set(pairsKV[0]);
        DoubleWritable avg = new DoubleWritable(Double.parseDouble(pairsKV[1]));

        context.write(avg, idVG);
    }
}
