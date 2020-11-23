package map_reduce.job2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ClassifierMapper extends Mapper<Object, Text, Text, Text> {

    private final Text interest = new Text();
    private final Text idVG = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] arrayString = value.toString().split("\t");
        double avg = Double.parseDouble(arrayString[1]);

        if(avg <= Interest.LOW.getValue())
            interest.set(Interest.LOW.name());
        else if(Interest.LOW.getValue() < avg && avg < Interest.MEDIUM.getValue())
            interest.set(Interest.MEDIUM.name());
        else if(Interest.MEDIUM.getValue() < avg && avg < Interest.HIGH.getValue())
            interest.set(Interest.HIGH.name());

        idVG.set(key.toString());
        context.write(interest, idVG);
    }
}