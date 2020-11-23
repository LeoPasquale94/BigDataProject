package map_reduce.job2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReviewsClassificationReducer extends Reducer<Text, Text, Text, IntWritable> {

    private int countReviews = 0;
    private IntWritable numberOfReviews = new IntWritable();

    public void reduce(Text key, Iterable<Text> idVGs, Context context) throws IOException, InterruptedException {
        for (Text ignored : idVGs) {
            countReviews++;
        }
        numberOfReviews.set(countReviews);
        context.write(key, numberOfReviews);
    }
}