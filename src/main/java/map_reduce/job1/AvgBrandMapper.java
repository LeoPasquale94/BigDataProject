package map_reduce.job1;

import map_reduce.Utils.Avg;
import map_reduce.Utils.AvgReview;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AvgBrandMapper extends Mapper<Object, Text, Text, Avg> {

    public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
        Text brand = new Text();
        AvgReview avgReview = parsingInputString(line);

        Avg avg = avgReview.getAvg();
        brand.set(avgReview.getBrand());
        context.write(brand, avg);
    }

    private AvgReview parsingInputString(Text line  ){
        AvgReview avgReview = new AvgReview();
        String[] items = line.toString().split("\t");
       if (items.length == 5) {
            avgReview.setIdRW(items[1]);
            avgReview.setBrand(items[2]);
            avgReview.setAvg(stringToDouble(items[3].trim()), stringToInt(items[4].trim()));
        }
       return avgReview;
    }

    private double stringToDouble(String string){
        return Double.parseDouble(string);
    }

    private int stringToInt(String string){return Integer.parseInt(string);}
}
