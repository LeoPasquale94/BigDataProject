package map_reduce.job1;

import map_reduce.Utils.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AvgReviewReducer extends Reducer<Text, ReviewOrBrand, Text, AvgReview> {


    public void reduce(Text idVg, Iterable<ReviewOrBrand> values, Context context) throws IOException, InterruptedException {

            Avg avg = new Avg();
            String nameBrand = "";
            AvgReview avgReview = new AvgReview();

            for ( ReviewOrBrand reviewOrBrand : values){
                if(reviewOrBrand.isReview()){
                    Review review = reviewOrBrand.getReview();
                    avg.add(review.getOverall());
                } else{
                    Brand brand = reviewOrBrand.getBrand();
                    nameBrand = brand.getName();
                }
            }

            setAvgReview(idVg, nameBrand, avg, avgReview);
            context.write(idVg, avgReview);

    }

    private void setAvgReview(Text idVg, String nameBrand, Avg avg , AvgReview avgReview){
        avgReview.setIdRW(idVg.toString());
        avgReview.setBrand(nameBrand);
        avgReview.setAvg(avg);
    }



}
