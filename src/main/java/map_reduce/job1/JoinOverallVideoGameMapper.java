package map_reduce.job1;


import map_reduce.Utils.ParserJson;
import map_reduce.Utils.Review;
import map_reduce.Utils.ReviewOrBrand;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONException;


import java.io.IOException;

public class JoinOverallVideoGameMapper extends Mapper<Object, Text, Text, ReviewOrBrand> {

    private String idVG = "";
    private Double overall = 0.0;
    private final Review review = new Review();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
           idVG = ParserJson.parser(value.toString(),"asin");
           overall = Double.parseDouble( ParserJson.parser(value.toString(),"overall"));
        } catch (JSONException e) {
            e.printStackTrace();
        }
        review.setIdRW(idVG);
        review.setOverall(overall);

        context.write(new Text(idVG), new ReviewOrBrand(review));
    }


  }