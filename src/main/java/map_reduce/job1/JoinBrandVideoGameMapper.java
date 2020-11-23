package map_reduce.job1;

import map_reduce.Utils.Brand;
import map_reduce.Utils.ParserJson;
import map_reduce.Utils.ReviewOrBrand;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONException;


import java.io.IOException;

public class JoinBrandVideoGameMapper extends Mapper<Object, Text, Text, ReviewOrBrand> {

    private String idVG = "";
    private String nameBrand = "";
    private final Brand brand = new Brand();


    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            idVG = ParserJson.parser(value.toString(), "asin");
            nameBrand = ParserJson.parser(value.toString(), "brand");
        } catch (JSONException e) {
            e.printStackTrace();
        }
        brand.setIdBrand(idVG);
        brand.setName(nameBrand);
        context.write(new Text(idVG), new ReviewOrBrand(brand));
    }

}
