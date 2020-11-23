package map_reduce.Utils;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class ReviewOrBrand implements Writable {

    private Review review = new Review();
    private Brand brand = new Brand();
    private boolean isReview = true;

    public ReviewOrBrand(Review review){
        this.review = review;

    }

    public ReviewOrBrand(Brand brand){
        this.brand = brand;
        isReview = false;
    }

    public ReviewOrBrand(){}

    public boolean isReview(){
        return this.isReview;
    }

    public static ReviewOrBrand read(DataInput in) throws IOException {
        ReviewOrBrand rob = new ReviewOrBrand();
        rob.readFields(in);
        return rob;
    }


    @Override
    public void write(DataOutput out) throws IOException {

        this.review.write(out);
        this.brand.write(out);
        out.writeBoolean(isReview);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.review = Review.read(in);
        this.brand = Brand.read(in);
        this.isReview = in.readBoolean();

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReviewOrBrand that = (ReviewOrBrand) o;
        return  Objects.equals(review, that.review) &&
                Objects.equals(brand, that.brand) &&
                isReview == that.isReview;
    }

    @Override
    public String toString(){
        return review + "\t" + brand + "\t" + isReview;
    }

    public Review getReview() {
        return review;
    }

    public Brand getBrand() {
        return brand;
    }

    public void setReview(Review review) {
        this.review = review;
    }

    public void setBrand(Brand brand) {
        this.brand = brand;
    }
}
