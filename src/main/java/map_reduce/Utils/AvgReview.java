package map_reduce.Utils;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class AvgReview implements Writable {

    private String idRW = "";
    private String brand = "";
    private Avg avg = new Avg();

    public AvgReview(String idRW, String brand, Avg avg) {
        this.idRW = idRW;
        this.brand = brand;
        this.avg.setN(avg.getN());
        this.avg.setSum(avg.getSum());
    }
    public AvgReview(String idRW, String brand, Double sum, int n) {
        this.idRW = idRW;
        this.brand = brand;
        this.avg = new Avg(sum,n);
    }
    public AvgReview(){}

    public static AvgReview read(DataInput in) throws IOException {
        AvgReview avgReview = new AvgReview();
        avgReview.readFields(in);
        return avgReview;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(idRW);
        out.writeUTF(brand);
        this.avg.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
       this.idRW = in.readUTF();
       this.brand = in.readUTF();
       this.avg = Avg.read(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AvgReview that = (AvgReview) o;
        return  Objects.equals(idRW, that.idRW) &&
                Objects.equals(brand, that.brand) &&
                Objects.equals(avg, that.avg);
    }
    @Override
    public String toString(){
        return idRW + "\t" + brand + "\t" + avg;
    }

    public String getIdRW() {
        return idRW;
    }

    public String getBrand() {
        return brand;
    }

    public Avg getAvg() {
        return avg;
    }

    public void setIdRW(String idRW) {
        this.idRW = idRW;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    public void setAvg(Avg avg) {
        this.avg = avg;
    }

    public void setAvg(Double sum, int n) {
        setAvg(new Avg(sum,n));
    }
}
