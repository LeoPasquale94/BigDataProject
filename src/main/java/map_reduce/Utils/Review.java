package map_reduce.Utils;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class Review implements Writable {

    private String idRW = "";
    private double overall = 0.0;


    public Review(String idRW, double overall){
        this.idRW = idRW;
        this.overall = overall;
    }
    public Review(){}

    public static Review read(DataInput in) throws IOException {
        Review r = new Review();
        r.readFields(in);
        return r;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(idRW);
        out.writeDouble(overall);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.idRW = in.readUTF();
        this.overall = in.readDouble();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Review review = (Review) o;
        return Objects.equals(idRW, ((Review) o).idRW) &&
                overall == review.overall;
    }

    @Override
    public int hashCode() {
        return Objects.hash(idRW, overall);
    }

    @Override
    public String toString(){
        return idRW + "\t" + overall;
    }

    public String getIdRW() {
        return idRW;
    }

    public void setIdRW(String idRW) {
        this.idRW = idRW;
    }

    public void setOverall(double overall) {
        this.overall = overall;
    }

    public double getOverall() {
        return overall;
    }
}
