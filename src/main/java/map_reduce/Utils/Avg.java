package map_reduce.Utils;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class Avg implements Writable {

    private double sum = 0.0;
    private int n = 0;


    public Avg(double idBrand, int name){
        this.sum = idBrand;
        this.n = name;
    }
    public Avg(){}

    public static Avg read(DataInput in) throws IOException {
        Avg avg = new Avg();
        avg.readFields(in);
        return avg;
    }

    public void add(double value){
        setSum(sum + value);
        n ++;
    }

    public void mergeAvg(Avg other){
        setSum(sum + other.getSum());
        setN(n + other.getN());
    }

    public double computeAvg(){
        return n == 0 ? 0 : sum / n;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(sum);
        out.writeInt(n);
        out.writeDouble(computeAvg());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        sum = in.readDouble();
        n = in.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Avg avg = (Avg) o;
        return n == avg.n &&
                sum == avg.sum;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sum, n);
    }

    @Override
    public String toString(){
        return sum + "\t" + n;
    }

    public double getSum() {
        return sum;
    }

    public int getN() {
        return n;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    public void setN(int n) {
        this.n = n;
    }
}
