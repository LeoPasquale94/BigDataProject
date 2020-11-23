package map_reduce.Utils;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class Brand implements Writable {
    private String idBrand = "";
    private String name = "";

    public Brand(String idBrand, String name){
        this.idBrand = idBrand;
        this.name = name;
    }
    public Brand(){}

    public static Brand read(DataInput in) throws IOException {
        Brand b = new Brand();
        b.readFields(in);
        return b;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(idBrand);
        out.writeUTF(name);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        idBrand = in.readUTF();
        name = in.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Brand brand = (Brand) o;
        return Objects.equals(idBrand, ((Brand) o).idBrand)&&
                Objects.equals(name, ((Brand) o).name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(idBrand, name);
    }

    @Override
    public String toString(){
        return idBrand + "\t" + name;
    }

    public String getIdBrand() {
        return idBrand;
    }

    public String getName() {
        return name;
    }

    public void setIdBrand(String idBrand) {
        this.idBrand = idBrand;
    }

    public void setName(String name) {
        this.name = name;
    }
}
