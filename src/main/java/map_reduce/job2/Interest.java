package map_reduce.job2;

enum Interest {

    LOW(3),
    MEDIUM(4.0),
    HIGH(5.0);

    private double value;

    Interest(double value) {
        this.value = value;
    }

    public double getValue() {
        return value;
    }
}
