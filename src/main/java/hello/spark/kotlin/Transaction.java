package hello.spark.kotlin;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

@SuppressWarnings("unused")
public class Transaction implements Serializable {
    private static final long serialVersionUID = 1L;

    public Transaction() {
    }

    public Transaction(String context, long epoch, List<String> items) {
        this.context = context;
        this.epoch = epoch;
        this.items = items;
    }

    public static Encoder<Transaction> getEncoder() {
        return Encoders.bean(Transaction.class);
    }

    private String context;
    private long epoch;
    private List<String> items;

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public long getEpoch() {
        return epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    public List<String> getItems() {
        return items;
    }

    public void setItems(List<String> items) {
        this.items = items;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Transaction that = (Transaction) o;
        return epoch == that.epoch &&
                Objects.equals(context, that.context) &&
                Objects.equals(items, that.items);
    }

    @Override
    public int hashCode() {
        return Objects.hash(context, epoch, items);
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "context='" + context + '\'' +
                ", epoch=" + epoch +
                ", items=" + items +
                '}';
    }
}
