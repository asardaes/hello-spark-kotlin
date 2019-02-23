package hello.spark.kotlin

import org.apache.spark.api.java.function.MapGroupsFunction
import org.apache.spark.sql.Row
import scala.Tuple2
import java.io.Serializable
import java.sql.Timestamp

class TransactionCreator : MapGroupsFunction<Tuple2<String, Timestamp>, Row, Transaction> {
    companion object {
        @JvmStatic
        private val serialVersionUID = 1L
    }

    override fun call(key: Tuple2<String, Timestamp>, values: MutableIterator<Row>): Transaction {
        val seq = generateSequence { if (values.hasNext()) values.next().getString(2) else null }
        val items = seq.toCollection(HashSet())
        return Transaction(key._1, key._2.time, items).also { println("inside call Transaction=$it") }
    }
}

// no setters would mean Spark cannot serialize correctly (?), same for abstract (Mutable)Set
data class Transaction(var context: String = "", var epoch: Long = -1L, var items: HashSet<String> = HashSet()) :
    Serializable {
    companion object {
        @JvmStatic
        private val serialVersionUID = 1L
    }
}
