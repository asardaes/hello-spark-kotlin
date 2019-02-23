package hello.spark.kotlin

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.KryoSerializable
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import org.apache.spark.api.java.function.MapPartitionsFunction
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Aggregator
import scala.collection.mutable.WrappedArray
import java.util.*

class ItemEnlister : MapPartitionsFunction<Row, Transaction> {
    companion object {
        @JvmStatic
        private val serialVersionUID = 1L
    }

    override fun call(input: MutableIterator<Row>): MutableIterator<Transaction> {
        val transactionItems = mutableMapOf<Pair<String, Long>, MutableList<String>>()
        for (row in input) {
            val items =
                transactionItems.computeIfAbsent(Pair(row.getString(0), row.getTimestamp(1).time)) { ArrayList() }
            items.add(row.getString(2))
        }

        return transactionItems
            .map { Transaction(it.key.first, it.key.second, it.value) }
            .toMutableList()
            .also { println("partition transactions: $it") }
            .iterator()
    }
}

class StringSet(other: Collection<String>) : HashSet<String>(other), KryoSerializable {
    companion object {
        @JvmStatic
        private val serialVersionUID = 1L
    }

    constructor() : this(Collections.emptyList())

    override fun write(kryo: Kryo, output: Output) {
        output.writeInt(this.size)
        for (string in this) {
            output.writeString(string)
        }
    }

    override fun read(kryo: Kryo, input: Input) {
        val size = input.readInt()
        repeat(size) { this.add(input.readString()) }
    }
}

class ItemAggregator : Aggregator<Row, StringSet, StringSet>() {
    companion object {
        @JvmStatic
        private val serialVersionUID = 1L
    }

    override fun zero() = StringSet()

    override fun reduce(buffer: StringSet, row: Row): StringSet {
        @Suppress("UNCHECKED_CAST")
        val items = row.get(2) as WrappedArray<String>
        for (item in items) buffer.add(item)
        return buffer
    }

    override fun merge(b1: StringSet, b2: StringSet): StringSet {
        return StringSet(b1).apply { addAll(b2) }
    }

    override fun finish(items: StringSet) = items.also { println("aggregation=$it") }

    override fun bufferEncoder(): Encoder<StringSet> = Encoders.kryo(StringSet::class.java)

    override fun outputEncoder(): Encoder<StringSet> = Encoders.kryo(StringSet::class.java)
}
