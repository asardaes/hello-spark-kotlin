package hello.spark.kotlin

import org.apache.spark.SparkConf
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import scala.Tuple2
import java.sql.Timestamp

internal val sparkConf = SparkConf()
    .setAppName("Hello Spark with Kotlin")
    .set("spark.kryo.registrator", "hello.spark.kotlin.MyRegistrator")

fun main() {
    DBIntermediary.init()

    @Suppress("UsePropertyAccessSyntax")
    val spark = SparkSession
        .builder()
        .config(sparkConf)
        .getOrCreate()

    spark.sparkContext().setLogLevel("WARN")

    val inputDataset = spark.read()
        .format("jdbc")
        .option("url", DBIntermediary.url)
        .option("dbtable", "${DBIntermediary.schema}.${DBIntermediary.table}")
        .option("user", DBIntermediary.user)
        .option("password", DBIntermediary.pass)
        .load()

    inputDataset.show()

    val transactionEncoder = Encoders.bean(Transaction::class.java)
    val transactions = inputDataset
        .groupByKey(KeyExtractor(), KeyExtractor.getKeyEncoder())
        .mapGroups(TransactionCreator(), transactionEncoder)
        .collectAsList()

    transactions.forEach { println("collected Transaction=$it") }

    spark.stop()
}

class KeyExtractor : MapFunction<Row, Tuple2<String, Timestamp>> {
    companion object {
        @JvmStatic
        private val serialVersionUID = 1L

        @JvmStatic
        fun getKeyEncoder(): Encoder<Tuple2<String, Timestamp>> {
            return Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP())
        }
    }

    override fun call(value: Row): Tuple2<String, Timestamp> {
        return Tuple2(value.getString(0), value.getTimestamp(1))
    }
}
