package hello.spark.kotlin

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

internal val sparkConf = SparkConf()
    .setAppName("Hello Spark with Kotlin")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrationRequired", "true")
    .registerKryoClasses(arrayOf(StringSet::class.java))

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

    val aggregator = ItemAggregator().toColumn().alias("items")
    val transactions = inputDataset
        .mapPartitions(ItemEnlister(), Transaction.getEncoder())
        .also { it.printSchema() }
        .groupBy("context", "epoch")
        .agg(aggregator)
        .also { it.printSchema() }
        .collectAsList()

    transactions.forEach { println("collected transaction=${it.mkString(",")}") }

    spark.stop()
}
