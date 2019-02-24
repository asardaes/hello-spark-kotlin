package hello.spark.kotlin

import java.sql.DriverManager
import java.sql.Timestamp

object DBIntermediary {
    const val driver = "org.hsqldb.jdbc.JDBCDriver"
    const val url = "jdbc:hsqldb:mem:sarda"
    const val user = "SA"
    const val pass = ""
    const val schema = "hello_spark"
    const val table = "tbl"

    fun init() {
        val initialValues = listOf(
            Triple("context1", Timestamp(0L), "a"),
            Triple("context1", Timestamp(0L), "b"),
            Triple("context1", Timestamp(1000L), "c")
        )

        DriverManager.getConnection(url, user, pass).use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("CREATE SCHEMA IF NOT EXISTS $schema")
                stmt.execute(
                    """
                    CREATE TABLE IF NOT EXISTS $schema.$table (
                      ctx VARCHAR(512),
                      time TIMESTAMP,
                      item VARCHAR(512)
                    )
                """.trimIndent()
                )
            }

            conn.prepareStatement("INSERT INTO $schema.$table (ctx, time, item) VALUES (?, ?, ?)").use { stmt ->
                for (value in initialValues) {
                    stmt.setString(1, value.first)
                    stmt.setTimestamp(2, value.second)
                    stmt.setString(3, value.third)
                    stmt.addBatch()
                }

                stmt.executeBatch()
            }
        }
    }
}
