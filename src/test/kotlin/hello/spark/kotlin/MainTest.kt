package hello.spark.kotlin

import org.junit.jupiter.api.Test

class MainTest {
    @Test
    fun mainTest() {
        sparkConf.setMaster("local[*]")
        main()
    }
}
