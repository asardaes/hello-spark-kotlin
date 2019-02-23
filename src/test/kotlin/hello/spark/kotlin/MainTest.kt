package hello.spark.kotlin

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream

class MainTest {
    @Test
    fun mainTest() {
        sparkConf.setMaster("local[*]")
        main()
    }

    @Test
    fun stringSetKryoSerializationWorks() {
        val file = File("stringSetKryoSerializationWorks.dat").apply {
            createNewFile()
            deleteOnExit()
        }

        val input = Input(FileInputStream(file))
        val output = Output(FileOutputStream(file))

        try {
            val kryo = Kryo()
            val set = StringSet(listOf("a"))
            kryo.writeObject(output, set)
            output.close()

            val deserialized = kryo.readObject(input, StringSet::class.java)
            input.close()

            Assertions.assertEquals(set, deserialized)
            Assertions.assertEquals("a", deserialized.first())

        } catch (e: Exception) {
            input.close()
            output.close()
            throw e
        }
    }
}
