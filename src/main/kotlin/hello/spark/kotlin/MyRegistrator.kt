package hello.spark.kotlin

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.serializers.JavaSerializer
import org.apache.spark.serializer.KryoRegistrator

class MyRegistrator : KryoRegistrator {
    override fun registerClasses(kryo: Kryo) {
        kryo.register(HashSet::class.java, JavaSerializer()) // kotlin's HashSet
    }
}
