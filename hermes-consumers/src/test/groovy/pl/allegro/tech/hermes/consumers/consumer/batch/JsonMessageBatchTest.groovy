package pl.allegro.tech.hermes.consumers.consumer.batch

import pl.allegro.tech.hermes.common.kafka.offset.PartitionOffset
import spock.lang.Specification
import spock.lang.Unroll

import java.nio.BufferOverflowException
import java.nio.ByteBuffer
import java.time.Clock

import static java.nio.ByteBuffer.allocateDirect

class JsonMessageBatchTest extends Specification {

    static def LARGE_BATCH_SIZE = 100
    static def LARGE_BATCH_TIME = Integer.MAX_VALUE
    static def LARGE_MESSAGE_TTL = Integer.MAX_VALUE
    static def LARGE_BATCH_VOLUME = 1024

    static def BATCH_ID = "1"

    Clock clock = Clock.systemUTC();

    @Unroll
    def "should append data into buffer"() {
        given:
        JsonMessageBatch batch = new JsonMessageBatch(BATCH_ID, ByteBuffer.allocate(capacity), LARGE_BATCH_SIZE, LARGE_BATCH_TIME, LARGE_MESSAGE_TTL, clock)

        when:
        data.each { it -> batch.append(it.bytes, Stub(PartitionOffset)) }
        batch.close();

        then:
        new String(batch.getContent().array()).replaceAll("\\[|\\]", "").split(",") == data

        where:
        data            | capacity
        ['xx']          | 4
        ["x", "x"]      | 5
        ["x", "x", "x"] | 7
    }

    @Unroll
    def "should throw exception when there is no remaining space for given element"() {
        given:
        JsonMessageBatch batch = new JsonMessageBatch(BATCH_ID, allocateDirect(capacity), LARGE_BATCH_SIZE, Integer.MAX_VALUE, Integer.MAX_VALUE, clock)

        when:
        data.each { it -> batch.append(it.bytes, Stub(PartitionOffset)) }
        batch.close();

        then:
        thrown BufferOverflowException

        where:
        data       | capacity
        ["xx"]     | 1
        ["xx"]     | 2
        ["xx"]     | 3
        ["x", "x"] | 4
    }

    @Unroll
    def "should be ready for delivery after ttl exceeded"() {
        given:
        def batchTtl = 1
        def data = "xxx".bytes

        Clock clock = Stub() { millis() >>> [10, 20] }
        JsonMessageBatch jsonMessageBatch = new JsonMessageBatch(BATCH_ID, allocateDirect(LARGE_BATCH_VOLUME), LARGE_BATCH_SIZE, batchTtl, LARGE_MESSAGE_TTL, clock)

        when:
        jsonMessageBatch.append(data, Stub(PartitionOffset))

        then:
        jsonMessageBatch.isReadyForDelivery() == true
    }
}
