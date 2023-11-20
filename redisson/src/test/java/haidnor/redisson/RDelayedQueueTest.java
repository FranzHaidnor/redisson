package haidnor.redisson;

import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RBlockingQueue;
import org.redisson.api.RDelayedQueue;
import org.redisson.api.RQueue;

import java.util.concurrent.TimeUnit;

public class RDelayedQueueTest extends RedissonBaseTest {

    @Test
    public void test_() throws Exception {
        RBlockingQueue<String> blockingFairQueue = redisson.getBlockingQueue("RDelayedQueue_String");
        /** {@link Redisson#getDelayedQueue(RQueue)}*/
        RDelayedQueue<String> delayedQueue = redisson.getDelayedQueue(blockingFairQueue);
        delayedQueue.offer("delay msg", 10, TimeUnit.SECONDS);
        delayedQueue.destroy();
    }

}
