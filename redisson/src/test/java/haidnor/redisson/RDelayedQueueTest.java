package haidnor.redisson;

import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.RedissonBlockingQueue;
import org.redisson.api.RBlockingQueue;
import org.redisson.api.RDelayedQueue;
import org.redisson.api.RQueue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class RDelayedQueueTest extends RedissonBaseTest {

    @Test
    public void test_() throws Exception {
        // 阻塞队列
        RBlockingQueue<String> blockingQueue = redisson.getBlockingQueue("Test");

        /** {@link Redisson#getDelayedQueue(RQueue)}*/
        RDelayedQueue<String> delayedQueue = redisson.getDelayedQueue(blockingQueue);
        delayedQueue.offer("delay msg", 1, TimeUnit.SECONDS);

        // 阻塞获取值
        /** {@link RedissonBlockingQueue#take()}*/
        String take = blockingQueue.take();
        System.out.println(take);

        // 延迟队列用完以后, 销毁延迟队列客户端监听器
        delayedQueue.destroy();
        new CompletableFuture<>().get();
    }


}
