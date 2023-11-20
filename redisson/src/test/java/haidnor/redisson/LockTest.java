package haidnor.redisson;

import org.junit.jupiter.api.Test;
import org.redisson.RedissonBaseLock;
import org.redisson.RedissonLock;
import org.redisson.api.RBlockingQueue;
import org.redisson.api.RDelayedQueue;
import org.redisson.api.RLock;

import java.util.concurrent.TimeUnit;

public class LockTest extends RedissonBaseTest {

    /**
     * 参考链接 <a href="https://zhuanlan.zhihu.com/p/135864820">...</a>
     */
    @Test
    public void test_getLock() throws Exception {
        RLock lock = redisson.getLock("lock");
        /** {@link RedissonLock#tryLock()}*/
        if (lock.tryLock()) {
            System.out.println("do something");
        }
        /** {@link RedissonBaseLock#unlock()}*/
        lock.unlock();
    }

    @Test
    public void test_getLock_2() throws Exception {
        RLock lock = redisson.getLock("lock");
        /** {@link RedissonLock#tryLock(long, TimeUnit)}*/
        if (lock.tryLock(10, TimeUnit.SECONDS)) {
            System.out.println("do something");
        }
        /** {@link RedissonBaseLock#unlock()}*/
        lock.unlock();
    }


}
