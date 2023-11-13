package haidnor;

import org.junit.jupiter.api.Test;
import org.redisson.RedissonBaseLock;
import org.redisson.RedissonLock;
import org.redisson.api.RLock;

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

}
