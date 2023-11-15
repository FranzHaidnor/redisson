package haidnor.redisson;

import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.RedissonBucket;
import org.redisson.api.RBucket;

public class RedissonBucketTest extends RedissonBaseTest {
    @Test
    public void test_() throws Exception {
        RBucket<Object> bucket = redisson.getBucket("bucket");  /** {@link Redisson#getBucket(String)}*/
        bucket.set("zhangsan"); /** {@link RedissonBucket#set(Object)}*/
    }

}
