package haidnor;

import org.junit.jupiter.api.Test;
import org.redisson.api.RBucket;

public class RedissonBucketTest extends RedissonBaseTest {
    @Test
    public void test_() throws Exception {
        RBucket<Object> bucket = redisson.getBucket("bucket");
        bucket.set("zhangsan");
    }

}
