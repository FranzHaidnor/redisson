package haidnor;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.io.IOException;

public abstract class RedissonBaseTest {

    protected static RedissonClient redisson;

    @BeforeAll
    public static void beforeClass() throws IOException, InterruptedException {
        redisson = createInstance();
    }

    @AfterAll
    public static void afterClass() throws InterruptedException {
        redisson.shutdown();
    }

    @BeforeEach
    public void before() throws IOException, InterruptedException {
        // 清空 Redis 库中所有的数据
        redisson.getKeys().flushall();
    }

    public static Config createConfig() {
        Config config = new Config();
        config.useSingleServer().setAddress("redis://192.168.100.100:6379").setPassword("root");
        return config;
    }

    public static RedissonClient createInstance() {
        Config config = createConfig();
        // 创建 Redisson
        return Redisson.create(config);
    }

}
