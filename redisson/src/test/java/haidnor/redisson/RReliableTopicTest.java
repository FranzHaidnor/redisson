package haidnor.redisson;

import org.junit.jupiter.api.Test;
import org.redisson.api.RFuture;
import org.redisson.api.RReliableTopic;
import org.redisson.api.listener.MessageListener;

import java.util.concurrent.CompletableFuture;

// https://github.com/redisson/redisson/wiki/6.-Distributed-objects#613-reliable-topic
public class RReliableTopicTest extends RedissonBaseTest {

    private RReliableTopic topic = redisson.getReliableTopic("REFRESH_DEPT_LOCAL_CACHE");

    @Test
    public void publish() throws Exception {
        topic.publish("msg1");
        topic.publish("msg2");
        topic.publish("msg3");
    }

    @Test
    public void test_3() throws Exception {
        topic.delete();
        long l = topic.sizeInMemory();
    }

    @Test
    public void listener() throws Exception {
        topic.addListener(String.class, new MessageListener<String>() {
            @Override
            public void onMessage(CharSequence channel, String msg) {
                System.out.println(msg);
            }
        });

        topic.addListener(String.class, new MessageListener<String>() {
            @Override
            public void onMessage(CharSequence channel, String msg) {
                System.out.println(msg);
            }
        });

        new CompletableFuture<>().get();
    }

}
