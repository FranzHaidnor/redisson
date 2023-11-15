package haidnor.netty.rediscodec;

import haidnor.javaApi.CompletableFutureDemoTest;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.redis.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;

/**
 * Redis 订阅模式
 * <a href="https://www.runoob.com/redis/redis-pub-sub.html">教程</a>
 */
public class RedisClientSub {

    static final Logger log = LoggerFactory.getLogger(CompletableFutureDemoTest.class);

    public static void main(String[] args) {
        NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap
                    .group(eventLoopGroup)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast(new ChannelInboundHandlerAdapter() {                           // InboundHandler
                                @Override
                                public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
                                    super.channelRegistered(ctx);
                                    ChannelAttrUtil.set(ctx.channel(), new ConcurrentLinkedDeque<>(), ConcurrentLinkedDeque.class);
                                }
                            });
                            pipeline.addLast(new RedisDecoder());                                           // InboundHandler
                            pipeline.addLast(new SimpleChannelInboundHandler<RedisMessage>() {              // InboundHandler
                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, RedisMessage msg) {
                                    // 请求成功
                                    if (msg instanceof SimpleStringRedisMessage) {
                                        SimpleStringRedisMessage message = (SimpleStringRedisMessage) msg;
                                        ConcurrentLinkedDeque<CommandData> deque = ChannelAttrUtil.get(ctx.channel(), ConcurrentLinkedDeque.class);
                                        CommandData commandData = deque.pop();
                                        log.info("{} > {}", commandData.getContent(), message.content());
                                        commandData.getPromise().complete(null);
                                    }
                                    // 请求异常
                                    else if (msg instanceof ErrorRedisMessage) {
                                        ErrorRedisMessage message = (ErrorRedisMessage) msg;
                                        ConcurrentLinkedDeque<CommandData> deque = ChannelAttrUtil.get(ctx.channel(), ConcurrentLinkedDeque.class);
                                        CommandData commandData = deque.pop();
                                        commandData.getPromise().completeExceptionally(new RuntimeException(message.content()));
                                    }
                                    // 接收订阅的消息
                                    else if (msg instanceof DefaultLastBulkStringRedisContent) {
                                        DefaultLastBulkStringRedisContent message = (DefaultLastBulkStringRedisContent) msg;
                                        log.info("DefaultLastBulkStringRedisContent > {}", message.content().toString(StandardCharsets.UTF_8));
                                    }
                                }
                            });

                            // -------------------------------------
                            pipeline.addLast(new RedisEncoder());                               // OutboundHandler
                            pipeline.addLast(new MessageToMessageEncoder<CommandData>() {       // OutboundHandler
                                protected void encode(ChannelHandlerContext ctx, CommandData msg, List<Object> out) throws Exception {
                                    ConcurrentLinkedDeque<CommandData> deque = ChannelAttrUtil.get(ctx.channel(), ConcurrentLinkedDeque.class);
                                    deque.add(msg);
                                    InlineCommandRedisMessage message = new InlineCommandRedisMessage(msg.getContent());
                                    out.add(message);
                                }
                            });
                        }
                    });

            ChannelFuture connectFuture = bootstrap.connect("192.168.100.100", 6379).sync();
            Channel channel = connectFuture.channel();

            // 密码登陆 redis
            request("AUTH root", channel);

            // 发送指令.  订阅通道 “redisson_lock_channel:LOCK”
            request("SUBSCRIBE redisson_lock_channel:LOCK", channel);

            connectFuture.channel().closeFuture().sync();
        } catch (Exception e) {
            log.error("", e);
        } finally {
            eventLoopGroup.shutdownGracefully();
        }
    }

    public static void request(String command, Channel channel) {
        CommandData data = new CommandData();
        data.setContent(command);
        CompletableFuture<Void> promise = new CompletableFuture<>();
        data.setPromise(promise);

        channel.writeAndFlush(data);

        try {
            promise.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

}
