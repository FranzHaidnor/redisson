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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;

public class RedisClient {

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
                                    ConcurrentLinkedDeque<CommandData> deque = ChannelAttrUtil.get(ctx.channel(), ConcurrentLinkedDeque.class);
                                    CommandData commandData = deque.pop();
                                    CompletableFuture<Void> promise = commandData.getPromise();
                                    // 请求成功
                                    if (msg instanceof SimpleStringRedisMessage) {
                                        SimpleStringRedisMessage message = (SimpleStringRedisMessage) msg;
                                        log.info("{} > {}", commandData.getContent(), message.content());
                                        commandData.getPromise().complete(null);
                                    }
                                    // 请求异常
                                    else if (msg instanceof ErrorRedisMessage) {
                                        ErrorRedisMessage message = (ErrorRedisMessage) msg;
                                        promise.completeExceptionally(new RuntimeException(message.content()));
                                    } else if (msg instanceof InlineCommandRedisMessage) {

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

            // 发送指令
            request("SET OA_DEPT_CACHE 121231233", channel);

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
