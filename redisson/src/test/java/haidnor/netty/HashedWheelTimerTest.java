package haidnor.netty;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class HashedWheelTimerTest {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // 任务
        CompletableFuture<String> mainPromise = new CompletableFuture<>();

        // 3 秒后超时
        HashedWheelTimer hashedWheelTimer = new HashedWheelTimer();
        Timeout timeout = hashedWheelTimer.newTimeout(new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {
                mainPromise.completeExceptionally(new RuntimeException("TimeOut"));
            }
        }, 3, TimeUnit.SECONDS);

        // 执行任务
        CompletableFuture.runAsync(() -> {
            // 模拟处理任务耗时
            sleep(2);

            // 判断任务有没有结束
            // System.out.println(mainPromise.isDone());

            // 返回结果
            mainPromise.complete("咖啡蛋糕");
            // 取消超时任务
            timeout.cancel();
        });

        System.out.println(mainPromise.get());

        new CompletableFuture<>().get();
    }


    public static void sleep(long timeout) {
        try {
            TimeUnit.SECONDS.sleep(timeout);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}

