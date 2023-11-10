package haidnor;

import org.junit.jupiter.api.Test;

import java.util.concurrent.*;

public class CompletableFutureDemo {

    @Test
    public void test_() throws Exception {
        CompletableFuture<String> future = new CompletableFuture<>();

        future.get();  // 获取不到值会一直阻塞
        System.out.println("hello"); // 此行代码不会执行
    }

    @Test
    public void test_1() throws Exception {
        CompletableFuture<String> future = new CompletableFuture<>();

        String val = future.get(3, TimeUnit.SECONDS);   // 获取,等待指定时间后放行
        // 等待指定时间后没有获取到值将会抛出异常 java.util.concurrent.TimeoutException
        System.out.println(val); // 此行代码不会执行
    }


    public static void main(String[] args) throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(10);

//        Future<?> future = executorService.submit(() -> {
//            while (!Thread.currentThread().isInterrupted()) {
//                System.out.println("任务执行中...");
//                try {
//                    TimeUnit.MILLISECONDS.sleep(200);
//                } catch (InterruptedException e) {
//                    // 响应中断信号
//                    System.out.println("收到中断信号，停止任务执行");
//                    return;
//                }
//            }
//        });

        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                System.out.println("任务执行中...");
                try {
                    TimeUnit.MILLISECONDS.sleep(200);
                } catch (InterruptedException e) {
                    // 响应中断信号
                    System.out.println("收到中断信号，停止任务执行");
                    return;
                }
            }
        }, executorService);

        TimeUnit.SECONDS.sleep(1);

        // 某个时刻取消任务
        boolean cancelled = future.cancel(true); // 取消任务，并尝试中断执行

        // 检查任务是否已经被取消
        if (cancelled) {
            System.out.println("任务已成功取消");
        } else {
            System.out.println("任务未能成功取消");
        }

        new CountDownLatch(1).await();
    }
}
