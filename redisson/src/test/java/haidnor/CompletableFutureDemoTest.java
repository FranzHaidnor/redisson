package haidnor;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class CompletableFutureDemoTest {

    static final Logger log = LoggerFactory.getLogger(CompletableFutureDemoTest.class);

    /**
     * 执行无返回值的异步任务
     * public static CompletableFuture<Void> runAsync(Runnable runnable)
     * public static CompletableFuture<Void> runAsync(Runnable runnable, Executor executor)
     */
    @Test
    public void test_runAsync() throws Exception {
        CompletableFuture.runAsync(() -> {
            // do some thing
        });
    }

    /**
     * 执行有返回值的异步任务
     * public static <U> CompletableFuture<U> supplyAsync(Supplier<U> supplier)
     * public static <U> CompletableFuture<U> supplyAsync(Supplier<U> supplier, Executor executor)
     */
    @Test
    public void test_supplyAsync() throws Exception {
        CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {
            return "pig";
        });
    }

    // ----------------------------------------------------------------------------------------------

    /**
     * 取消任务
     */
    @Test
    public void test_cancel() throws Exception {
        CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {
            return "pig";
        });

        // boolean mayInterruptIfRunning 参数是没有任何意义的,因为源码中没有用到它
        // 一旦任务已经在执行了，则不会中断其执行。
        // 取消行为只会在其未完成之前修改其结果指示其已经被取消，即使已经处于执行中的任务最后成功完成也不能再修改其结果
        future.cancel(true);
    }

    /**
     * CompletableFuture 在运行中不可以取消(示例)
     */
    @Test
    public void test_cancel_1() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(10);

        // CompletableFuture 在运行中不可以取消
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

        new CompletableFuture<>().get();
    }

    @Test
    public void test_cancel_2() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(10);

        // Future 在运行中可以取消
        Future<?> future = executorService.submit(() -> {
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
        });

        TimeUnit.SECONDS.sleep(1);

        // 某个时刻取消任务
        boolean cancelled = future.cancel(true); // 取消任务，并尝试中断执行

        // 检查任务是否已经被取消
        if (cancelled) {
            System.out.println("任务已成功取消");
        } else {
            System.out.println("任务未能成功取消");
        }

        new CompletableFuture<>().get();
    }

    @Test
    public void test_isCancelled() {
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            // do something
        });

        // 判断任务是否取消了
        boolean cancelled = future.isCancelled();
    }

    /**
     * get() 获取值,如果任务没有完整则等待. 需要处理捕获的异常
     */
    @Test
    public void test_get() {
        CompletableFuture<Object> future = new CompletableFuture<>();
        try {
            future.get(); // 等待获取值
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
        try {
            future.get(1, TimeUnit.SECONDS);  // 等待获取值
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
        future.getNow("张三");        // 等待获取值, 如果没有就返回传入的
    }

    /**
     * join() 获取值,如果任务没有完整则等待. 不需要捕获异常
     */
    @Test
    public void test_join() {
        CompletableFuture<Object> future = new CompletableFuture<>();
        future.join();  // 和 get 的作用一样,不需要捕获异常
    }

    @Test
    public void test_() throws Exception {

    }
}
