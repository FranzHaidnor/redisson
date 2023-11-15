package haidnor.javaApi;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * CompletionStage 接口使用示例
 */
public class CompletionStageDemoTest {

    static final Logger log = LoggerFactory.getLogger(CompletionStageDemoTest.class);

    // 根据阶段正常完成结果的生产型（或者叫函数型） -------------------------------------------------------------------------

    /**
     * thenApply    一个阶段完成后执行    有返回值(生产行为)
     * <p>
     * public <U> CompletionStage<U> thenApply(Function<? super T,? extends U> fn);     // 默认执行方式
     * public <U> CompletionStage<U> thenApplyAsync(Function<? super T,? extends U> fn);// 默认的异步执行方式
     * public <U> CompletionStage<U> thenApplyAsync(Function<? super T,? extends U> fn,Executor executor); //自定义的执行方式
     * <p>
     * 2023.11.13 11:35:25.978 INFO  CommandAsyncService : 线程池开始执行任务
     * 2023.11.13 11:35:26.988 INFO  CommandAsyncService : thenAccept: 咖啡蛋糕
     */
    @Test
    public void test_thenApply() throws InterruptedException {
        CompletableFuture<String> future = new CompletableFuture<>();
        // 如果 mark1 正常执行完成以后,会执行 thenAccept 方法
        future.thenApply(s -> {
            log.info("thenAccept: {}", s);
            return s;         // 须有生产返回值
        });

        ExecutorService executor = Executors.newFixedThreadPool(10);
        executor.execute(() -> {
            log.info("线程池开始执行任务");
            sleepSeconds(1L);
            future.complete("咖啡蛋糕");    // mark1
        });
        await();
    }

    /**
     * thenCombine    依赖两个阶段全部完成后执行    有返回值(生产行为)
     * <p>
     * public <U,V> CompletionStage<V> thenCombine(CompletionStage<? extends U> other, BiFunction<? super T,? super U,? extends V> fn);
     * public <U,V> CompletionStage<V> thenCombineAsync(CompletionStage<? extends U> other, BiFunction<? super T,? super U,? extends V> fn);
     * public <U,V> CompletionStage<V> thenCombineAsync(CompletionStage<? extends U> other, BiFunction<? super T,? super U,? extends V> fn, Executor executor);
     */
    @Test
    public void test_thenCombine() throws ExecutionException, InterruptedException {
        CompletableFuture<String> future1 = new CompletableFuture<>();
        CompletableFuture<String> future2 = new CompletableFuture<>();

        // 只有当 future1 和 future2 都执行完了以后才会执行 thenCombine
        CompletableFuture<Object> result = future1.thenCombine(future2, new BiFunction<String, String, Object>() {
            @Override
            public Object apply(String s1, String s2) {
                log.info("s1 {} s2: {}", s1, s2);
                return s2;  // 返回 future2 的值
            }
        });

        ExecutorService executor = Executors.newFixedThreadPool(10);
        executor.execute(() -> {
            log.info("线程池开始执行任务1");
            sleepSeconds(1L);
            future1.complete("咖啡蛋糕");    // mark1
        });

        executor.execute(() -> {
            log.info("线程池开始执行任务2");
            sleepSeconds(2L);
            future2.complete("番茄鸡鸡蛋");    // mark2
        });

        // 获取最终的结果
        log.info("result {}", result.get());
    }

    /**
     * applyToEither    依赖两个阶段任意完成一个后执行    有返回值(生产行为)
     * <p>
     * public <U> CompletionStage<U> applyToEither(CompletionStage<? extends T> other,Function<? super T, U> fn);
     * public <U> CompletionStage<U> applyToEitherAsync(CompletionStage<? extends T> other,Function<? super T, U> fn);
     * public <U> CompletionStage<U> applyToEitherAsync(CompletionStage<? extends T> other,Function<? super T, U> fn,Executor executor);
     */
    @Test
    public void test_applyToEither() throws ExecutionException, InterruptedException {
        CompletableFuture<String> future1 = new CompletableFuture<>();
        CompletableFuture<String> future2 = new CompletableFuture<>();

        //  future1 和 future2 任意 applyToEither
        CompletableFuture<Object> result = future1.applyToEither(future2, new Function<String, Object>() {
            @Override
            public Object apply(String s) {
                return s;       // 具有生产返回值
            }
        });

        ExecutorService executor = Executors.newFixedThreadPool(10);
        executor.execute(() -> {
            log.info("线程池开始执行任务1");
            sleepSeconds(3L);
            future1.complete("咖啡蛋糕");    // mark1
        });

        executor.execute(() -> {
            log.info("线程池开始执行任务2");
            sleepSeconds(1L);
            future2.complete("番茄鸡鸡蛋");    // mark2
        });

        // 获取最终的结果
        log.info("result {}", result.get());
    }

    // 根据阶段正常完成结果的消费型----------------------------------------------------------------------------------------------------------------

    /**
     * thenAccept    一个阶段完成后执行    无返回值(消费行为)
     * <p>
     * public <U> CompletionStage<U> thenApply(Function<? super T,? extends U> fn);     // 默认执行方式
     * public <U> CompletionStage<U> thenApplyAsync(Function<? super T,? extends U> fn);// 默认的异步执行方式
     * public <U> CompletionStage<U> thenApplyAsync(Function<? super T,? extends U> fn,Executor executor); //自定义的执行方式
     */
    @Test
    public void test_thenAccept() throws InterruptedException {
        CompletableFuture<String> future = new CompletableFuture<>();
        // 如果 mark1 正常执行完成以后,会执行 thenAccept 方法
        future.thenAccept(s -> {
            log.info("thenAccept: {}", s);  // 没有返回值
        });

        ExecutorService executor = Executors.newFixedThreadPool(10);
        executor.execute(() -> {
            log.info("线程池开始执行任务");
            sleepSeconds(1L);
            future.complete("咖啡蛋糕");    // mark1
        });
        await();
    }


    /**
     * thenAcceptBoth    依赖两个阶段全部完成后执行    无返回值(消费行为)
     * <p>
     * public <U> CompletionStage<Void> thenAcceptBoth(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action);
     * public <U> CompletionStage<Void> thenAcceptBothAsync(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action);
     * public <U> CompletionStage<Void> thenAcceptBothAsync(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action, Executor executor);
     */
    @Test
    public void test_thenAcceptBoth() throws ExecutionException, InterruptedException {
        CompletableFuture<String> future1 = new CompletableFuture<>();
        CompletableFuture<String> future2 = new CompletableFuture<>();

        // 只有当 future1 和 future2 都执行完了以后才会执行 thenAcceptBoth
        CompletableFuture<Void> result = future1.thenAcceptBoth(future2, new BiConsumer<String, String>() {
            @Override
            public void accept(String s1, String s2) {
                log.info("s1 {} s2: {}", s1, s2);        // 没有返回值
            }
        });

        ExecutorService executor = Executors.newFixedThreadPool(10);
        executor.execute(() -> {
            log.info("线程池开始执行任务1");
            sleepSeconds(1L);
            future1.complete("咖啡蛋糕");    // mark1
        });

        executor.execute(() -> {
            log.info("线程池开始执行任务2");
            sleepSeconds(2L);
            future2.complete("番茄鸡鸡蛋");    // mark2
        });

        // 获取最终的结果
        log.info("result {}", result.get());
    }

    /**
     * acceptEither    依赖两个阶段任意完成一个后执行    无返回值(消费行为)
     * <p>
     * public CompletionStage<Void> acceptEither(CompletionStage<? extends T> other, Consumer<? super T> action);
     * public CompletionStage<Void> acceptEitherAsync(CompletionStage<? extends T> other, Consumer<? super T> action);
     * public CompletionStage<Void> acceptEitherAsync(CompletionStage<? extends T> other, Consumer<? super T> action, Executor executor);
     */
    @Test
    public void test_acceptEither() throws ExecutionException, InterruptedException {
        CompletableFuture<String> future1 = new CompletableFuture<>();
        CompletableFuture<String> future2 = new CompletableFuture<>();

        //  future1 和 future2 任意 applyToEither
        CompletableFuture<Void> result = future1.acceptEither(future2, new Consumer<String>() {
            @Override
            public void accept(String s) {
                log.info("s1 {}", s);        // 没有返回值
            }
        });

        ExecutorService executor = Executors.newFixedThreadPool(10);
        executor.execute(() -> {
            log.info("线程池开始执行任务1");
            sleepSeconds(3L);
            future1.complete("咖啡蛋糕");    // mark1
        });

        executor.execute(() -> {
            log.info("线程池开始执行任务2");
            sleepSeconds(1L);
            future2.complete("番茄鸡鸡蛋");    // mark2
        });

        // 获取最终的结果
        log.info("result {}", result.get());
    }

    // 三、只要求依赖的阶段正常完成的不消耗也不产出型 --------------------------------------------------------------------------

    /**
     * thenRun    一个阶段完成后执行    无返回值,无消费
     * <p>
     * public CompletionStage<Void> thenRun(Runnable action);
     * public CompletionStage<Void> thenRunAsync(Runnable action);
     * public CompletionStage<Void> thenRunAsync(Runnable action, Executor executor);
     */
    @Test
    public void test_thenRun() throws InterruptedException {
        CompletableFuture<String> future = new CompletableFuture<>();
        // 如果 mark1 正常执行完成以后,会执行 thenAccept 方法
        future.thenRun(() -> {
            log.info("do something");
        });

        ExecutorService executor = Executors.newFixedThreadPool(10);
        executor.execute(() -> {
            log.info("线程池开始执行任务");
            sleepSeconds(1L);
            future.complete("咖啡蛋糕");    // mark1
        });
        await();
    }

    /**
     * runAfterBoth    依赖两个阶段全部完成后执行    无返回值,无消费
     * <p>
     * public CompletionStage<Void> runAfterBoth(CompletionStage<?> other, Runnable action);
     * public CompletionStage<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action);
     * public CompletionStage<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action, Executor executor);
     */
    @Test
    public void test_runAfterBoth() throws ExecutionException, InterruptedException {
        CompletableFuture<String> future1 = new CompletableFuture<>();
        CompletableFuture<String> future2 = new CompletableFuture<>();

        // 只有当 future1 和 future2 都执行完了以后才会执行 runAfterBoth
        CompletableFuture<Void> result = future1.runAfterBoth(future2, new Runnable() {
            @Override
            public void run() {
                log.info("do something");
            }
        });

        ExecutorService executor = Executors.newFixedThreadPool(10);
        executor.execute(() -> {
            log.info("线程池开始执行任务1");
            sleepSeconds(1L);
            future1.complete("咖啡蛋糕");    // mark1
        });

        executor.execute(() -> {
            log.info("线程池开始执行任务2");
            sleepSeconds(2L);
            future2.complete("番茄鸡鸡蛋");    // mark2
        });

        // 获取最终的结果
        log.info("result {}", result.get());
    }

    /**
     * runAfterEither    依赖两个阶段任意完成一个后执行    无返回值,无消费
     * <p>
     * public CompletionStage<Void> runAfterEither(CompletionStage<?> other, Runnable action);
     * public CompletionStage<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action);
     * public CompletionStage<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action, Executor executor);
     */
    @Test
    public void test_runAfterEither() throws ExecutionException, InterruptedException {
        CompletableFuture<String> future1 = new CompletableFuture<>();
        CompletableFuture<String> future2 = new CompletableFuture<>();

        //  future1 和 future2 任意 applyToEither
        CompletableFuture<Void> result = future1.runAfterEither(future2, new Runnable() {
            @Override
            public void run() {
                log.info("do something");
            }
        });

        ExecutorService executor = Executors.newFixedThreadPool(10);
        executor.execute(() -> {
            log.info("线程池开始执行任务1");
            sleepSeconds(3L);
            future1.complete("咖啡蛋糕");    // mark1
        });

        executor.execute(() -> {
            log.info("线程池开始执行任务2");
            sleepSeconds(1L);
            future2.complete("番茄鸡鸡蛋");    // mark2
        });

        // 获取最终的结果
        log.info("result {}", result.get());
    }

    // 四、根据正常完成的阶段本身而不是其结果的产出型 -------------------------------------------------------------------------

    /**
     * test_thenCompose    一个阶段完成后执行    必须返回一个 CompletableFuture
     * thenCompose 和 thenCombine 很相似，但看起来 thenCompose 比thenCombine更简洁。
     * <p>
     * public <U> CompletionStage<U> thenCompose(Function<? super T, ? extends CompletionStage<U>> fn);
     * public <U> CompletionStage<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn);
     * public <U> CompletionStage<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn, Executor executor);
     * <p>
     * 2023.11.13 14:29:35.003 INFO  CommandAsyncService : 1
     * 2023.11.13 14:29:38.016 INFO  CommandAsyncService : 2
     * 2023.11.13 14:29:38.016 INFO  CommandAsyncService : result hello world
     */
    @Test
    public void test_thenCompose() throws ExecutionException, InterruptedException {
        CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {        // 阶段1
            sleepSeconds(3L);
            log.info("1");
            return "hello";

        });
        future.thenCompose(s -> {                                                       // 阶段1
            sleepSeconds(3L);
            log.info("2");
            return new CompletableFuture<>().completeAsync(() -> {
                return s + " world";
            });
        });

        log.info("result {}", future.get());
    }

    @Test
    public void test_thenCompose_2() throws ExecutionException, InterruptedException {
        CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {        // 阶段1
            sleepSeconds(3L);
            log.info("1");
            return "hello";

        });
        future = future.thenCompose(s -> CompletableFuture.supplyAsync(() -> {                   // 阶段2
            sleepSeconds(3L);
            log.info("2");
            return s + " world";   // s 是阶段 1 的返回结果
        }));

        log.info("result {}", future.get());
    }

    // 五、不论阶段正常还是异常完成的消耗型 ----------------------------------------------------------------------------------


    /**
     * whenComplete不论依赖的上一个阶段是正常完成还是异常完成都不会影响它的执行. 它是一个消耗型接口，即不会对阶段的原来结果产生影响，
     * <p>
     * public CompletionStage<T> whenComplete(BiConsumer<? super T, ? super Throwable> action);
     * public CompletionStage<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action);
     * public CompletionStage<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action, Executor executor);
     * <p>
     * <p>
     * 2023.11.13 14:58:32.504 INFO  CommandAsyncService : result: null
     * 2023.11.13 14:58:32.508 INFO  CommandAsyncService : throwable: java.util.concurrent.CompletionException: java.lang.RuntimeException: throw  RuntimeException
     * java.util.concurrent.ExecutionException: java.lang.RuntimeException: throw  RuntimeException
     */
    @Test
    public void test_whenComplete() throws ExecutionException, InterruptedException {
        CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {        // 阶段1
            if (true) {
                throw new RuntimeException("throw  RuntimeException");
            }
            log.info("阶段1执行结束");
            return "hello";

        });
        // 无论阶段1是否执行成功, whenComplete 都会执行
        future = future.whenComplete((result, throwable) -> {
            log.info("result: {}", result);
            log.info("throwable: {}", throwable.toString());
        });

        log.info("result {}", future.get());    // 执行 get() 时将会抛出阶段1的异常
    }

    // 六、不论阶段正常还是异常完成的产出型 -------------------------------------------------------------------------------------

    /**
     * handle 不论阶段正常还是异常完成的产出型
     * whenComplete是对不论依赖阶段正常完成还是异常完成时的消耗或者消费，即不会改变阶段的现状，而handle前缀的方法则是对应的产出型方法，即可以对正常完成的结果进行转换，也可以对异常完成的进行补偿一个结果，即可以改变阶段的现状。
     * <p>
     * public <U> CompletionStage<U> handle (BiFunction<? super T, Throwable, ? extends U> fn);
     * public <U> CompletionStage<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn);
     * public <U> CompletionStage<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn, Executor executor);
     * <p>
     * 2023.11.13 15:01:26.595 INFO  CommandAsyncService : result: null
     * 2023.11.13 15:01:26.598 INFO  CommandAsyncService : throwable: java.util.concurrent.CompletionException: java.lang.RuntimeException: throw  RuntimeException
     * 2023.11.13 15:01:26.598 INFO  CommandAsyncService : result 阶段2结果
     */
    @Test
    public void test_handle() throws ExecutionException, InterruptedException {
        CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {        // 阶段1
            if (true) {
                throw new RuntimeException("throw  RuntimeException");
            }
            log.info("阶段1执行结束");
            return "hello";

        });
        // 无论阶段1是否执行成功, handle 都会执行
        future = future.handle((result, throwable) -> {
            log.info("result: {}", result);
            log.info("throwable: {}", throwable.toString());

            return "阶段2结果";     // 有返回值
        });

        log.info("result {}", future.get());    // 执行 get() 时将不会抛出阶段1的异常
    }

    // 七、异常完成的产出型 -------------------------------------------------------------------------------------

    @Test
    public void test_exceptionally() throws ExecutionException, InterruptedException {
        CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {        // 阶段1
            if (true) {
                throw new RuntimeException("throw  RuntimeException");
            }
            log.info("阶段1执行结束");
            return "hello";

        });

        future = future.exceptionally(throwable -> {
            log.info("捕获异常 {}", throwable.toString());
            return "阶段2结果";     // 有返回值
        });

        log.info("result {}", future.get());    // 执行 get() 时将不会抛出阶段1的异常
    }


    // 八、实现该接口不同实现之间互操作的类型转换方法： -------------------------------------------------------------------------

    @Test
    public void test_() throws Exception {
        CompletionStage<String> future1 = CompletableFuture.supplyAsync(() -> {
            sleepSeconds(1L);
            return "咖啡蛋糕";
        });
        System.out.println(future1.hashCode());      // 52514534

        CompletableFuture<String> future2 = future1.toCompletableFuture();
        System.out.println(future2.hashCode());     // 52514534

        CompletableFuture<String> future3 = (CompletableFuture<String>) future1.thenApply(s -> s);
        System.out.println(future3.hashCode());     // 2104973502
        System.out.println(future3.get());
    }

    // ---------------------------------------------------------------------------------------------------------------

    public static void sleepSeconds(Long seconds) {
        try {
            TimeUnit.SECONDS.sleep(seconds);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void await() {
        try {
            new CountDownLatch(1).await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
