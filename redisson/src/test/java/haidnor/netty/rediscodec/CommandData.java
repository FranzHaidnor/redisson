package haidnor.netty.rediscodec;

import java.util.concurrent.CompletableFuture;

public class CommandData {

    private String content;

    private CompletableFuture<Void> promise;

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public CompletableFuture<Void> getPromise() {
        return promise;
    }

    public void setPromise(CompletableFuture<Void> promise) {
        this.promise = promise;
    }

}
