package io.github.prideoo.xsocket;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Level;

final class EventLoop {

    private final ScheduledThreadPoolExecutor executor;
    private volatile Thread thread;

    EventLoop(String name, boolean isDaemon) {
        executor = new ScheduledThreadPoolExecutor(1, runnable -> {
            Thread loopThread = new Thread(runnable, name);
            loopThread.setDaemon(isDaemon);
            thread = loopThread;
            return loopThread;
        });
        executor.setRemoveOnCancelPolicy(true);
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
    }

    boolean isLoopThread() {
        return Thread.currentThread() == thread;
    }

    boolean isShutdown() {
        return executor.isShutdown();
    }

    boolean execute(Runnable task) {
        try {
            executor.execute(() -> runSafe(task));
            return true;
        } catch (RejectedExecutionException e) {
            return false;
        }
    }

    boolean run(Runnable task) {
        if (isLoopThread()) {
            runSafe(task);
            return true;
        }
        return execute(task);
    }

    <T> T call(Supplier<T> task) {
        if (isLoopThread()) {
            return task.get();
        }
        try {
            return executor.submit(task::get).get();
        } catch (RejectedExecutionException e) {
            throw new XSocketException("destroy");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new XSocketException(Helpers.getErrorMessage(cause), cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new XSocketException("interrupted", e);
        }
    }

    <T> CompletableFuture<T> async(Supplier<CompletableFuture<T>> task) {
        CompletableFuture<T> result = new CompletableFuture<>();
        boolean isRun = run(() -> {
            CompletableFuture<T> future;
            try {
                future = task.get();
            } catch (Throwable e) {
                result.completeExceptionally(e);
                return;
            }
            future.whenComplete((value, e) -> {
                if (e != null) {
                    result.completeExceptionally(Helpers.unwrap(e));
                } else {
                    result.complete(value);
                }
            });
        });
        if (!isRun) {
            result.completeExceptionally(new XSocketException("destroy"));
        }
        return result;
    }

    ScheduledFuture<?> schedule(Runnable task, long delay) {
        try {
            return executor.schedule(() -> runSafe(task), Math.max(0, delay), TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            return null;
        }
    }

    ScheduledFuture<?> interval(Runnable task, long period) {
        try {
            return executor.scheduleAtFixedRate(() -> runSafe(task), period, period, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            return null;
        }
    }

    static void cancel(ScheduledFuture<?> future) {
        if (future != null) {
            future.cancel(false);
        }
    }

    void shutdown() {
        executor.shutdown();
    }

    private static void runSafe(Runnable task) {
        try {
            task.run();
        } catch (Throwable e) {
            Helpers.logger.log(Level.WARNING, "Task error", e);
        }
    }
}
