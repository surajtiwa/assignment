package executor;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskExecutorServiceSynchronized implements TaskExecutor {

    private final int maxConcurrency;
    private final ExecutorService executorService;
    private final Map<UUID, Object> groupLocks = new ConcurrentHashMap<>();
    private final BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();
    private final AtomicInteger runningTasks = new AtomicInteger(0);

    public TaskExecutorServiceSynchronized(int maxConcurrency) {
        this.maxConcurrency = maxConcurrency;
        this.executorService = Executors.newFixedThreadPool(maxConcurrency);
        startTaskProcessor();
    }

    //starts a asynchronus thread to read value from theBlocking queue
    private void startTaskProcessor() {
        Thread taskProcessor = new Thread(() -> {
            while (true) {
                try {
                    Runnable task = taskQueue.take();
                    executorService.submit(task);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        taskProcessor.setDaemon(true);
        taskProcessor.start();
    }

    @Override
    public <T> Future<T> submitTask(Task<T> task) {
        CompletableFuture<T> future = new CompletableFuture<>();
        Runnable taskWrapper = () -> {
            Object groupLock = groupLocks.computeIfAbsent(task.taskGroup().groupUUID(), k -> new Object());
            synchronized (groupLock) {
                try {
                    T result = task.taskAction().call();
                    future.complete(result);
                } catch (Exception e) {
                    future.completeExceptionally(e);
                } finally {
                    runningTasks.decrementAndGet();
                }
            }
        };

        taskQueue.offer(() -> {
            if (runningTasks.incrementAndGet() <= maxConcurrency) {
                taskWrapper.run();
            } else {
                runningTasks.decrementAndGet();
                taskQueue.offer(taskWrapper);
            }
        });

        return future;
    }

    public void shutdown() {
        executorService.shutdown();
    }


}
