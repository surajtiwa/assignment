package executor;

import java.util.UUID;
import java.util.concurrent.*;

public class TaskExecutorSemaphoreImpl implements TaskExecutor {
    Semaphore globalSemaphore;
    ConcurrentHashMap<UUID, Semaphore> groupSemaphoreMap;
    ExecutorService executorService;

    TaskExecutorSemaphoreImpl(int maxCapcaity) {
        globalSemaphore = new Semaphore(maxCapcaity);
        groupSemaphoreMap = new ConcurrentHashMap<>();
        executorService = Executors.newFixedThreadPool(maxCapcaity);
    }

    @Override
    public <T> Future<T> submitTask(Task<T> task) {
        CompletableFuture future = new CompletableFuture<>();

        executorService.submit(() -> {
            try {
                //accquire global semaphore
                globalSemaphore.acquire();
                Semaphore groupSemaphore = groupSemaphoreMap.computeIfAbsent(task.taskUUID(), id -> new Semaphore(1));
                //accquire group semaphore
                groupSemaphore.acquire();
                try {
                    T result = task.taskAction().call();
                    future.complete(result);
                } catch (Exception e) {
                    future.completeExceptionally(e);
                } finally {
                    groupSemaphore.release();
                }
            } catch (InterruptedException e) {
                future.completeExceptionally(e);
            } finally {
                globalSemaphore.release();
            }

        });


        return future;
    }

    @Override
    public void shutdown() {
        executorService.shutdown();
    }
}
