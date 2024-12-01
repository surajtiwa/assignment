package executor;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskExecutorService implements Main.TaskExecutor {

    private final int maxConcurrency;
    private final ExecutorService executorService;
    private final Map<UUID, Object> groupLocks = new ConcurrentHashMap<>();
    private final BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();
    private final AtomicInteger runningTasks = new AtomicInteger(0);

    public TaskExecutorService(int maxConcurrency) {
        this.maxConcurrency = maxConcurrency;
        this.executorService = Executors.newFixedThreadPool(maxConcurrency);
        startTaskProcessor();
    }

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
    public <T> Future<T> submitTask(Main.Task<T> task) {
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

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        TaskExecutorService taskExecutor = new TaskExecutorService(4);

        Main.TaskGroup group1 = new Main.TaskGroup(UUID.randomUUID());
        Main.TaskGroup group2 = new Main.TaskGroup(UUID.randomUUID());

        List<Future<String>> results = new ArrayList<>();

        for (int i = 1; i <= 5; i++) {
            int taskId = i;
            results.add(taskExecutor.submitTask(new Main.Task<>(
                    UUID.randomUUID(),
                    group1,
                    Main.TaskType.WRITE,
                    () -> {
                        Thread.sleep(1000);
                        return "Group1 Task " + taskId + " completed";
                    }
            )));
        }

        for (int i = 1; i <= 5; i++) {
            int taskId = i;
            results.add(taskExecutor.submitTask(new Main.Task<>(
                    UUID.randomUUID(),
                    group2,
                    Main.TaskType.READ,
                    () -> {
                        Thread.sleep(500);
                        return "Group2 Task " + taskId + " completed";
                    }
            )));
        }

        for (Future<String> result : results) {
            System.out.println(result.get());
        }

        taskExecutor.shutdown();
    }
}
