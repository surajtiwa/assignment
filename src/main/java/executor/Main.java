package executor;

import java.util.*;
import java.util.concurrent.*;

public class Main {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        TaskExecutor taskExecutor = new TaskExecutorServiceSynchronized(4);
        TaskExecutor taskExecutorSemaphore=new TaskExecutorSemaphoreImpl(4);
     //   startTaskExecutor(taskExecutor);
        startTaskExecutor(taskExecutorSemaphore);

    }

    public static void startTaskExecutor(TaskExecutor taskExecutor) throws ExecutionException, InterruptedException {
        TaskGroup group1 = new TaskGroup(UUID.randomUUID());
        TaskGroup group2 = new TaskGroup(UUID.randomUUID());

        List<Future<String>> results = new ArrayList<>();

        for (int i = 1; i <= 5; i++) {
            int taskId = i;
            results.add(taskExecutor.submitTask(new Task<>(
                    UUID.randomUUID(),
                    group1,
                    TaskType.WRITE,
                    () -> {
                        Thread.sleep(1000);
                        return "Group1 Task " + taskId + " completed";
                    }
            )));
        }

        for (int i = 1; i <= 5; i++) {
            int taskId = i;
            results.add(taskExecutor.submitTask(new Task<>(
                    UUID.randomUUID(),
                    group2,
                    TaskType.READ,
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