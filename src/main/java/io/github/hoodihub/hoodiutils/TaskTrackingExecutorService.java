package io.github.hoodihub.hoodiutils;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.*;

import static java.util.Optional.ofNullable;

/**
 * Wrapper of an {@link ExecutorService} with a fixed thread pool, which keeps track of the submitted tasks.
 * It provides a method to receive all unfinished tasks at any point in time (while still active or after termination),
 * with details on their current state.
 */
public class TaskTrackingExecutorService {

    private final ExecutorService executorService;
    private final Set<SubmittedTask> unfinishedTasks = ConcurrentHashMap.newKeySet();

    /**
     * Creates an executor service with the given number of threads.
     *
     * @param threads number of threads for the executor.
     */
    public TaskTrackingExecutorService(final int threads) {
        this.executorService = Executors.newFixedThreadPool(threads);
    }

    public <T> Future<T> submit(final Callable<T> task, final String taskName) {
        final SubmittedTask submittedTask = new SubmittedTask(taskName);
        unfinishedTasks.add(submittedTask);
        final Callable<T> wrappedTask = () -> {
            submittedTask.setTimeStarted(LocalDateTime.now());
            final T result = task.call();
            unfinishedTasks.remove(submittedTask);
            return result;
        };
        return executorService.submit(wrappedTask);
    }

    public void shutdown() {
        executorService.shutdown();
    }

    public void shutdownNow() {
        executorService.shutdownNow();
    }

    public boolean awaitTermination(final long timeout, final TimeUnit timeUnit) throws InterruptedException {
        return executorService.awaitTermination(timeout, timeUnit);
    }

    /**
     * @return the current set of unfinished tasks. Note that this returns a copy of the internally held set, so the
     * returned set itself won't change over time. However, the tasks are not copied, so their properties can change.
     */
    public Set<SubmittedTask> getUnfinishedTasks() {
        return new HashSet<>(unfinishedTasks);
    }

    public static final class SubmittedTask {

        private final String taskName;
        private final LocalDateTime timeSubmitted;
        private volatile LocalDateTime timeStarted;

        private SubmittedTask(final String taskName) {
            this.taskName = taskName;
            this.timeSubmitted = LocalDateTime.now();
        }

        /**
         * @return The task name that was passed when submitting the task.
         */
        public String getTaskName() {
            return taskName;
        }

        /**
         * @return Time when the task was submitted.
         */
        public LocalDateTime getTimeSubmitted() {
            return timeSubmitted;
        }

        /**
         * @return Time when the task was started. If it wasn't yet started, an empty optional is returned.
         */
        public Optional<LocalDateTime> getTimeStarted() {
            return ofNullable(timeStarted);
        }

        private void setTimeStarted(LocalDateTime timeStarted) {
            this.timeStarted = timeStarted;
        }
    }
}
