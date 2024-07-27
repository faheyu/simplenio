package com.simplenio

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.Runnable
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.launch
import java.util.concurrent.Callable
import java.util.concurrent.CancellationException
import java.util.concurrent.Delayed
import java.util.concurrent.ExecutionException
import java.util.concurrent.Future
import java.util.concurrent.RunnableScheduledFuture
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.system.measureTimeMillis

@OptIn(ExperimentalCoroutinesApi::class)
class MyThreadPoolExecutor (corePoolSize: Int = 0, name: String = "mypool"): ScheduledThreadPoolExecutor(corePoolSize) {

    private class MyFutureTask <V>(
        private val future: RunnableScheduledFuture<V>
    ) : RunnableScheduledFuture<V> {
        override fun run() {
            if (DEBUG) {
                val executeTime = measureTimeMillis {
                    future.run()
                }

                if (executeTime > MAX_EXECUTION_TIME) {
                    throw RuntimeException(
                        "execution time too long: $executeTime > $MAX_EXECUTION_TIME"
                    )
                }
            } else {
                future.run()
            }
        }

        override fun cancel(mayInterruptIfRunning: Boolean): Boolean {
            return future.cancel(mayInterruptIfRunning)
        }

        override fun isCancelled(): Boolean {
            return future.isCancelled
        }

        override fun isDone(): Boolean {
            return future.isDone
        }

        override fun get(): V {
            return future.get()
        }

        override fun get(timeout: Long, unit: TimeUnit): V {
            return future.get(timeout, unit)
        }

        override fun compareTo(other: Delayed?): Int {
            return future.compareTo(other)
        }

        override fun getDelay(unit: TimeUnit): Long {
            return future.getDelay(unit)
        }

        override fun isPeriodic(): Boolean {
            return future.isPeriodic
        }
    }

    private class MyThreadFactory (name: String): ThreadFactory {
        private val threadNumber = AtomicInteger(1)
        private val namePrefix: String = "$name-"

        override fun newThread(r: Runnable): Thread {
            val t = Thread(
                null, r,
                namePrefix + threadNumber.getAndIncrement(),
                0
            )
            if (t.isDaemon) t.isDaemon = false
            if (t.priority != Thread.NORM_PRIORITY) t.priority = Thread.NORM_PRIORITY
            return t
        }
    }

    companion object {
        private val logger = DebugLogger(MyThreadPoolExecutor::class.java.simpleName)
        private const val DEBUG = false

        /**
         * ensure task execution time is not greater than this value in debug mode
         */
        const val MAX_EXECUTION_TIME = 100L

        /**
         * max number of tasks can be cleared when call [clearExpiredTasks]
         *
         * make clear time is not too long, as it blocks adding new task when clearing
         */
        const val CLEAR_TASK_NUM = 50_000
    }

    /**
     * the dispatcher to launch coroutines
     */
    private val coroutineDispatcher = asCoroutineDispatcher()

    /**
     * [Dispatchers.IO] with customized thread pool size
     */
    var ioDispatcher : CoroutineDispatcher
        private set

    /**
     * save future tasks to handle later
     */
    private val futureTasks = HashMap<Runnable, ArrayList<Future<*>>>()

    init {
        maximumPoolSize = if (corePoolSize > 0) corePoolSize * 2 else Int.MAX_VALUE
        setKeepAliveTime(30, TimeUnit.SECONDS)
        allowCoreThreadTimeOut(true)
        removeOnCancelPolicy = true
        threadFactory = MyThreadFactory(name)

        // init dispatcher here as corePoolSize can be zero
        ioDispatcher =
            if (corePoolSize > 0)
                Dispatchers.IO.limitedParallelism(corePoolSize)
            else
                Dispatchers.IO

        // periodically clear finished tasks
        // this fix memory leaks
        scheduleWithFixedDelay(
            {
                clearExpiredTasks()
            },
            0,
            10_000L,
            TimeUnit.MILLISECONDS
        )
    }

    override fun setCorePoolSize(corePoolSize: Int) {
        super.setCorePoolSize(corePoolSize).also {
            /**
             * update maximum pool size to fix [IllegalArgumentException] when [maximumPoolSize] < [corePoolSize]
             */
            maximumPoolSize = if (corePoolSize > 0) corePoolSize * 2 else Int.MAX_VALUE
            if (corePoolSize > 0 && this.corePoolSize != corePoolSize) {
                ioDispatcher = Dispatchers.IO.limitedParallelism(corePoolSize)
            }
        }
    }

    override fun <V : Any?> decorateTask(
        runnable: Runnable,
        task: RunnableScheduledFuture<V>
    ): RunnableScheduledFuture<V> {
        return MyFutureTask(task)
    }

    override fun <V : Any?> decorateTask(
        callable: Callable<V>,
        task: RunnableScheduledFuture<V>
    ): RunnableScheduledFuture<V> {
        return MyFutureTask(task)
    }

    override fun afterExecute(r: Runnable?, t: Throwable?) {
        super.afterExecute(r, t)

        if (
            t == null
            && r is Future<*>
            && (r as Future<*>).isDone
        ) {
            try {
                (r as Future<*>).get()
            } catch (ce: CancellationException) {
                logger.w("task $r is cancelled\n${ce.stackTraceToString()}")
            } catch (ee: ExecutionException) {
                ee.cause?.let { throw it }
            } catch (ie: InterruptedException) {
                Thread.currentThread().interrupt()
            }
        }

        t?.let { throw t }
    }

    /**
     * schedule task with delay in milliseconds and save it to the [futureTasks] to handle later
     *
     * @param command task to run in future
     * @param delay delay in milliseconds
     * @return the scheduled future task
     */
    fun scheduleAndSave(command: Runnable, delay: Long = 0): ScheduledFuture<*> {
        return super.schedule(command, delay, TimeUnit.MILLISECONDS).also {
            addFutureTask(command, it)
        }
    }

    /**
     * schedule task periodically with [initialDelay] and [delay] in milliseconds and save it to the [futureTasks] to handle later
     *
     * @param command task to run in future
     * @param initialDelay
     * @param delay delay in milliseconds
     * @return the scheduled future task
     */
    fun schedulePeriodAndSave(command: Runnable, initialDelay: Long, delay: Long) : ScheduledFuture<*> {
        return super.scheduleWithFixedDelay(command, initialDelay, delay, TimeUnit.MILLISECONDS).also {
            addFutureTask(command, it)
        }
    }

    /**
     * remove the task saved before
     *
     * @param task task to remove
     * @return true if successful
     */
    fun removeSavedTask(task: Runnable) : Boolean {
        return super.remove(task).also {
            synchronized(futureTasks) {
                futureTasks[task]?.forEach { it.cancel(true) }
                futureTasks.remove(task)
            }
        }
    }

    /**
     * save future task to hash map to handle later
     */
    private fun addFutureTask(command: Runnable, task: ScheduledFuture<*>) {
        synchronized(futureTasks) {
            // add the task to the list if the key command exists in hash map
            if (futureTasks[command]?.add(task) == null) {
                // if the list do not exists for this key, create new one and add the task to the list
                futureTasks[command] = ArrayList<Future<*>>().apply {
                    add(task)
                }
            }
        }
    }

    /**
     * Remove all completed or cancelled tasks
     */
    private fun clearExpiredTasks() {
        var cleared = 0

        val clearTime = measureTimeMillis {
            synchronized(futureTasks) {
                futureTasks.iterator().let {
                    while (it.hasNext()) {
                        val (_, list) = it.next()

                        list.iterator().let { listIterator ->
                            while (listIterator.hasNext()) {
                                val future = listIterator.next()
                                if (future.isDone || future.isCancelled) {
                                    listIterator.remove()
                                    cleared += 1
                                    if (cleared > CLEAR_TASK_NUM) {
                                        return@synchronized
                                    }
                                }
                            }
                        }

                        // remove key that has no future task
                        if (list.isEmpty()) {
                            it.remove()
                        }
                    }
                }
            }
        }

        if (cleared > 0)
            logger.log("cleared $cleared expired tasks took $clearTime ms; remain ${futureTasks.size} tasks")
    }

    /**
     * launch a coroutine with this thread pool as coroutine dispatcher
     */
    fun launchCoroutine(job: Job = Job(), block: suspend CoroutineScope.() -> Unit) : Job {
        return CoroutineScope(job).launch(coroutineDispatcher, block=block)
    }
}