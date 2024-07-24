package com.simplenio

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
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

class MyThreadPoolExecutor (corePoolSize: Int = 0, name: String = "mypool"): ScheduledThreadPoolExecutor(corePoolSize) {

    private inner class MyFutureTask <V>(
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
        const val MAX_EXECUTION_TIME = 100L
    }

    /**
     * the dispatcher to launch coroutines
     */
    private val coroutineDispatcher = asCoroutineDispatcher()

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
     * schedule with delay in milliseconds
     */
    fun schedule(command: Runnable, delay: Long): ScheduledFuture<*> {
        return schedule(command, delay, TimeUnit.MILLISECONDS)
    }

    override fun schedule(command: Runnable, delay: Long, unit: TimeUnit): ScheduledFuture<*> {
        return super.schedule(command, delay, unit).also {
            if (delay > 0) {
                addFutureTask(command, it)
            }
        }
    }

    override fun scheduleWithFixedDelay(
        command: Runnable,
        initialDelay: Long,
        delay: Long,
        unit: TimeUnit
    ): ScheduledFuture<*> {
        return super.scheduleWithFixedDelay(command, initialDelay, delay, unit).also {
            if (delay > 0) {
                addFutureTask(command, it)
            }
        }
    }

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

    override fun remove(task: Runnable?): Boolean {
        return super.remove(task).also {
            if (task != null) {
                synchronized(futureTasks) {
                    futureTasks[task]?.forEach { it.cancel(true) }
                }
            }
        }
    }

    /**
     * Remove all completed or cancelled tasks
     */
    private fun clearExpiredTasks() {
        var cleared = 0

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

        if (cleared > 0)
            println("${logger.tagName} cleared $cleared expired tasks")
    }

    fun launchCoroutine(job: Job = Job(), block: suspend CoroutineScope.() -> Unit) : Job {
        return CoroutineScope(job).launch(coroutineDispatcher, block=block)
    }
}