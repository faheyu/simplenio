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
import java.util.concurrent.TimeUnit

class MyThreadPoolExecutor (corePoolSize: Int): ScheduledThreadPoolExecutor(corePoolSize) {

    private inner class MyFutureTask <V>(
        private val future: RunnableScheduledFuture<V>
    ) : RunnableScheduledFuture<V> {
        override fun run() {
            if (DEBUG) {
                synchronized(taskExecutionTimes) {
                    val startTime = taskExecutionTimes[future]
                    if (startTime != null) {
                        throw RuntimeException(
                            "already init start time $startTime, now = ${System.currentTimeMillis()}"
                        )
                    }
                    taskExecutionTimes[future] = System.currentTimeMillis()

                    future.run()

                    val executeTime = System.currentTimeMillis() - taskExecutionTimes[future]!!
                    taskExecutionTimes.remove(future)

                    if (executeTime > MAX_EXECUTION_TIME) {
                        throw RuntimeException(
                            "execution time too long: $executeTime > $MAX_EXECUTION_TIME"
                        )
                    }
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

    companion object {
        @Suppress("unused")
        private val logger = DebugLogger(MyThreadPoolExecutor::class.java.simpleName)
        private const val DEBUG = false
        const val MAX_EXECUTION_TIME = 100L
    }

    private val coroutineDispatcher = asCoroutineDispatcher()
    private val futureTasks = HashMap<Runnable, ArrayList<Future<*>>>()
    private val taskExecutionTimes = HashMap<Runnable, Long>()

    init {
        maximumPoolSize = if (corePoolSize > 0) corePoolSize * 2 else Int.MAX_VALUE
        setKeepAliveTime(30, TimeUnit.SECONDS)
        allowCoreThreadTimeOut(true)
        removeOnCancelPolicy = true
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
            } catch (_: CancellationException) {

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
            if (futureTasks[command]?.add(task) == null) {
                futureTasks[command] = ArrayList<Future<*>>().apply {
                    add(task)
                }
            } else {
                clearExpiredTasks()
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

            clearExpiredTasks()
        }
    }

    private fun clearExpiredTasks() {
        synchronized(futureTasks) {
            futureTasks.iterator().let {
                while (it.hasNext()) {
                    val (_, list) = it.next()

                    list.iterator().let { listIterator ->
                        while (listIterator.hasNext()) {
                            val future = listIterator.next()
                            if (future.isDone || future.isCancelled) {
                                listIterator.remove()
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

    fun launchCoroutine(job: Job = Job(), block: suspend CoroutineScope.() -> Unit) : Job {
        return CoroutineScope(job).launch(coroutineDispatcher, block=block)
    }
}