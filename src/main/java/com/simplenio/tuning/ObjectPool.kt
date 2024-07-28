package com.simplenio.tuning

import com.simplenio.DebugLogger
import java.util.LinkedList

abstract class ObjectPool<T: Any> {

    companion object {
        private val logger = DebugLogger(ObjectPool::class.java.simpleName)

        /**
         * max number of reusable objects in the pool
         */
        var MAX_OBJECTS = 200
    }

    /**
     * Class holds the reference to object need to reuse
     */
    inner class ReusableObject(internal val obj: T) {
        /**
         * Determine if the object in use somewhere
         */
        var inUse = false

        /**
         * determine if the object is removed from the pool and added to the pool again
         */
        var isRecycled = false

        /**
         * Determine if it need to recycle object when finalize() called
         */
        var autoRecycle = true

        /**
         * get the actual object
         */
        fun get() : T {
            return obj
        }

        protected fun finalize() {
            // auto cleaning
            clean()

            if (!autoRecycle) return

            logger.d("finalize ReusableObject, recycle or add object to pool again")
            try {
                /**
                 * try to recycle the object if it still exist in the pool
                 */
                recycle(obj)
            } catch (_: IllegalArgumentException) {
                /**
                 * do this to prevent somewhere get same object, see [ObjectPool.get]
                 */
                isRecycled = true

                /**
                 * if we got error here, the [ReusableObject] should be removed from the pool
                 * so we can add the object to the pool again
                 */
                add(obj, false)
            }
        }
    }

    private val pool = LinkedList<ReusableObject>()

    /**
     * get the object is not used by anywhere and mark it as in use
     *
     * @param predicate if define, only get the object matching the given [predicate]
     * @return the reusable object or null if not found
     */
    protected fun get(predicate: ((T) -> Boolean)? = null) : ReusableObject? {
        synchronized(this) {
            var reusableObject : ReusableObject

            // find usable object not in use
            pool.iterator().let { poolIterator ->
                while (poolIterator.hasNext()) {
                    reusableObject = poolIterator.next()

                    if (canReuse(reusableObject, predicate)) {
                        /**
                         * found the reusable object
                         * we remove the [ReusableObject] here, let GC delete it and call finalize()
                         * to automatically recycle the object
                         */
                        poolIterator.remove()
                        return reusableObject.also {
                            // mark as in use so it can not be used in anywhere
                            it.inUse = true
                        }
                    }
                }
            }

            return null
        }
    }

    fun <R : Comparable<R>> getMinByOrNull(selector: (T) -> R) : ReusableObject? {
        synchronized(this) {
            // get not in use object, and get min by selector
            val reusableObject = pool.filter { !it.inUse }.minByOrNull {
                selector(it.obj)
            } ?: return null

            return reusableObject.also {
                /**
                 * remove for recycling, see [ObjectPool.get]
                 */
                pool.remove(reusableObject)
                // mark as in use so it can not be used in anywhere
                reusableObject.inUse = true
            }
        }
    }

    /**
     * Determine if the object can be reused or not
     *
     * @param reusableObject the [ReusableObject] need to check
     * @param predicate additional condition to check
     * @return true if the [ReusableObject] can be reused now, false otherwise
     */
    private fun canReuse(reusableObject : ReusableObject, predicate: ((T) -> Boolean)? = null) : Boolean {
        /**
         * we can get the [ReusableObject] newly recycled here
         * but we shouldn't get it
         * as its finalizer waiting for adding itself to the pool (due to lock)
         * it will reset the [ReusableObject.inUse] property to false when added
         * and somewhere can get the same [ReusableObject]
         *
         * to fix this, set [ReusableObject.isRecycled] to false and ignore it
         */
        if (reusableObject.isRecycled) {
            reusableObject.isRecycled = false
            return false
        }

        // ignore object in use
        if (reusableObject.inUse)
            return false

        // if we got here, we found the reusable object
        if (predicate == null || predicate(reusableObject.obj)) {
            return true
        }

        return false
    }

    /**
     * add the object to the pool
     *
     * @param obj the reusable object
     * @param inUse mark the new object as in use or not
     * @return the added object
     */
    protected fun add(obj : T, inUse: Boolean = false) : ReusableObject {
        synchronized(this) {
            val reusableObject = ReusableObject(obj)
            reusableObject.inUse = inUse

            // only add this object to the pool if not in use
            // so its finalizer can be invoked when garbage-collected
            // and add again to the pool for reusing
            if (!inUse) {
                pool.addLast(reusableObject)
            }

            return reusableObject
        }
    }

    /**
     * make object created from this pool reusable
     *
     * @param predicate recycle first object matches the predicate
     * @throws [IllegalArgumentException] when the object is not created from the pool
     */
    @Throws(java.lang.IllegalArgumentException::class)
    protected fun recycle(predicate: (T) -> Boolean) {
        synchronized(this) {
            pool.forEach {
                if (predicate(it.obj)) {
                    // mark this object as not in use, so it can be reused in somewhere
                    it.inUse = false
                    return
                }
            }

            // if we got here, no such object in the pool
            throw IllegalArgumentException("the object is not created from the pool")
        }
    }

    /**
     * make object created from this pool reusable
     *
     * @param obj the object created from this pool
     * @throws [IllegalArgumentException] when the object is not created from the pool
     */
    @Throws(java.lang.IllegalArgumentException::class)
    protected fun recycle(obj: T) {
        // we don't need to use lock here
        // as the recycle() function below do this for us
        recycle {
            obj === it
        }
    }

    /**
     * removes [ReusableObject] from the pool.
     * This function by default removes objects until the pool size is no larger than [MAX_OBJECTS].
     * Sub-classes can override this function to change behavior.
     *
     * this function is called by [ReusableObject] finalizer for cleaning automatically.
     */
    protected open fun clean() {
        clean {
            pool.size > MAX_OBJECTS
        }
    }


    /**
     * removes all [ReusableObject]s from the pool that match the [filter].
     *
     * @param filter the condition to remove the [ReusableObject] from the pool.
     */
    protected fun clean(filter: (T) -> Boolean) {
        synchronized(this) {
            pool.iterator().let {
                while (it.hasNext()) {
                    val reusableObject = it.next()
                    // ignore in use object
                    if (reusableObject.inUse)
                        continue

                    if (filter(reusableObject.obj)) {
                        // set auto recycle to false, so we can completely remove it from the pool
                        reusableObject.autoRecycle = false
                        it.remove()
                    }
                }
            }
        }
    }
}