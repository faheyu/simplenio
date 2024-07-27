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
    private inner class ReusableObject(val obj: T) {
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

        protected fun finalize() {
            // auto cleaning
            clean()

            if (!autoRecycle) return

            logger.log("finalize ReusableObject, recycle or add object to pool again")
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
     * @param predicate if define, only get the object matches the [predicate]
     * @return the reusable object or null if not found
     */
    protected fun get(predicate: ((T) -> Boolean)? = null) : T? {
        synchronized(this) {
            var reusableObject : ReusableObject

            // find usable object not in use
            pool.iterator().let { poolIterator ->
                while (poolIterator.hasNext()) {
                    reusableObject = poolIterator.next()

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
                        continue
                    }

                    // ignore object in use
                    if (reusableObject.inUse)
                        continue

                    // if we got here, we found the reusable object
                    if (predicate == null || predicate(reusableObject.obj)) {
                        /**
                         * found the reusable object
                         * we remove the [ReusableObject] here, let GC delete it and call finalize()
                         * to automatically recycle the object
                         */
                        poolIterator.remove()
                        return reusableObject.let {
                            // mark as in use so it can not be used in anywhere
                            it.inUse = true
                            it.obj
                        }
                    }
                }
            }

            return null
        }
    }

    /**
     * add the object to the pool
     *
     * @param obj the reusable object
     * @param setInUse mark the new object as in use or not
     * @return the added object
     */
    protected fun add(obj : T, setInUse: Boolean = false) : T {
        synchronized(this) {
            val reusableObject = ReusableObject(obj)
            reusableObject.inUse = setInUse
            pool.addLast(reusableObject)
            return obj
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
     * removes [ReusableObject] from the pool matches [filter].
     * If number of objects in the pool over [MAX_OBJECTS] limit,
     * it will remove objects until the pool size not greater than [MAX_OBJECTS]
     *
     * this function is called by [ReusableObject] for cleaning automatically
     *
     * @param filter can be null, the condition to remove the [ReusableObject] from the pool.
     */
    protected open fun clean(filter: ((T) -> Boolean)? = null) {
        synchronized(this) {
            pool.iterator().let {
                while (it.hasNext()) {
                    val reusableObject = it.next()
                    if ((filter != null && filter(reusableObject.obj)) || pool.size > MAX_OBJECTS) {
                        // set auto recycle to false, so we can completely remove it from the pool
                        reusableObject.autoRecycle = false
                        it.remove()
                    }
                }
            }
        }
    }
}