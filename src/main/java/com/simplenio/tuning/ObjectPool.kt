package com.simplenio.tuning

import java.util.LinkedList

abstract class ObjectPool<T: Any> {

    companion object {
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

        private var onRecycleListener : (() -> Unit)? = null

        /**
         * get the actual object
         */
        fun get() : T {
            return obj
        }

        fun recycle() {
            synchronized(pool) {
                if (!inUse)
                    return

                inUse = false
                onRecycleListener?.invoke()
            }
        }

        fun onRecycle(listener: () -> Unit) {
            onRecycleListener = listener
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
        synchronized(pool) {
            var reusableObject : ReusableObject

            // find usable object not in use
            pool.iterator().let { poolIterator ->
                while (poolIterator.hasNext()) {
                    reusableObject = poolIterator.next()

                    if (canReuse(reusableObject, predicate)) {
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

    /**
     * get the first object is not used by anywhere yielding the smallest value of the given [selector] and mark it as in use.
     * Returns null if not found.
     *
     * @param selector the function yielding from
     * @return the reusable object or null if not found
     */
    fun <R : Comparable<R>> getMinByOrNull(selector: (T) -> R) : ReusableObject? {
        synchronized(pool) {
            // get not in use object, and get min by selector
            val reusableObject = pool.filter { !it.inUse }.minByOrNull {
                selector(it.obj)
            } ?: return null

            return reusableObject.also {
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
        // clean first
        clean()

        synchronized(pool) {
            val reusableObject = ReusableObject(obj)
            reusableObject.inUse = inUse
            pool.addLast(reusableObject)
            return reusableObject
        }
    }

    /**
     * removes [ReusableObject] from the pool.
     * This function by default removes objects until the pool size is no larger than [MAX_OBJECTS].
     * Sub-classes can override this function to change behavior.
     *
     * this function is called when add new object to the pool
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
        synchronized(pool) {
            pool.iterator().let {
                while (it.hasNext()) {
                    val reusableObject = it.next()
                    // ignore in use object
                    if (reusableObject.inUse)
                        continue

                    if (filter(reusableObject.obj)) {
                        it.remove()
                    }
                }
            }
        }
    }
}