package net.syncthing.java.bep.utils

inline fun <T> Iterable<T>.longMaxBy(selector: (T) -> Long, defaultValue: Long): Long {
    var max = defaultValue

    this.forEach {
        max = Math.max(max, selector(it))
    }

    return max
}
