package net.syncthing.java.bep.utils

inline fun <T> Iterable<T>.longSumBy(selector: (T) -> Long): Long {
    var sum = 0L

    this.forEach {
        sum += selector(it)
    }

    return sum
}
