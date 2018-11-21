package net.syncthing.repository.android

import net.syncthing.java.core.interfaces.IndexRepository
import net.syncthing.java.core.interfaces.IndexTransaction
import net.syncthing.repository.android.database.RepositoryDatabase
import java.util.concurrent.Callable

class SqliteIndexRepository(
        private val database: RepositoryDatabase,
        private val closeDatabaseOnClose: Boolean,
        private val clearTempStorageHook: () -> Unit
): IndexRepository {
    // TODO: remove this if possible
    private var folderStatsChangeListener: ((IndexRepository.FolderStatsUpdatedEvent) -> Unit)? = null

    override fun <T> runInTransaction(action: (IndexTransaction) -> T): T {
            return database.runInTransaction (object: Callable<T> {
                override fun call(): T {
                    val transaction = SqliteTransaction(
                            database = database,
                            threadId = Thread.currentThread().id,
                            clearTempStorageHook = clearTempStorageHook,
                            folderStatsChangeListener = folderStatsChangeListener
                    )

                    return try {
                        action(transaction)
                    } finally {
                        transaction.markFinished()
                    }
                }
            })
    }

    override fun close() {
        if (closeDatabaseOnClose) {
            database.close()
        }
    }

    override fun setOnFolderStatsUpdatedListener(listener: ((IndexRepository.FolderStatsUpdatedEvent) -> Unit)?) {
        folderStatsChangeListener = listener
    }
}
