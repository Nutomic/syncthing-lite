/* 
 * Copyright (C) 2016 Davide Imbriaco
 *
 * This Java file is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.syncthing.java.repository.repo

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import net.syncthing.java.core.interfaces.IndexRepository
import net.syncthing.java.core.interfaces.IndexTransaction
import net.syncthing.java.core.interfaces.TempRepository
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.File
import java.sql.Connection
import java.sql.SQLException
import java.util.*

class SqlRepository(databaseFolder: File) : Closeable, IndexRepository, TempRepository {

    private val logger = LoggerFactory.getLogger(javaClass)
    private val dataSource: HikariDataSource
    //    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    @Throws(SQLException::class)
    private fun getConnection() = dataSource.connection

    init {
        val dbDir = File(databaseFolder, "h2_index_database")
        dbDir.mkdirs()
        assert(dbDir.isDirectory && dbDir.canWrite())
        val jdbcUrl = "jdbc:h2:file:" + File(dbDir, "index").absolutePath + ";TRACE_LEVEL_FILE=0;TRACE_LEVEL_SYSTEM_OUT=0;FILE_LOCK=FS;PAGE_SIZE=1024;CACHE_SIZE=8192;"
        val hikariConfig = HikariConfig()
        hikariConfig.driverClassName = "org.h2.Driver"
        hikariConfig.jdbcUrl = jdbcUrl
        hikariConfig.minimumIdle = 4
        val newDataSource = HikariDataSource(hikariConfig)
        dataSource = newDataSource
        checkDb()
        recreateTemporaryTables()
        //        scheduledExecutorService.submitLogging(new Runnable() {
        //            @Override
        //            public void run() {
        //                Thread.currentThread().setPriority(Thread.MIN_PRIORITY);
        //            }
        //        });
        //        scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
        //            @Override
        //            public void run() {
        //                if (folderStatsDirty) {
        //                    folderStatsDirty = false;
        //                    updateFolderStats();
        //                }
        //            }
        //        }, 15, 30, TimeUnit.SECONDS);
        logger.debug("database ready")
    }

    private fun checkDb() {
        try {
            getConnection().use { connection ->
                connection.prepareStatement("SELECT version_number FROM version").use { statement ->
                    val resultSet = statement.executeQuery()
                    assert(resultSet.first())
                    val version = resultSet.getInt(1)
                    assert(version == VERSION, {"database version mismatch, expected $VERSION, found $version"})
                    logger.info("Database check successful, version = {}", version)
                }
            }
        } catch (ex: SQLException) {
            logger.warn("Invalid database, resetting db", ex)
            getConnection().use {
                initDb(it)
            }
        }
    }

    @Throws(SQLException::class)
    private fun initDb(connection: Connection) {
        logger.info("init db")
        connection.prepareStatement("DROP ALL OBJECTS").use { prepareStatement -> prepareStatement.execute() }

            connection.prepareStatement("CREATE TABLE index_sequence (index_id BIGINT NOT NULL PRIMARY KEY, current_sequence BIGINT NOT NULL)").use { prepareStatement -> prepareStatement.execute() }
            connection.prepareStatement("CREATE TABLE folder_index_info (folder VARCHAR NOT NULL,"
                    + "device_id VARCHAR NOT NULL,"
                    + "index_id BIGINT NOT NULL,"
                    + "local_sequence BIGINT NOT NULL,"
                    + "max_sequence BIGINT NOT NULL,"
                    + "PRIMARY KEY (folder, device_id))").use { prepareStatement -> prepareStatement.execute() }
            connection.prepareStatement("CREATE TABLE folder_stats (folder VARCHAR NOT NULL PRIMARY KEY,"
                    + "file_count BIGINT NOT NULL,"
                    + "dir_count BIGINT NOT NULL,"
                    + "last_update BIGINT NOT NULL,"
                    + "size BIGINT NOT NULL)").use { prepareStatement -> prepareStatement.execute() }
            connection.prepareStatement("CREATE TABLE file_info (folder VARCHAR NOT NULL,"
                    + "path VARCHAR NOT NULL,"
                    + "file_name VARCHAR NOT NULL,"
                    + "parent VARCHAR NOT NULL,"
                    + "size BIGINT,"
                    + "hash VARCHAR,"
                    + "last_modified BIGINT NOT NULL,"
                    + "file_type VARCHAR NOT NULL,"
                    + "version_id BIGINT NOT NULL,"
                    + "version_value BIGINT NOT NULL,"
                    + "is_deleted BOOLEAN NOT NULL,"
                    + "PRIMARY KEY (folder, path))").use { prepareStatement -> prepareStatement.execute() }
            connection.prepareStatement("CREATE TABLE file_blocks (folder VARCHAR NOT NULL,"
                    + "path VARCHAR NOT NULL,"
                    + "hash VARCHAR NOT NULL,"
                    + "size BIGINT NOT NULL,"
                    + "blocks BINARY NOT NULL,"
                    + "PRIMARY KEY (folder, path))").use { prepareStatement -> prepareStatement.execute() }
            connection.prepareStatement("CREATE INDEX file_info_folder ON file_info (folder)").use { prepareStatement -> prepareStatement.execute() }
            connection.prepareStatement("CREATE INDEX file_info_folder_path ON file_info (folder, path)").use { prepareStatement -> prepareStatement.execute() }
            connection.prepareStatement("CREATE INDEX file_info_folder_parent ON file_info (folder, parent)").use { prepareStatement -> prepareStatement.execute() }
            connection.prepareStatement("CREATE TABLE version (version_number INT NOT NULL)").use { prepareStatement -> prepareStatement.execute() }
            connection.prepareStatement("INSERT INTO index_sequence VALUES (?,?)").use { prepareStatement ->
                val newIndexId = Math.abs(Random().nextLong()) + 1
                val newStartingSequence = Math.abs(Random().nextLong()) + 1
                prepareStatement.setLong(1, newIndexId)
                prepareStatement.setLong(2, newStartingSequence)
                assert(prepareStatement.executeUpdate() == 1)
            }
            connection.prepareStatement("INSERT INTO version (version_number) VALUES (?)").use { prepareStatement ->
                prepareStatement.setInt(1, VERSION)
                assert(prepareStatement.executeUpdate() == 1)
            }

        logger.info("database initialized")
    }

    @Throws(SQLException::class)
    private fun recreateTemporaryTables() {
        getConnection().use { connection ->
            connection
                    .prepareStatement("CREATE CACHED TEMPORARY TABLE IF NOT EXISTS temporary_data (record_key VARCHAR NOT NULL PRIMARY KEY," + "record_data BINARY NOT NULL)")
                    .use { prepareStatement -> prepareStatement.execute() }
        }
    }

    override fun <T> runInTransaction(action: (IndexTransaction) -> T): T {
        return getConnection().use {  connection ->
            val transaction = SqlTransaction(connection, ::initDb)

            try {
                connection.autoCommit = false
                connection.transactionIsolation = Connection.TRANSACTION_SERIALIZABLE

                action(transaction)
            } catch (ex: Exception) {
                connection.rollback()

                throw ex
            } finally {
                transaction.close()

                connection.commit()
                connection.autoCommit = true
            }
        }
    }

    override fun close() {
        logger.info("closing index repository (sql)")
        //        scheduledExecutorService.shutdown();
        if (!dataSource.isClosed) {
            dataSource.close()
        }
        //        ExecutorUtils.awaitTerminationSafe(scheduledExecutorService);
    }

    @Throws(SQLException::class)
    override fun pushTempData(data: ByteArray): String {
        getConnection().use { connection ->
            val key = UUID.randomUUID().toString()
            connection.prepareStatement("INSERT INTO temporary_data"
                    + " (record_key,record_data)"
                    + " VALUES (?,?)").use { prepareStatement ->
                prepareStatement.setString(1, key)
                prepareStatement.setBytes(2, data)
                prepareStatement.executeUpdate()
            }
            return key
        }
    }

    @Throws(SQLException::class)
    override fun popTempData(key: String): ByteArray {
        assert(!key.isEmpty())
        getConnection().use { connection ->
            var data: ByteArray? = null
            connection.prepareStatement("SELECT record_data FROM temporary_data WHERE record_key = ?").use { statement ->
                statement.setString(1, key)
                val resultSet = statement.executeQuery()
                assert(resultSet.first())
                data = resultSet.getBytes(1)
            }
            connection.prepareStatement("DELETE FROM temporary_data WHERE record_key = ?").use { statement ->
                statement.setString(1, key)
                val count = statement.executeUpdate()
                assert(count == 1)
            }
            return data!!
        }
    }

    @Throws(SQLException::class)
    override fun deleteTempData(keys: List<String>) {
        getConnection().use { connection ->
            connection.prepareStatement("DELETE FROM temporary_data WHERE record_key = ?").use { statement ->
                keys.forEach {
                    key ->

                    statement.setString(1, key)
                    statement.executeUpdate()
                }
            }
        }
    }

    companion object {
        private const val VERSION = 13
    }
}
