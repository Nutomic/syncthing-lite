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

import com.google.protobuf.ByteString
import com.google.protobuf.InvalidProtocolBufferException
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import net.syncthing.java.bep.BlockExchangeExtraProtos
import net.syncthing.java.bep.BlockExchangeProtos
import net.syncthing.java.core.beans.*
import net.syncthing.java.core.beans.FileInfo.FileType
import net.syncthing.java.core.beans.FileInfo.Version
import net.syncthing.java.core.interfaces.IndexRepository
import net.syncthing.java.core.interfaces.Sequencer
import net.syncthing.java.core.interfaces.TempRepository
import org.apache.commons.lang3.tuple.Pair
import org.bouncycastle.util.encoders.Hex
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.File
import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Types
import java.util.*

class SqlRepository(databaseFolder: File) : Closeable, IndexRepository, TempRepository {

    private val logger = LoggerFactory.getLogger(javaClass)
    private var sequencer: Sequencer = IndexRepoSequencer()
    private val dataSource: HikariDataSource
    //    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    private var onFolderStatsUpdatedListener: ((IndexRepository.FolderStatsUpdatedEvent) -> Unit)? = null

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

    override fun setOnFolderStatsUpdatedListener(listener: ((IndexRepository.FolderStatsUpdatedEvent) -> Unit)?) {
        onFolderStatsUpdatedListener = listener
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
            initDb()
        }
    }

    @Throws(SQLException::class)
    private fun initDb() {
        logger.info("init db")
        getConnection().use { connection -> connection.prepareStatement("DROP ALL OBJECTS").use { prepareStatement -> prepareStatement.execute() } }

        getConnection().use { connection ->
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

    override fun getSequencer(): Sequencer = sequencer

    @Throws(SQLException::class)
    private fun readFolderIndexInfo(resultSet: ResultSet): IndexInfo {
        return IndexInfo.newBuilder()
                .setFolder(resultSet.getString("folder"))
                .setDeviceId(resultSet.getString("device_id"))
                .setIndexId(resultSet.getLong("index_id"))
                .setLocalSequence(resultSet.getLong("local_sequence"))
                .setMaxSequence(resultSet.getLong("max_sequence"))
                .build()
    }

    @Throws(SQLException::class)
    override fun updateIndexInfo(indexInfo: IndexInfo) {
        getConnection().use { connection ->
            connection.prepareStatement("MERGE INTO folder_index_info"
                    + " (folder,device_id,index_id,local_sequence,max_sequence)"
                    + " VALUES (?,?,?,?,?)").use { prepareStatement ->
                prepareStatement.setString(1, indexInfo.folderId)
                prepareStatement.setString(2, indexInfo.deviceId)
                prepareStatement.setLong(3, indexInfo.indexId)
                prepareStatement.setLong(4, indexInfo.localSequence)
                prepareStatement.setLong(5, indexInfo.maxSequence)
                prepareStatement.executeUpdate()
            }
        }
    }

    override fun findIndexInfoByDeviceAndFolder(deviceId: DeviceId, folder: String): IndexInfo? {
        val key = Pair.of(deviceId, folder)
        return doFindIndexInfoByDeviceAndFolder(key.left, key.right)
    }

    @Throws(SQLException::class)
    private fun doFindIndexInfoByDeviceAndFolder(deviceId: DeviceId, folder: String): IndexInfo? {
        getConnection().use { connection ->
            connection.prepareStatement("SELECT * FROM folder_index_info WHERE device_id=? AND folder=?").use { prepareStatement ->
                prepareStatement.setString(1, deviceId.deviceId)
                prepareStatement.setString(2, folder)
                val resultSet = prepareStatement.executeQuery()
                return if (resultSet.first()) {
                    readFolderIndexInfo(resultSet)
                } else {
                    null
                }
            }
        }
    }

    @Throws(SQLException::class)
    override fun findFileInfo(folder: String, path: String): FileInfo? {
        getConnection().use { connection ->
            connection.prepareStatement("SELECT * FROM file_info WHERE folder=? AND path=?").use { prepareStatement ->
                prepareStatement.setString(1, folder)
                prepareStatement.setString(2, path)
                val resultSet = prepareStatement.executeQuery()
                return if (resultSet.first()) {
                    readFileInfo(resultSet)
                } else {
                    null
                }
            }
        }
    }

    @Throws(SQLException::class)
    override fun findFileInfoLastModified(folder: String, path: String): Date? {
        getConnection().use { connection ->
            connection.prepareStatement("SELECT last_modified FROM file_info WHERE folder=? AND path=?").use { prepareStatement ->
                prepareStatement.setString(1, folder)
                prepareStatement.setString(2, path)
                val resultSet = prepareStatement.executeQuery()
                return if (resultSet.first()) {
                    Date(resultSet.getLong("last_modified"))
                } else {
                    null
                }
            }
        }
    }

    @Throws(SQLException::class)
    override fun findNotDeletedFileInfo(folder: String, path: String): FileInfo? {
        getConnection().use { connection ->
            connection.prepareStatement("SELECT * FROM file_info WHERE folder=? AND path=? AND is_deleted=FALSE").use { prepareStatement ->
                prepareStatement.setString(1, folder)
                prepareStatement.setString(2, path)
                val resultSet = prepareStatement.executeQuery()
                return if (resultSet.first()) {
                    readFileInfo(resultSet)
                } else {
                    null
                }
            }
        }
    }

    @Throws(SQLException::class)
    private fun readFileInfo(resultSet: ResultSet): FileInfo {
        val folder = resultSet.getString("folder")
        val path = resultSet.getString("path")
        val fileType = FileType.valueOf(resultSet.getString("file_type"))
        val lastModified = Date(resultSet.getLong("last_modified"))
        val versionList = listOf(Version(resultSet.getLong("version_id"), resultSet.getLong("version_value")))
        val isDeleted = resultSet.getBoolean("is_deleted")
        val builder = FileInfo.Builder()
                .setFolder(folder)
                .setPath(path)
                .setLastModified(lastModified)
                .setVersionList(versionList)
                .setDeleted(isDeleted)
        return if (fileType == FileType.DIRECTORY) {
            builder.setTypeDir().build()
        } else {
            builder.setTypeFile().setSize(resultSet.getLong("size")).setHash(resultSet.getString("hash")).build()
        }
    }

    @Throws(SQLException::class, InvalidProtocolBufferException::class)
    override fun findFileBlocks(folder: String, path: String): FileBlocks? {
        getConnection().use { connection ->
            connection.prepareStatement("SELECT * FROM file_blocks WHERE folder=? AND path=?").use { prepareStatement ->
                prepareStatement.setString(1, folder)
                prepareStatement.setString(2, path)
                val resultSet = prepareStatement.executeQuery()
                return if (resultSet.first()) {
                    readFileBlocks(resultSet)
                } else {
                    null
                }
            }
        }
    }

    @Throws(SQLException::class, InvalidProtocolBufferException::class)
    private fun readFileBlocks(resultSet: ResultSet): FileBlocks {
        val blocks = BlockExchangeExtraProtos.Blocks.parseFrom(resultSet.getBytes("blocks"))
        val blockList = blocks.blocksList.map { record ->
            BlockInfo(record!!.offset, record.size, Hex.toHexString(record.hash.toByteArray()))
        }
        return FileBlocks(resultSet.getString("folder"), resultSet.getString("path"), blockList)
    }

    @Throws(SQLException::class)
    override fun updateFileInfo(fileInfo: FileInfo, fileBlocks: FileBlocks?) {
        val version = fileInfo.versionList.last()
        //TODO open transsaction, rollback
        getConnection().use { connection ->
            if (fileBlocks != null) {
                FileInfo.checkBlocks(fileInfo, fileBlocks)
                connection.prepareStatement("MERGE INTO file_blocks"
                        + " (folder,path,hash,size,blocks)"
                        + " VALUES (?,?,?,?,?)").use { prepareStatement ->
                    prepareStatement.setString(1, fileBlocks.folder)
                    prepareStatement.setString(2, fileBlocks.path)
                    prepareStatement.setString(3, fileBlocks.hash)
                    prepareStatement.setLong(4, fileBlocks.size)
                    prepareStatement.setBytes(5, BlockExchangeExtraProtos.Blocks.newBuilder()
                            .addAllBlocks(fileBlocks.blocks.map { input ->
                                BlockExchangeProtos.BlockInfo.newBuilder()
                                        .setOffset(input.offset)
                                        .setSize(input.size)
                                        .setHash(ByteString.copyFrom(Hex.decode(input.hash)))
                                        .build()
                            }).build().toByteArray())
                    prepareStatement.executeUpdate()
                }
            }
            val oldFileInfo = findFileInfo(fileInfo.folder, fileInfo.path)
            connection.prepareStatement("MERGE INTO file_info"
                    + " (folder,path,file_name,parent,size,hash,last_modified,file_type,version_id,version_value,is_deleted)"
                    + " VALUES (?,?,?,?,?,?,?,?,?,?,?)").use { prepareStatement ->
                prepareStatement.setString(1, fileInfo.folder)
                prepareStatement.setString(2, fileInfo.path)
                prepareStatement.setString(3, fileInfo.fileName)
                prepareStatement.setString(4, fileInfo.parent)
                prepareStatement.setLong(7, fileInfo.lastModified.time)
                prepareStatement.setString(8, fileInfo.type.name)
                prepareStatement.setLong(9, version.id)
                prepareStatement.setLong(10, version.value)
                prepareStatement.setBoolean(11, fileInfo.isDeleted)
                if (fileInfo.isDirectory()) {
                    prepareStatement.setNull(5, Types.BIGINT)
                    prepareStatement.setNull(6, Types.VARCHAR)
                } else {
                    prepareStatement.setLong(5, fileInfo.size!!)
                    prepareStatement.setString(6, fileInfo.hash)
                }
                prepareStatement.executeUpdate()
            }
            //update stats
            var deltaFileCount: Long = 0
            var deltaDirCount: Long = 0
            var deltaSize: Long = 0
            val oldMissing = oldFileInfo == null || oldFileInfo.isDeleted
            val newMissing = fileInfo.isDeleted
            val oldSizeMissing = oldMissing || !oldFileInfo!!.isFile()
            val newSizeMissing = newMissing || !fileInfo.isFile()
            if (!oldSizeMissing) {
                deltaSize -= oldFileInfo!!.size!!
            }
            if (!newSizeMissing) {
                deltaSize += fileInfo.size!!
            }
            if (!oldMissing) {
                if (oldFileInfo!!.isFile()) {
                    deltaFileCount--
                } else if (oldFileInfo.isDirectory()) {
                    deltaDirCount--
                }
            }
            if (!newMissing) {
                if (fileInfo.isFile()) {
                    deltaFileCount++
                } else if (fileInfo.isDirectory()) {
                    deltaDirCount++
                }
            }
            val folderStats = updateFolderStats(connection, fileInfo.folder, deltaFileCount, deltaDirCount, deltaSize, fileInfo.lastModified)

            onFolderStatsUpdatedListener?.invoke(object : IndexRepository.FolderStatsUpdatedEvent() {
                override fun getFolderStats(): List<FolderStats> {
                    return listOf(folderStats)
                }
            })
        }
    }

    @Throws(SQLException::class)
    override fun findNotDeletedFilesByFolderAndParent(folder: String, parentPath: String): MutableList<FileInfo> {
        getConnection().use { connection ->
            connection.prepareStatement("SELECT * FROM file_info WHERE folder=? AND parent=? AND is_deleted=FALSE").use { prepareStatement ->
                prepareStatement.setString(1, folder)
                prepareStatement.setString(2, parentPath)
                val resultSet = prepareStatement.executeQuery()
                val list = mutableListOf<FileInfo>()
                while (resultSet.next()) {
                    list.add(readFileInfo(resultSet))
                }
                return list
            }
        }
    }

    @Throws(SQLException::class)
    override fun findFileInfoBySearchTerm(query: String): List<FileInfo> {
        assert(query.isNotBlank())
        //        checkArgument(maxResult > 0);
        //        try (Connection connection = getConnection(); PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM file_info WHERE LOWER(file_name) LIKE ? AND is_deleted=FALSE LIMIT ?")) {
            getConnection().use { connection ->
                connection.prepareStatement("SELECT * FROM file_info WHERE LOWER(file_name) REGEXP ? AND is_deleted=FALSE").use { preparedStatement ->
                    //        try (Connection connection = getConnection(); PreparedStatement prepareStatement = connection.prepareStatement("SELECT * FROM file_info LIMIT 10")) {
                    //            preparedStatement.setString(1, "%" + query.trim().toLowerCase() + "%");
                    preparedStatement.setString(1, query.trim { it <= ' ' }.toLowerCase())
                    //            preparedStatement.setInt(2, maxResult);
                    val resultSet = preparedStatement.executeQuery()
                    val list = mutableListOf<FileInfo>()
                    while (resultSet.next()) {
                        list.add(readFileInfo(resultSet))
                    }
                    return list
                }
            }
    }

    @Throws(SQLException::class)
    override fun countFileInfoBySearchTerm(query: String): Long {
        assert(query.isNotBlank())
        getConnection().use { connection ->
            connection.prepareStatement("SELECT COUNT(*) FROM file_info WHERE LOWER(file_name) REGEXP ? AND is_deleted=FALSE").use { preparedStatement ->
                //        try (Connection connection = getConnection(); PreparedStatement preparedStatement = connection.prepareStatement("SELECT COUNT(*) FROM file_info")) {
                preparedStatement.setString(1, query.trim { it <= ' ' }.toLowerCase())
                val resultSet = preparedStatement.executeQuery()
                assert(resultSet.first())
                return resultSet.getLong(1)
            }
        }
    }

    // FILE INFO - END
    override fun clearIndex() {
        initDb()
        sequencer = IndexRepoSequencer()
    }

    // FOLDER STATS - BEGIN
    @Throws(SQLException::class)
    private fun readFolderStats(resultSet: ResultSet): FolderStats {
        return FolderStats.Builder()
                .setFolder(resultSet.getString("folder"))
                .setDirCount(resultSet.getLong("dir_count"))
                .setFileCount(resultSet.getLong("file_count"))
                .setSize(resultSet.getLong("size"))
                .setLastUpdate(Date(resultSet.getLong("last_update")))
                .build()
    }

    override fun findFolderStats(folder: String): FolderStats? {
        return doFindFolderStats(folder)
    }

    @Throws(SQLException::class)
    private fun doFindFolderStats(folder: String): FolderStats? {
        getConnection().use { connection ->
            connection.prepareStatement("SELECT * FROM folder_stats WHERE folder=?").use { prepareStatement ->
                prepareStatement.setString(1, folder)
                val resultSet = prepareStatement.executeQuery()
                return if (resultSet.first()) {
                    readFolderStats(resultSet)
                } else {
                    null
                }
            }
        }
    }

    @Throws(SQLException::class)
    override fun findAllFolderStats(): List<FolderStats> {
        getConnection().use { connection ->
            connection.prepareStatement("SELECT * FROM folder_stats").use { prepareStatement ->
                val resultSet = prepareStatement.executeQuery()
                val list = mutableListOf<FolderStats>()
                while (resultSet.next()) {
                    val folderStats = readFolderStats(resultSet)
                    list.add(folderStats)
                }
                return list
            }
        }
    }

    @Throws(SQLException::class)
    private fun updateFolderStats(connection: Connection, folder: String, deltaFileCount: Long, deltaDirCount: Long, deltaSize: Long, lastUpdate: Date): FolderStats {
        val oldFolderStats = findFolderStats(folder)
        val newFolderStats: FolderStats
        if (oldFolderStats == null) {
            newFolderStats = FolderStats.Builder()
                    .setDirCount(deltaDirCount)
                    .setFileCount(deltaFileCount)
                    .setFolder(folder)
                    .setLastUpdate(lastUpdate)
                    .setSize(deltaSize)
                    .build()
        } else {
            newFolderStats = oldFolderStats.copyBuilder()
                    .setDirCount(oldFolderStats.dirCount + deltaDirCount)
                    .setFileCount(oldFolderStats.fileCount + deltaFileCount)
                    .setSize(oldFolderStats.size + deltaSize)
                    .setLastUpdate(if (lastUpdate.after(oldFolderStats.lastUpdate)) lastUpdate else oldFolderStats.lastUpdate)
                    .build()
        }
        updateFolderStats(connection, newFolderStats)
        return newFolderStats
    }

    //    private void updateFolderStats() {
    //        logger.info("updateFolderStats BEGIN");
    //        final Map<String, FolderStats.Builder> map = Maps.newHashMap();
    //        final Function<String, FolderStats.Builder> func = new Function<String, FolderStats.Builder>() {
    //            @Override
    //            public FolderStats.Builder apply(String folder) {
    //                FolderStats.Builder res = map.get(folder);
    //                if (res == null) {
    //                    res = FolderStats.newBuilder().setFolder(folder);
    //                    map.put(folder, res);
    //                }
    //                return res;
    //            }
    //        };
    //        final List<FolderStats> list;
    //        try (Connection connection = getConnection()) {
    //            try (PreparedStatement prepareStatement = connection.prepareStatement("SELECT folder, COUNT(*) AS file_count, SUM(size) AS size, MAX(last_modified) AS last_update FROM file_info WHERE file_type=? AND is_deleted=FALSE GROUP BY folder")) {
    //                prepareStatement.setString(1, FileType.FILE.name());
    //                ResultSet resultSet = prepareStatement.executeQuery();
    //                while (resultSet.next()) {
    //                    FolderStats.Builder builder = func.apply(resultSet.getString("folder"));
    //                    builder.setSize(resultSet.getLong("size"));
    //                    builder.setFileCount(resultSet.getLong("file_count"));
    //                    builder.setLastUpdate(new Date(resultSet.getLong("last_update")));
    //                }
    //            }
    //            try (PreparedStatement prepareStatement = connection.prepareStatement("SELECT folder, COUNT(*) AS dir_count FROM file_info WHERE file_type=? AND is_deleted=FALSE GROUP BY folder")) {
    //                prepareStatement.setString(1, FileType.DIRECTORY.name());
    //                ResultSet resultSet = prepareStatement.executeQuery();
    //                while (resultSet.next()) {
    //                    FolderStats.Builder builder = func.apply(resultSet.getString("folder"));
    //                    builder.setDirCount(resultSet.getLong("dir_count"));
    //                }
    //            }
    //            list = Lists.newArrayList(Iterables.transform(map.values(), new Function<FolderStats.Builder, FolderStats>() {
    //                @Override
    //                public FolderStats apply(FolderStats.Builder builder) {
    //                    return builder.build();
    //                }
    //            }));
    //            for (FolderStats folderStats : list) {
    //                updateFolderStats(connection, folderStats);
    //            }
    //        } catch (SQLException ex) {
    //            throw new RuntimeException(ex);
    //        }
    //        logger.info("updateFolderStats END");
    //        eventBus.post(new FolderStatsUpdatedEvent() {
    //            @Override
    //            public List<FolderStats> getFolderStats() {
    //                return Collections.unmodifiableList(list);
    //            }
    //        });
    //    }
    @Throws(SQLException::class)
    private fun updateFolderStats(connection: Connection, folderStats: FolderStats) {
        assert(folderStats.fileCount >= 0)
        assert(folderStats.dirCount >= 0)
        assert(folderStats.size >= 0)
        connection.prepareStatement("MERGE INTO folder_stats"
                + " (folder,file_count,dir_count,size,last_update)"
                + " VALUES (?,?,?,?,?)").use { prepareStatement ->
            prepareStatement.setString(1, folderStats.folderId)
            prepareStatement.setLong(2, folderStats.fileCount)
            prepareStatement.setLong(3, folderStats.dirCount)
            prepareStatement.setLong(4, folderStats.size)
            prepareStatement.setLong(5, folderStats.lastUpdate.time)
            prepareStatement.executeUpdate()
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

    //SEQUENCER
    private inner class IndexRepoSequencer : Sequencer {

        private var indexId: Long? = null
        private var currentSequence: Long? = null

        @Throws(SQLException::class)
        @Synchronized private fun loadFromDb() {
            getConnection().use { connection ->
                connection.prepareStatement("SELECT index_id,current_sequence FROM index_sequence").use { statement ->
                    val resultSet = statement.executeQuery()
                    assert(resultSet.first())
                    indexId = resultSet.getLong("index_id")
                    currentSequence = resultSet.getLong("current_sequence")
                    logger.info("loaded index info from db, index_id = {}, current_sequence = {}", indexId, currentSequence)
                }
            }
        }

        @Synchronized override fun indexId(): Long {
            if (indexId == null) {
                loadFromDb()
            }
            return indexId!!
        }

        @Throws(SQLException::class)
        @Synchronized override fun nextSequence(): Long {
            val nextSequence = currentSequence() + 1
            getConnection().use { connection ->
                connection.prepareStatement("UPDATE index_sequence SET current_sequence=?").use { statement ->
                    statement.setLong(1, nextSequence)
                    assert(statement.executeUpdate() == 1)
                    logger.debug("update local index sequence to {}", nextSequence)
                }
            }

            currentSequence = nextSequence
            return nextSequence
        }

        @Synchronized override fun currentSequence(): Long {
            if (currentSequence == null) {
                loadFromDb()
            }
            return currentSequence!!
        }
    }

    @Throws(SQLException::class)
    private fun readDeviceAddress(resultSet: ResultSet): DeviceAddress {
        val instanceId = resultSet.getLong("instance_id")
        return DeviceAddress.Builder()
                .setAddress(resultSet.getString("address_url"))
                .setDeviceId(DeviceId(resultSet.getString("device_id")))
                .setInstanceId(if (instanceId == 0L) null else instanceId)
                .setProducer(DeviceAddress.AddressProducer.valueOf(resultSet.getString("address_producer")))
                .setScore(resultSet.getInt("address_score"))
                .setLastModified(Date(resultSet.getLong("last_modified")))
                .build()
    }

    companion object {
        private const val VERSION = 13
    }
}
