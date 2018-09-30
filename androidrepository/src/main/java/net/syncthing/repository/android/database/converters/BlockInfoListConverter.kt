package net.syncthing.repository.android.database.converters

import android.arch.persistence.room.TypeConverter
import com.google.protobuf.ByteString
import net.syncthing.java.bep.BlockExchangeExtraProtos
import net.syncthing.java.bep.BlockExchangeProtos
import net.syncthing.java.core.beans.BlockInfo
import org.bouncycastle.util.encoders.Hex

// the original implementation used this approach too
class BlockInfoListConverter {
    @TypeConverter
    fun toByteArray(blockInfos: List<BlockInfo>) = BlockExchangeExtraProtos.Blocks.newBuilder()
            .addAllBlocks(blockInfos.map { input ->
                BlockExchangeProtos.BlockInfo.newBuilder()
                        .setOffset(input.offset)
                        .setSize(input.size)
                        .setHash(ByteString.copyFrom(Hex.decode(input.hash)))
                        .build()
            }).build().toByteArray()

    @TypeConverter
    fun fromString(data: ByteArray) = BlockExchangeExtraProtos.Blocks.parseFrom(data).blocksList.map { record ->
        BlockInfo(record!!.offset, record.size, Hex.toHexString(record.hash.toByteArray()))
    }
}
