package net.syncthing.lite.dialogs

import android.app.AlertDialog
import android.app.Dialog
import android.content.ClipData
import android.content.ClipboardManager
import android.content.Context
import android.content.Intent
import android.graphics.Bitmap
import android.graphics.Color
import android.os.Bundle
import android.support.v4.app.FragmentManager
import android.util.Log
import android.view.LayoutInflater
import android.view.View
import android.widget.Toast
import com.google.zxing.BarcodeFormat
import com.google.zxing.WriterException
import com.google.zxing.qrcode.QRCodeWriter
import kotlinx.coroutines.experimental.android.UI
import kotlinx.coroutines.experimental.async
import net.syncthing.lite.R
import net.syncthing.lite.databinding.DialogDeviceIdBinding
import net.syncthing.lite.fragments.SyncthingDialogFragment
import org.jetbrains.anko.doAsync

class DeviceIdDialogFragment: SyncthingDialogFragment() {
    companion object {
        private const val QR_RESOLUTION = 512
        private const val TAG = "DeviceIdDialog"
    }

    override fun onCreateDialog(savedInstanceState: Bundle?): Dialog {
        val binding = DialogDeviceIdBinding.inflate(LayoutInflater.from(context), null, false)

        // use an placeholder to prevent size changes; this string is never shown
        binding.deviceId.text = "XXXXXXX-XXXXXXX-XXXXXXX-XXXXXXX-XXXXXXX-XXXXXXX-XXXXXXX-XXXXXXX"
        binding.deviceId.visibility = View.INVISIBLE

        binding.qrCode.setImageBitmap(Bitmap.createBitmap(QR_RESOLUTION, QR_RESOLUTION, Bitmap.Config.RGB_565))

        libraryHandler.library { configuration, _, _ ->
            val deviceId = configuration.localDeviceId

            fun copyDeviceId() {
                val clipboard = context!!.getSystemService(Context.CLIPBOARD_SERVICE) as ClipboardManager
                val clip = ClipData.newPlainText(context!!.getString(R.string.device_id), deviceId.deviceId)

                clipboard.primaryClip = clip

                Toast.makeText(context, context!!.getString(R.string.device_id_copied), Toast.LENGTH_SHORT)
                        .show()
            }

            fun shareDeviceId() {
                context!!.startActivity(Intent.createChooser(
                        Intent(Intent.ACTION_SEND).apply {
                            type = "text/plain"
                            putExtra(Intent.EXTRA_TEXT, deviceId.deviceId)
                        },
                        context!!.getString(R.string.share_device_id_chooser)
                ))
            }

            async (UI) {
                binding.deviceId.text = deviceId.deviceId
                binding.deviceId.visibility = View.VISIBLE

                binding.deviceId.setOnClickListener { copyDeviceId() }
                binding.share.setOnClickListener { shareDeviceId() }
            }

            doAsync {
                val writer = QRCodeWriter()
                try {
                    val bitMatrix = writer.encode(deviceId.deviceId, BarcodeFormat.QR_CODE, QR_RESOLUTION, QR_RESOLUTION)
                    val width = bitMatrix.width
                    val height = bitMatrix.height
                    val bmp = Bitmap.createBitmap(width, height, Bitmap.Config.RGB_565)
                    for (x in 0 until width) {
                        for (y in 0 until height) {
                            bmp.setPixel(x, y, if (bitMatrix.get(x, y)) Color.BLACK else Color.WHITE)
                        }
                    }

                    async(UI) {
                        binding.flipper.displayedChild = 1
                        binding.qrCode.setImageBitmap(bmp)
                    }
                } catch (e: WriterException) {
                    Log.w(TAG, e)
                }
            }
        }

        return AlertDialog.Builder(context!!, theme)
                .setTitle(context!!.getString(R.string.device_id))
                .setView(binding.root)
                .setPositiveButton(android.R.string.ok, null)
                .create()
    }

    fun show(manager: FragmentManager?) {
        super.show(manager, TAG)
    }
}
