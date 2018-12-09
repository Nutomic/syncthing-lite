package net.syncthing.lite.activities

import android.app.AlertDialog
import android.content.res.Configuration
import android.databinding.DataBindingUtil
import android.os.Bundle
import android.support.v4.app.Fragment
import android.support.v7.app.ActionBarDrawerToggle
import android.view.Gravity
import android.view.MenuItem
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import net.syncthing.lite.R
import net.syncthing.lite.databinding.ActivityMainBinding
import net.syncthing.lite.dialogs.DeviceIdDialogFragment
import net.syncthing.lite.fragments.DevicesFragment
import net.syncthing.lite.fragments.FoldersFragment
import net.syncthing.lite.fragments.SettingsFragment
import org.jetbrains.anko.defaultSharedPreferences
import org.jetbrains.anko.intentFor

class MainActivity : SyncthingActivity() {

    companion object {
        const val PREF_IS_FIRST_START = "net.syncthing.lite.activities.MainActivity.IS_FIRST_START"
    }

    private lateinit var binding: ActivityMainBinding
    private var drawerToggle: ActionBarDrawerToggle? = null

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        if (defaultSharedPreferences.getBoolean(PREF_IS_FIRST_START, true)) {
            startActivity(intentFor<IntroActivity>())
            finish()
        }

        binding = DataBindingUtil.setContentView(this, R.layout.activity_main)

        drawerToggle = ActionBarDrawerToggle(
                this, binding.drawerLayout, R.string.app_name, R.string.app_name)
        binding.drawerLayout.addDrawerListener(drawerToggle!!)
        binding.navigation.setNavigationItemSelectedListener( { onNavigationItemSelectedListener(it) })
        supportActionBar!!.setHomeButtonEnabled(true)
        supportActionBar!!.setDisplayHomeAsUpEnabled(true)
    }

    /**
     * Sync the toggle state and fragment after onRestoreInstanceState has occurred.
     */
    override fun onPostCreate(savedInstanceState: Bundle?) {
        super.onPostCreate(savedInstanceState)

        drawerToggle!!.syncState()
        val menu = binding.navigation.menu
        val selection = (0 until menu.size())
                .map { menu.getItem(it) }
                .find { it.isChecked }
                ?: menu.getItem(0)
        onNavigationItemSelectedListener(selection)
    }

    override fun onConfigurationChanged(newConfig: Configuration) {
        super.onConfigurationChanged(newConfig)
        drawerToggle!!.onConfigurationChanged(newConfig)
    }

    override fun onOptionsItemSelected(item: MenuItem): Boolean {
        // Pass the event to ActionBarDrawerToggle, if it returns
        // true, then it has handled the app icon touch event
        return if (drawerToggle!!.onOptionsItemSelected(item)) {
            true
        } else super.onOptionsItemSelected(item)
        // Handle your other action bar items...
    }

    private fun onNavigationItemSelectedListener(menuItem: MenuItem): Boolean {
        when (menuItem.itemId) {
            R.id.folders -> setContentFragment(FoldersFragment())
            R.id.devices -> setContentFragment(DevicesFragment())
            R.id.settings -> setContentFragment(SettingsFragment())
            R.id.device_id -> DeviceIdDialogFragment().show(supportFragmentManager)
            R.id.clear_index -> AlertDialog.Builder(this)
                    .setTitle(getString(R.string.clear_cache_and_index_title))
                    .setMessage(getString(R.string.clear_cache_and_index_body))
                    .setIcon(android.R.drawable.ic_dialog_alert)
                    .setPositiveButton(android.R.string.yes) { _, _ -> cleanCacheAndIndex() }
                    .setNegativeButton(android.R.string.no, null)
                    .show()
        }
        binding.drawerLayout.closeDrawer(Gravity.START)
        return true
    }

    private fun setContentFragment(fragment: Fragment) {
        supportFragmentManager
                .beginTransaction()
                .replace(R.id.content_frame, fragment)
                .commit()
    }

    private fun cleanCacheAndIndex() {
        launch {
            libraryHandler.libraryManager.withLibrary {
                it.syncthingClient.clearCacheAndIndex()
            }
        }
    }
}
