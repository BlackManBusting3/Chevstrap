package com.chevstrap.rbx

import com.chevstrap.rbx.integrations.ActivityWatcher
import com.chevstrap.rbx.integrations.RobloxDiscordRPC
import com.chevstrap.rbx.models.entities.ActivityData

class CustomWatcher private constructor() {

    @JvmField
    var activityData: ActivityData? = null

    var activityWatcher: ActivityWatcher? = null
        private set

    var robloxDiscordRPC: RobloxDiscordRPC? = null
        private set

    var isDisposed: Boolean = false
        private set

    private var running = false

    // FIX: the old init { createWatcher() } was removed. It created an extra
    // ActivityWatcher + RobloxDiscordRPC (a second Discord gateway session) that
    // run() later dropped without ever disposing.

    /** Stops and releases the current watcher + RPC (safe to call repeatedly). */
    private fun teardownCurrent() {
        val watcher = activityWatcher
        val rpc = robloxDiscordRPC

        activityWatcher = null
        robloxDiscordRPC = null
        running = false

        try {
            if (watcher != null && !watcher.isStopMonitoring) {
                watcher.dispose()
            }
        } catch (ex: Exception) {
            App.logger.writeException(LOG_IDENTIFIER, ex)
        }

        try {
            rpc?.dispose()
        } catch (ex: Exception) {
            App.logger.writeException(LOG_IDENTIFIER, ex)
        }
    }

    @Synchronized
    fun dispose() {
        if (isDisposed) return

        isDisposed = true

        App.logger.writeLine(LOG_IDENTIFIER, "Disposing CustomWatcher")

        teardownCurrent()
        activityData = null

        synchronized(CustomWatcher::class.java) {
            if (instance === this) {
                instance = null
            }
        }
    }

    @Synchronized
    fun run() {
        if (isDisposed) return

        // FIX: always tear down the previous session and start a fresh one.
        // Before, `running` was never reset, so every launch after the first
        // returned early and no new watcher was ever started.
        teardownCurrent()

        try {
            val watcher = ActivityWatcher()
            val rpc = RobloxDiscordRPC(watcher)

            activityWatcher = watcher
            robloxDiscordRPC = rpc

            running = true
            watcher.runWatcher()
        } catch (ex: Exception) {
            running = false
            activityWatcher = null
            robloxDiscordRPC = null
            App.logger.writeException(LOG_IDENTIFIER, ex)
        }
    }

    val allHistoryServer: MutableCollection<ActivityData>
        get() = activityWatcher?.history
            ?.filterNotNull()
            ?.toMutableList()
            ?: mutableListOf()

    companion object {
        private const val LOG_IDENTIFIER = "CustomWatcher"

        private var instance: CustomWatcher? = null

        @JvmStatic
        fun getInstance(): CustomWatcher {
            synchronized(CustomWatcher::class.java) {
                return instance ?: CustomWatcher().also {
                    instance = it
                }
            }
        }
    }
}
