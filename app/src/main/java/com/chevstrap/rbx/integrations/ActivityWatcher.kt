package com.chevstrap.rbx.integrations

import com.chevstrap.rbx.App
import com.chevstrap.rbx.CustomWatcher
import com.chevstrap.rbx.appDirectories.RobloxClientData
import com.chevstrap.rbx.enums.ServerType
import com.chevstrap.rbx.models.entities.ActivityData
import com.chevstrap.rbx.ui.NotifyIconWrapper
import org.json.JSONObject
import java.io.File
import java.io.IOException
import java.io.RandomAccessFile
import java.util.Date
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import kotlin.concurrent.Volatile

class ActivityWatcher {

    private val executor: ExecutorService = Executors.newSingleThreadExecutor()
    val history: MutableList<ActivityData> = ArrayList()
    private var discordRPC: RobloxDiscordRPC? = null

    @Volatile var isStopMonitoring = false
        private set
    @Volatile var isInExperience = false
        private set
    @Volatile var isEnableRPC = false
        private set
    @Volatile @JvmField var data: ActivityData? = null

    private fun start() {
        val tag = "ActivityWatcher::start"
        data = ActivityData("")
        val execPath = RobloxClientData().executablePath
        if (execPath.isNullOrEmpty()) {
            App.logger.writeLine(tag, "Executable path is null or empty")
            App.isLastLogFoundOrMaybeNot = true
            return
        }

        isEnableRPC = App.config.data.showGameActivity
        discordRPC = CustomWatcher.getInstance().robloxDiscordRPC
        App.logger.writeLine(tag, "isEnableRPC=$isEnableRPC, discordRPC=${discordRPC != null}")

        val logLocation = File(execPath, "logs")
        if (!logLocation.exists() && !logLocation.mkdirs()) {
            App.logger.writeLine(tag, "Failed to create log directory: ${logLocation.absolutePath}")
            App.isLastLogFoundOrMaybeNot = true
            return
        }
        App.logger.writeLine(tag, "Watching logs from ${logLocation.absolutePath}")

        var currentLogPath: String? = null
        var lastReadPosition = 0L
        val partialLineBuffer = StringBuilder()
        var lastHeartbeat = System.currentTimeMillis()
        var lastProgressTime = System.currentTimeMillis()
        val heartbeatIntervalMs = 15_000L

        try {
            App.isLastLogFoundOrMaybeNot = true

            val initial = getLatestLog(logLocation)
            if (initial != null) {
                currentLogPath = initial.absolutePath
                lastReadPosition = initial.length()
                App.logger.writeLine(tag, "Starting from EOF: ${initial.name} ($lastReadPosition bytes)")
            } else {
                App.logger.writeLine(tag, "No existing Roblox log found. Waiting for a new log...")
            }

            while (!isStopMonitoring) {
                try {
                    val latestLog = getLatestLog(logLocation)
                    if (latestLog == null) {
                        Thread.sleep(500)
                        maybeHeartbeat(tag, currentLogPath, lastReadPosition, -1L, 0, lastHeartbeat, heartbeatIntervalMs)?.let { lastHeartbeat = it }
                        continue
                    }

                    if (currentLogPath != latestLog.absolutePath) {
                        App.logger.writeLine(tag, "Detected new Roblox log: ${latestLog.name}")
                        currentLogPath = latestLog.absolutePath
                        lastReadPosition = 0
                        partialLineBuffer.clear()
                        isInExperience = false
                        data = ActivityData("")
                        lastProgressTime = System.currentTimeMillis()
                        App.logger.writeLine(tag, "Session state reset for new log")
                    }

                    // ★ ALWAYS try to read from a fresh handle. Never gate on File.length()
                    //   because DualSpace's overlay FS starts returning 0 for stat() mid-session
                    //   even though the underlying data remains readable via a fresh FD.
                    var rafLen = -1L
                    var bytesRead = 0
                    val buffer = ByteArray(MAX_CHUNK_SIZE.toInt())

                    try {
                        RandomAccessFile(latestLog, "r").use { raf ->
                            rafLen = raf.length()

                            // Only treat as truncation if rafLen is strictly between
                            // 1 and pos-1. rafLen == 0 is the DualSpace "lying" case
                            // and we still attempt a read.
                            if (rafLen in 1 until lastReadPosition) {
                                App.logger.writeLine(tag, "Truncation detected ($lastReadPosition -> $rafLen), resetting")
                                lastReadPosition = 0
                                partialLineBuffer.clear()
                                isInExperience = false
                                data = ActivityData("")
                            }

                            raf.seek(lastReadPosition)
                            bytesRead = raf.read(buffer)
                        }
                    } catch (e: IOException) {
                        App.logger.writeLine(tag, "IO exception opening/reading log: ${e.message}")
                        App.logger.writeException(tag, e)
                        Thread.sleep(500)
                        continue
                    }

                    if (bytesRead > 0) {
                        lastReadPosition += bytesRead
                        lastProgressTime = System.currentTimeMillis()

                        val chunk = String(buffer, 0, bytesRead, Charsets.UTF_8)
                        partialLineBuffer.append(chunk)

                        var idx = partialLineBuffer.indexOf("\n")
                        while (idx >= 0) {
                            val line = partialLineBuffer.substring(0, idx).trimEnd('\r')
                            partialLineBuffer.delete(0, idx + 1)
                            if (line.isNotEmpty()) handleLogEntry(line)
                            idx = partialLineBuffer.indexOf("\n")
                        }

                        if (partialLineBuffer.length > MAX_PARTIAL_LINE) {
                            App.logger.writeLine(tag, "Partial line buffer exceeded ${MAX_PARTIAL_LINE}, discarding")
                            partialLineBuffer.clear()
                        }
                    } else {
                        // No data. Detect "stuck at pos with rafLen=0" pattern.
                        val now = System.currentTimeMillis()
                        if (lastReadPosition > 0 && rafLen <= 0 &&
                            (now - lastProgressTime) > STUCK_THRESHOLD_MS) {
                            App.logger.writeLine(
                                tag,
                                "Stuck for >${STUCK_THRESHOLD_MS}ms (rafLen=$rafLen, pos=$lastReadPosition), resetting to 0"
                            )
                            lastReadPosition = 0
                            partialLineBuffer.clear()
                            isInExperience = false
                            data = ActivityData("")
                            lastProgressTime = now
                        }
                        Thread.sleep(500)
                        maybeHeartbeat(tag, currentLogPath, lastReadPosition, rafLen, bytesRead, lastHeartbeat, heartbeatIntervalMs)?.let { lastHeartbeat = it }
                    }

                } catch (e: IOException) {
                    App.logger.writeLine(tag, "IO error during watch")
                    App.logger.writeException(tag, e)
                    Thread.sleep(1000)
                } catch (e: Exception) {
                    App.logger.writeLine(tag, "Unexpected exception in watch loop")
                    App.logger.writeException(tag, e)
                    Thread.sleep(1000)
                }
            }
        } catch (e: InterruptedException) {
            if (!isStopMonitoring) {
                App.logger.writeLine(tag, "Activity watcher interrupted unexpectedly")
                App.logger.writeException(tag, e)
            } else {
                App.logger.writeLine(tag, "Activity watcher stopped")
            }
            Thread.currentThread().interrupt()
        } catch (e: Exception) {
            App.logger.writeLine(tag, "Unexpected error in activity watcher!")
            App.logger.writeException(tag, e)
        } finally {
            App.isLastLogFoundOrMaybeNot = true
        }
    }

    private fun maybeHeartbeat(
        tag: String,
        currentLogPath: String?,
        lastReadPosition: Long,
        rafLen: Long,
        lastBytesRead: Int,
        lastHeartbeat: Long,
        intervalMs: Long
    ): Long? {
        val now = System.currentTimeMillis()
        if (now - lastHeartbeat < intervalMs) return null
        val name = currentLogPath?.substringAfterLast('/') ?: "<none>"
        val fileLen = currentLogPath?.let {
            try { File(it).length() } catch (_: Exception) { -1L }
        } ?: -1L
        App.logger.writeLine(
            tag,
            "Heartbeat: file=$name, fileLen=$fileLen, rafLen=$rafLen, pos=$lastReadPosition, lastRead=$lastBytesRead"
        )
        return now
    }

    private fun getLatestLog(logLocation: File): File? {
        return try {
            logLocation.listFiles()
                ?.filter { it.isFile && it.name.endsWith(".log") }
                ?.maxByOrNull { it.lastModified() }
        } catch (e: Exception) {
            App.logger.writeException("ActivityWatcher::getLatestLog", e)
            null
        }
    }

    private fun updateServerType(activityData: ActivityData) {
        val rootPlaceId = activityData.rootPlaceId
        val placeId = activityData.placeId
        activityData.serverType = when {
            rootPlaceId == 0L || placeId == 0L || rootPlaceId == placeId -> ServerType.PUBLIC
            else -> ServerType.RESERVED
        }
        App.logger.writeLine(
            "ActivityWatcher::updateServerType",
            "Server type updated: rootPlaceId=$rootPlaceId, placeId=$placeId, serverType=${activityData.serverType}"
        )
    }

    @Synchronized
    private fun finishActiveActivity() {
        val activityData = data ?: return
        if (!isInExperience || activityData.placeId == 0L) {
            App.logger.writeLine(
                "ActivityWatcher::finishActiveActivity",
                "Skipped: isInExperience=$isInExperience, placeId=${activityData.placeId}"
            )
            return
        }

        App.logger.writeLine(
            "ActivityWatcher::finishActiveActivity",
            "Finishing activity for placeId=${activityData.placeId}"
        )

        activityData.timeLeft = Date()
        updateServerType(activityData)
        addToHistory(activityData)
        isInExperience = false
        data = ActivityData("")

        App.appContext?.let {
            NotifyIconWrapper.hideConnectionNotification(it)
            if (isEnableRPC) NotifyIconWrapper.hideDiscordRpcVisibilityNotification(it)
        }

        if (isEnableRPC) {
            App.logger.writeLine("ActivityWatcher::finishActiveActivity", "Calling setCurrentGame to clear RPC")
            discordRPC?.setCurrentGame()
        }
    }

    private fun addToHistory(activityData: ActivityData) {
        val previous = history.lastOrNull()
        if (
            previous != null &&
            previous.universeId == activityData.universeId &&
            (previous.serverType == ServerType.RESERVED || activityData.serverType == ServerType.RESERVED)
        ) {
            previous.timeJoined = listOfNotNull(previous.timeJoined, activityData.timeJoined).minOrNull()
            previous.timeLeft = listOfNotNull(previous.timeLeft, activityData.timeLeft).maxOrNull()
            return
        }
        history.add(activityData)
    }

    private fun handleLogEntry(line: String) {
        val tag = "ActivityWatcher::handleLogEntry"
        val joinServer = "[FLog::Output] ! Joining game"
        val leaveServer = "[FLog::Network] Time to disconnect replication data"
        val connection = "Info [DFLog::NetworkClient] Connection accepted from"
        val leavingRoblox = "[FLog::SingleSurfaceApp] setStage: (stage:None)"
        val universe = "[FLog::GameJoinLoadTime] Report game_join_loadtime:"
        val bsRPC = "[BloxstrapRPC]"

        if (!line.contains(joinServer) && !line.contains(leaveServer) &&
            !line.contains(connection) && !line.contains(bsRPC) &&
            !line.contains(leavingRoblox) && !line.contains(universe)) return

        if (line.contains(joinServer) && isInExperience) {
            App.logger.writeLine(tag, "Join seen while in experience; closing previous session first")
            finishActiveActivity()
        }

        val activityData = data ?: return

        try {
            if (line.contains(leavingRoblox)) {
                App.logger.writeLine(tag, "Detected leavingRoblox line")
                finishActiveActivity()
                return
            }

            if (isInExperience && activityData.placeId != 0L && line.contains(leaveServer)) {
                App.logger.writeLine(tag, "Detected leaveServer line")
                finishActiveActivity()
                return
            }

            if (!isInExperience && activityData.placeId == 0L && line.contains(joinServer)) {
                try {
                    val match = Regex("""Joining game '([^']+)' place (\d+) at (.+)""").find(line)
                    if (match != null) {
                        val jobId = match.groupValues[1].trim()
                        val placeId = match.groupValues[2].trim().toLongOrNull()
                        val serverIP = match.groupValues[3].trim()

                        if (jobId.isNotEmpty() && placeId != null) {
                            App.logger.writeLine(tag, "Joining game: placeId=$placeId, jobId=$jobId")
                            isInExperience = true
                            activityData.timeJoined = Date()
                            activityData.placeId = placeId
                            activityData.jobId = jobId
                            updateServerType(activityData)

                            if (isEnableRPC) discordRPC?.setCurrentGame()
                        } else {
                            App.logger.writeLine(tag, "Failed to parse join (jobId=$jobId, placeId=$placeId): $line")
                        }
                    } else {
                        App.logger.writeLine(tag, "Join regex did not match: $line")
                    }
                } catch (e: Exception) {
                    App.logger.writeLine(tag, "Exception during join parse")
                    App.logger.writeException(tag, e)
                }
                return
            }

            if (isInExperience && activityData.placeId != 0L && line.contains(connection)) {
                try {
                    val match = Regex("""Connection accepted from\s+(.+)$""").find(line)
                    if (match != null) {
                        val location = match.groupValues[1].trim().substringBefore("|").trim()
                        if (location.isNotEmpty()) {
                            activityData.machineAddress = location
                            if (activityData.universeId.toULong() != 0UL) {
                                App.appContext?.let {
                                    NotifyIconWrapper.showConnectionNotification(it, activityData, location, activityData.universeId.toString())
                                    if (isEnableRPC) NotifyIconWrapper.showDiscordRpcVisibilityNotification(it, discordRPC?.isVisibleState() != null)
                                }
                            }
                        }
                    }
                } catch (e: Exception) {
                    App.logger.writeException(tag, e)
                }
                return
            }

            if (isInExperience && activityData.placeId != 0L && line.contains(universe)) {
                try {
                    val userIndex = line.indexOf("userid:")
                    if (userIndex != -1) {
                        val userStart = userIndex + "userid:".length
                        val userEnd = line.indexOf(",", userStart).takeIf { it != -1 } ?: line.length
                        line.substring(userStart, userEnd).trim().toLongOrNull()?.let { activityData.userId = it }
                    }
                    val universeIndex = line.indexOf("universeid:")
                    if (universeIndex != -1) {
                        val universeStart = universeIndex + "universeid:".length
                        val universeEnd = line.indexOf(",", universeStart).takeIf { it != -1 } ?: line.length
                        line.substring(universeStart, universeEnd).trim().toLongOrNull()?.let { universeId ->
                            App.logger.writeLine(tag, "Parsed universeId=$universeId, fetching details")
                            activityData.setUniverseId(universeId) { success ->
                                if (success && isInExperience && data === activityData) {
                                    updateServerType(activityData)
                                    App.logger.writeLine(tag, "Universe details loaded, refreshing RPC")
                                    if (isEnableRPC) discordRPC?.setCurrentGame()
                                }
                            }
                        }
                    }
                } catch (e: Exception) {
                    App.logger.writeException(tag, e)
                }
                return
            }

            if (line.contains(bsRPC)) {
                if (!isInExperience) return
                if (!App.config.data.discordAllowCustomPresence) return

                val jsonStart = line.indexOf("{", line.indexOf(bsRPC))
                if (jsonStart == -1) {
                    App.logger.writeLine(tag, "Malformed BloxstrapRPC line: $line")
                    return
                }
                val messagePlain = line.substring(jsonStart).trim()
                try {
                    if (isEnableRPC) discordRPC?.processRPCMessage(JSONObject(messagePlain))
                } catch (e: Exception) {
                    App.logger.writeException(tag, e)
                }
            }
        } catch (e: Exception) {
            App.logger.writeException(tag, e)
        }
    }

    internal fun isInExperienceState(): Boolean = isInExperience
    internal fun getActivityData(): ActivityData? = data

    fun dispose() {
        val tag = "ActivityWatcher::dispose"
        if (isStopMonitoring) return
        finishActiveActivity()
        isStopMonitoring = true
        App.logger.writeLine(tag, "Disposing activity watcher session")
        try {
            discordRPC?.dispose()
        } catch (e: Exception) {
            App.logger.writeException(tag, e)
        }
        App.appContext?.let {
            NotifyIconWrapper.hideConnectionNotification(it)
            if (isEnableRPC) NotifyIconWrapper.hideDiscordRpcVisibilityNotification(it)
        }
        if (!executor.isShutdown) executor.shutdownNow()
    }

    fun runWatcher() {
        val tag = "ActivityWatcher::runWatcher"
        if (isStopMonitoring) {
            App.logger.writeLine(tag, "Cannot start: already disposed")
            return
        }
        if (executor.isShutdown) {
            App.logger.writeLine(tag, "Cannot start: executor shut down")
            return
        }
        App.logger.writeLine(tag, "Submitting watch task to executor")
        try {
            executor.submit { start() }
        } catch (e: Exception) {
            App.logger.writeLine(tag, "Failed to submit task")
            App.logger.writeException(tag, e)
        }
    }

    companion object {
        private const val MAX_CHUNK_SIZE = 64L * 1024L
        private const val MAX_PARTIAL_LINE = 2 * 1024 * 1024
        private const val STUCK_THRESHOLD_MS = 60_000L
    }
}