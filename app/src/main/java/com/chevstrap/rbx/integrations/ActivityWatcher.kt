package com.chevstrap.rbx.integrations

import com.chevstrap.rbx.App
import com.chevstrap.rbx.CustomWatcher
import com.chevstrap.rbx.appDirectories.RobloxClientData
import com.chevstrap.rbx.enums.ServerType
import com.chevstrap.rbx.models.entities.ActivityData
import com.chevstrap.rbx.ui.NotifyIconWrapper
import org.json.JSONObject
import java.io.File
import java.io.FileInputStream
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

    private val processedLines = LinkedHashSet<String>()
    private var lastProcessedJoinKey: String? = null

    private fun start() {
        val tag = "ActivityWatcher::start"
        data = ActivityData("")
        val execPath = RobloxClientData().executablePath
        if (execPath.isNullOrEmpty()) {
            App.logger.writeLine(tag, "Executable path is null or empty")
            App.isLastLogFoundOrMaybeNot = true
            return
        }
        App.logger.writeLine(tag, "Resolved Roblox executable path: $execPath")

        isEnableRPC = App.config.data.showGameActivity
        discordRPC = CustomWatcher.getInstance().robloxDiscordRPC
        App.logger.writeLine(
            tag,
            "isEnableRPC=$isEnableRPC, allowCustomRPC=${App.config.data.discordAllowCustomPresence}, discordRPC=${discordRPC != null}"
        )

        val logLocation = File(execPath, "logs")
        if (!logLocation.exists() && !logLocation.mkdirs()) {
            App.logger.writeLine(tag, "Failed to create log directory: ${logLocation.absolutePath}")
            App.isLastLogFoundOrMaybeNot = true
            return
        }
        App.logger.writeLine(tag, "Watching logs from ${logLocation.absolutePath}")

        var currentLogPath: String? = null
        var lastReadPosition = 0L
        var emptyReadStreak = 0
        var useStreamFallback = false
        val partialLineBuffer = StringBuilder()
        var lastHeartbeat = System.currentTimeMillis()
        var lastProgressTime = System.currentTimeMillis()
        val heartbeatIntervalMs = 15_000L

        try {
            App.isLastLogFoundOrMaybeNot = true

            val initial = getLatestLog(logLocation)
            if (initial != null) {
                currentLogPath = initial.absolutePath
                val rawLen = try { initial.length() } catch (_: Exception) { 0L }
                lastReadPosition = if (rawLen > INITIAL_REWIND_BYTES) rawLen - INITIAL_REWIND_BYTES else 0L
                App.logger.writeLine(tag, "Starting from ${initial.name} at pos=$lastReadPosition (rawLen=$rawLen)")
            } else {
                App.logger.writeLine(tag, "No existing Roblox log found. Waiting for a new log...")
            }

            while (!isStopMonitoring) {
                try {
                    val latestLog = getLatestLog(logLocation)
                    if (latestLog == null) {
                        App.logger.writeLine(tag, "No Roblox log file found in $logLocation")
                        Thread.sleep(500)
                        maybeHeartbeat(tag, currentLogPath, lastReadPosition, -1L, 0, lastHeartbeat, heartbeatIntervalMs)?.let { lastHeartbeat = it }
                        continue
                    }

                    if (currentLogPath != latestLog.absolutePath) {
                        App.logger.writeLine(tag, "Detected new Roblox log: ${latestLog.name}")
                        if (isInExperience || (data?.placeId ?: 0L) != 0L) {
                            finishActiveActivity(forceClearRpc = true)
                        }
                        currentLogPath = latestLog.absolutePath
                        lastReadPosition = 0L
                        emptyReadStreak = 0
                        useStreamFallback = false
                        processedLines.clear()
                        lastProcessedJoinKey = null
                        partialLineBuffer.clear()
                        isInExperience = false
                        data = ActivityData("")
                        lastProgressTime = System.currentTimeMillis()
                        App.logger.writeLine(tag, "Session state reset for new log")
                    }

                    val buffer = ByteArray(MAX_CHUNK_SIZE.toInt())
                    var rafLen = -1L
                    var bytesRead = -1

                    if (!useStreamFallback) {
                        val result = readFromLogHandle(latestLog, lastReadPosition, buffer) { truncatedTo ->
                            App.logger.writeLine(tag, "Truncation detected ($lastReadPosition -> $truncatedTo), resetting")
                            finishActiveActivity(forceClearRpc = true)
                            lastReadPosition = 0L
                            processedLines.clear()
                            partialLineBuffer.clear()
                            isInExperience = false
                            data = ActivityData("")
                        }
                        rafLen = result.first
                        bytesRead = result.second
                    } else {
                        val streamResult = readFromStreamFromOffset(latestLog, lastReadPosition, buffer)
                        bytesRead = streamResult
                    }

                    if (bytesRead > 0) {
                        lastReadPosition += bytesRead
                        emptyReadStreak = 0
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
                        emptyReadStreak++
                        val now = System.currentTimeMillis()

                        // If RandomAccessFile stops returning new bytes while in experience or after progress,
                        // switch to FileInputStream re-read from start so new lines in DualSpace VFS are still seen.
                        if (!useStreamFallback && emptyReadStreak >= STREAM_FALLBACK_CYCLES && lastReadPosition > 0) {
                            App.logger.writeLine(
                                tag,
                                "Read stalled at pos=$lastReadPosition for $emptyReadStreak cycles; switching to stream fallback from 0"
                            )
                            useStreamFallback = true
                            lastReadPosition = 0L
                            partialLineBuffer.clear()
                            emptyReadStreak = 0
                            lastProgressTime = now
                            continue
                        }

                        if (lastReadPosition > 0 && rafLen <= 0 &&
                            (now - lastProgressTime) > STUCK_THRESHOLD_MS) {
                            App.logger.writeLine(
                                tag,
                                "Stuck for >${STUCK_THRESHOLD_MS}ms (rafLen=$rafLen, pos=$lastReadPosition), resetting to 0"
                            )
                            finishActiveActivity(forceClearRpc = true)
                            lastReadPosition = 0L
                            useStreamFallback = false
                            processedLines.clear()
                            partialLineBuffer.clear()
                            isInExperience = false
                            data = ActivityData("")
                            lastProgressTime = now
                        }
                        Thread.sleep(500)
                        maybeHeartbeat(tag, currentLogPath, lastReadPosition, rafLen, bytesRead, lastHeartbeat, heartbeatIntervalMs)?.let { lastHeartbeat = it }
                    }

                } catch (e: InterruptedException) {
                    throw e
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
            finishActiveActivity(forceClearRpc = true)
            App.isLastLogFoundOrMaybeNot = true
        }
    }

    private fun readFromStreamFromOffset(file: File, offset: Long, buffer: ByteArray): Int {
        return try {
            FileInputStream(file).use { fis ->
                var remaining = offset
                while (remaining > 0) {
                    val skipped = fis.skip(remaining)
                    if (skipped <= 0) break
                    remaining -= skipped
                }
                fis.read(buffer)
            }
        } catch (e: IOException) {
            App.logger.writeLine("ActivityWatcher::readFromStreamFromOffset", "Stream read error: ${e.message}")
            -1
        }
    }

    private inline fun readFromLogHandle(
        file: File,
        pos: Long,
        buffer: ByteArray,
        onTruncated: (Long) -> Unit
    ): Pair<Long, Int> {
        var rafLen = -1L
        var bytesRead = -1
        try {
            RandomAccessFile(file, "r").use { raf ->
                rafLen = raf.length()
                if (rafLen in 1 until pos) {
                    onTruncated(rafLen)
                    raf.seek(0L)
                } else {
                    raf.seek(pos)
                }
                bytesRead = raf.read(buffer)
            }
        } catch (e: IOException) {
            App.logger.writeLine("ActivityWatcher::readFromLogHandle", "IO error reading ${file.name}: ${e.message}")
        }
        return rafLen to bytesRead
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
            "Heartbeat: file=$name, fileLen=$fileLen, rafLen=$rafLen, pos=$lastReadPosition, lastRead=$lastBytesRead, inExp=$isInExperience"
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
    private fun finishActiveActivity(forceClearRpc: Boolean = false) {
        val tag = "ActivityWatcher::finishActiveActivity"
        val activityData = data
        val hadActiveSession = isInExperience || (activityData != null && activityData.placeId != 0L)

        if (!hadActiveSession && !forceClearRpc) {
            App.logger.writeLine(
                tag,
                "Skipped: isInExperience=$isInExperience, placeId=${activityData?.placeId ?: 0L}"
            )
            return
        }

        if (activityData != null && activityData.placeId != 0L) {
            App.logger.writeLine(tag, "Finishing activity for placeId=${activityData.placeId}")
            activityData.timeLeft = Date()
            updateServerType(activityData)
            addToHistory(activityData)
        }

        isInExperience = false
        lastProcessedJoinKey = null
        data = ActivityData("")

        App.appContext?.let {
            NotifyIconWrapper.hideConnectionNotification(it)
            if (isEnableRPC || forceClearRpc) NotifyIconWrapper.hideDiscordRpcVisibilityNotification(it)
        }

        if (isEnableRPC || forceClearRpc) {
            val rpc = discordRPC ?: CustomWatcher.getInstance().robloxDiscordRPC
            App.logger.writeLine(tag, "Clearing RPC (rpcNull=${rpc == null}, force=$forceClearRpc)")
            rpc?.resetSessionState()
            rpc?.setCurrentGame()
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

    private fun isLeaveTrigger(line: String): Boolean {
        return line.contains("Time to disconnect replication data") ||
            line.contains("setStage: (stage:None)") ||
            line.contains("setStage: (stage:Leave)") ||
            line.contains("setStage: (stage:Home)") ||
            line.contains("Disconnected from server") ||
            line.contains("RakNet disconnect") ||
            line.contains("Destroying game") ||
            line.contains("shutdownPlayer")
    }

    private fun isDuplicateLine(line: String): Boolean {
        if (processedLines.contains(line)) return true
        if (processedLines.size >= MAX_PROCESSED_LINES) {
            val iter = processedLines.iterator()
            if (iter.hasNext()) {
                iter.next()
                iter.remove()
            }
        }
        processedLines.add(line)
        return false
    }

    private fun handleLogEntry(line: String) {
        val tag = "ActivityWatcher::handleLogEntry"
        val joinServer = "Joining game '"
        val connection = "Connection accepted from"
        val universe = "Report game_join_loadtime:"
        val bsRPC = "[BloxstrapRPC]"
        val left = isLeaveTrigger(line)

        val matchesAny = line.contains(joinServer) || left ||
            line.contains(connection) || line.contains(bsRPC) || line.contains(universe)
        if (!matchesAny) return
        if (isDuplicateLine(line)) return

        App.logger.writeLine(tag, "Matched line: $line")

        val activityData = data ?: ActivityData("").also { data = it }

        try {
            if (left) {
                if (!isInExperience && activityData.placeId == 0L) return
                App.logger.writeLine(tag, "Leave trigger matched")
                finishActiveActivity(forceClearRpc = true)
                return
            }

            if (line.contains(joinServer)) {
                val match = Regex("""Joining game '([^']+)' place (\d+) at (.+)""").find(line)
                if (match != null) {
                    val jobId = match.groupValues[1].trim()
                    val placeId = match.groupValues[2].trim().toLongOrNull()
                    val serverIP = match.groupValues[3].trim()

                    if (jobId.isNotEmpty() && placeId != null) {
                        val joinKey = "$placeId:$jobId"
                        if (isInExperience && joinKey == lastProcessedJoinKey) {
                            return
                        }
                        if (isInExperience || activityData.placeId != 0L) {
                            App.logger.writeLine(tag, "New join detected while previous session open; closing previous session first")
                            finishActiveActivity(forceClearRpc = true)
                        }

                        val freshData = ActivityData("").also { data = it }
                        lastProcessedJoinKey = joinKey
                        App.logger.writeLine(tag, "Joining game: placeId=$placeId, jobId=$jobId, ip=$serverIP")
                        isInExperience = true
                        freshData.timeJoined = Date()
                        freshData.placeId = placeId
                        freshData.jobId = jobId
                        freshData.machineAddress = serverIP
                        updateServerType(freshData)

                        val rpc = discordRPC ?: CustomWatcher.getInstance().robloxDiscordRPC
                        App.logger.writeLine(tag, "Calling setCurrentGame on join (rpcNull=${rpc == null}, isEnableRPC=$isEnableRPC)")
                        if (isEnableRPC) rpc?.setCurrentGame()
                    } else {
                        App.logger.writeLine(tag, "Failed to parse join values: jobId=$jobId, placeId=$placeId")
                    }
                } else {
                    App.logger.writeLine(tag, "Join regex did not match: $line")
                }
                return
            }

            if (isInExperience && activityData.placeId != 0L && line.contains(connection)) {
                val match = Regex("""Connection accepted from\s+(.+)$""").find(line)
                if (match != null) {
                    val location = match.groupValues[1].trim().substringBefore("|").trim()
                    if (location.isNotEmpty()) {
                        activityData.machineAddress = location
                        if (activityData.universeId.toULong() != 0UL) {
                            App.appContext?.let {
                                NotifyIconWrapper.showConnectionNotification(it, activityData, location, activityData.universeId.toString())
                                if (isEnableRPC) {
                                    val rpc = discordRPC ?: CustomWatcher.getInstance().robloxDiscordRPC
                                    NotifyIconWrapper.showDiscordRpcVisibilityNotification(it, rpc?.isVisibleState() != null)
                                }
                            }
                        }
                    }
                }
                return
            }

            if (isInExperience && activityData.placeId != 0L && line.contains(universe)) {
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
                            if (isInExperience && data === activityData) {
                                updateServerType(activityData)
                                val rpc = discordRPC ?: CustomWatcher.getInstance().robloxDiscordRPC
                                App.logger.writeLine(tag, "Universe details ${if (success) "loaded" else "failed to load"}, refreshing RPC (rpcNull=${rpc == null}, success=$success)")
                                if (isEnableRPC) rpc?.setCurrentGame()
                            }
                        }
                    }
                }
                return
            }

            if (line.contains(bsRPC)) {
                val allowCustom = App.config.data.discordAllowCustomPresence
                App.logger.writeLine(tag, "BloxstrapRPC line seen: isInExperience=$isInExperience, allowCustom=$allowCustom, isEnableRPC=$isEnableRPC")
                if (!allowCustom) return

                if (!isInExperience) {
                    isInExperience = true
                }

                val jsonStart = line.indexOf("{", line.indexOf(bsRPC))
                if (jsonStart == -1) {
                    App.logger.writeLine(tag, "Malformed BloxstrapRPC line (no '{'): $line")
                    return
                }
                val messagePlain = line.substring(jsonStart).trim()
                val rpc = discordRPC ?: CustomWatcher.getInstance().robloxDiscordRPC
                App.logger.writeLine(tag, "Passing BloxstrapRPC payload to rpc (rpcNull=${rpc == null}): $messagePlain")
                if (isEnableRPC) rpc?.processRPCMessage(JSONObject(messagePlain))
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
        App.logger.writeLine(tag, "Disposing activity watcher session")
        finishActiveActivity(forceClearRpc = true)
        isStopMonitoring = true
        try {
            val rpc = discordRPC ?: CustomWatcher.getInstance().robloxDiscordRPC
            rpc?.dispose()
        } catch (e: Exception) {
            App.logger.writeException(tag, e)
        }
        App.appContext?.let {
            NotifyIconWrapper.hideConnectionNotification(it)
            NotifyIconWrapper.hideDiscordRpcVisibilityNotification(it)
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
        private const val INITIAL_REWIND_BYTES = 16_384L
        private const val MAX_CHUNK_SIZE = 64L * 1024L
        private const val MAX_PARTIAL_LINE = 2 * 1024 * 1024
        private const val STUCK_THRESHOLD_MS = 60_000L
        private const val STREAM_FALLBACK_CYCLES = 10
        private const val MAX_PROCESSED_LINES = 2000
    }
}
