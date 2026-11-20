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

    var randomAccessFile: RandomAccessFile? = null
    var currentLogPath: String? = null

    // ★ Heartbeat tracking
    var lastHeartbeat = System.currentTimeMillis()
    val heartbeatIntervalMs = 15_000L
    var consecutiveNullReads = 0

    try {
        App.isLastLogFoundOrMaybeNot = true
        var latestLog = getLatestLog(logLocation)

        if (latestLog != null) {
            currentLogPath = latestLog.absolutePath
            randomAccessFile = RandomAccessFile(latestLog, "r")
            randomAccessFile.seek(randomAccessFile.length())
            App.logger.writeLine(tag, "Starting from EOF: ${latestLog.name} (${randomAccessFile.length()} bytes)")
        } else {
            App.logger.writeLine(tag, "No existing Roblox log found. Waiting for a new log...")
        }

        while (!isStopMonitoring) {
            try {
                latestLog = getLatestLog(logLocation)
                if (latestLog == null) {
                    Thread.sleep(500)
                    maybeHeartbeat(tag, currentLogPath, randomAccessFile, consecutiveNullReads, lastHeartbeat, heartbeatIntervalMs)?.let { lastHeartbeat = it }
                    continue
                }

                if (currentLogPath != latestLog.absolutePath) {
                    App.logger.writeLine(tag, "Detected new Roblox log: ${latestLog.name}")

                    isInExperience = false
                    data = ActivityData("")
                    consecutiveNullReads = 0
                    App.logger.writeLine(tag, "Session state reset for new log")

                    try { randomAccessFile?.close() } catch (_: IOException) {}
                    try {
                        val newLogFile = RandomAccessFile(latestLog, "r")
                        newLogFile.seek(0)
                        randomAccessFile = newLogFile
                        currentLogPath = latestLog.absolutePath
                        App.logger.writeLine(tag, "Now watching: ${latestLog.name}")
                    } catch (e: IOException) {
                        App.logger.writeLine(tag, "Failed to open new Roblox log: ${latestLog.absolutePath}")
                        App.logger.writeException(tag, e)
                        randomAccessFile = null
                        Thread.sleep(500)
                    }
                    continue
                }

                if (randomAccessFile == null) {
                    try {
                        val newLogFile = RandomAccessFile(latestLog, "r")
                        newLogFile.seek(newLogFile.length())
                        randomAccessFile = newLogFile
                        currentLogPath = latestLog.absolutePath
                    } catch (e: IOException) {
                        App.logger.writeLine(tag, "Failed to reopen Roblox log: ${latestLog.absolutePath}")
                        App.logger.writeException(tag, e)
                        Thread.sleep(500)
                        continue
                    }
                }

                if (randomAccessFile.length() < randomAccessFile.filePointer) {
                    App.logger.writeLine(tag, "Log file was truncated. Resetting reader.")
                    randomAccessFile.seek(0)
                }

                var hasReadData = false
                while (!isStopMonitoring) {
                    val line = randomAccessFile.readLine() ?: break
                    hasReadData = true
                    consecutiveNullReads = 0
                    val decodedLine = try {
                        String(line.toByteArray(Charsets.ISO_8859_1), Charsets.UTF_8)
                    } catch (_: Exception) { line }
                    handleLogEntry(decodedLine)
                }
                if (!hasReadData) {
                    consecutiveNullReads++
                    Thread.sleep(500)
                    maybeHeartbeat(tag, currentLogPath, randomAccessFile, consecutiveNullReads, lastHeartbeat, heartbeatIntervalMs)?.let { lastHeartbeat = it }
                }

            } catch (e: IOException) {
                App.logger.writeLine(tag, "IO error during watch, reopening log")
                App.logger.writeException(tag, e)
                try { randomAccessFile?.close() } catch (_: IOException) {}
                randomAccessFile = null
                currentLogPath = null
                Thread.sleep(1000)
            } catch (e: Exception) {
                App.logger.writeLine(tag, "Unexpected exception in watch loop, continuing")
                App.logger.writeException(tag, e)
                Thread.sleep(1000)
            }
        }
    } catch (e: InterruptedException) {
        if (!isStopMonitoring) {
            App.logger.writeLine(tag, "Activity watcher interrupted unexpectedly.")
            App.logger.writeException(tag, e)
        } else {
            App.logger.writeLine(tag, "Activity watcher stopped.")
        }
        Thread.currentThread().interrupt()
    } catch (e: Exception) {
        App.logger.writeLine(tag, "Unexpected error in activity watcher!")
        App.logger.writeException(tag, e)
    } finally {
        App.isLastLogFoundOrMaybeNot = true
        closeRandomAccessFile(randomAccessFile)
    }
}

/**
 * Emits a heartbeat log every [intervalMs] so the Chevstrap log makes it
 * obvious whether the watcher is still alive during periods of inactivity.
 * Returns the new heartbeat timestamp when a beat was emitted, otherwise null.
 */
private fun maybeHeartbeat(
    tag: String,
    currentLogPath: String?,
    raf: RandomAccessFile?,
    consecutiveNullReads: Int,
    lastHeartbeat: Long,
    intervalMs: Long
): Long? {
    val now = System.currentTimeMillis()
    if (now - lastHeartbeat < intervalMs) return null
    val size = try { raf?.length() ?: -1L } catch (_: Exception) { -1L }
    val ptr = try { raf?.filePointer ?: -1L } catch (_: Exception) { -1L }
    val name = currentLogPath?.substringAfterLast('/') ?: "<none>"
    App.logger.writeLine(
        tag,
        "Heartbeat: file=$name, size=$size, ptr=$ptr, nullReads=$consecutiveNullReads"
    )
    return now
}