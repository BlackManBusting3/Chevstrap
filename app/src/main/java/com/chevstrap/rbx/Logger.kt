package com.chevstrap.rbx

import chevstrap.extensions.FileTool
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat
import java.util.Collections
import java.util.Date
import java.util.Locale

class Logger private constructor() {
    private val lock = Any()
    private var filestream: FileOutputStream? = null

    val history: MutableList<String> = Collections.synchronizedList(ArrayList())

    var initialized: Boolean = false
        private set

    var noWriteMode: Boolean = false
        private set

    var fileLocation: String? = null
        private set

    private var cleanupDone = false

    fun initializePersistent() {
        if (initialized || noWriteMode) return

        val directory = File(Paths.logs)
        if (!directory.exists() && !directory.mkdirs()) {
            noWriteMode = true
            return
        }

        if (!cleanupDone) {
            cleanupOldLogs(directory)
            cleanupDone = true
        }

        val timestamp = SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'", Locale.US).format(Date())
        val file = File(directory, "Chevstrap_$timestamp.log")

        try {
            filestream = FileOutputStream(file, true)
            initialized = true
            fileLocation = file.absolutePath
            writeLine("Logger::Initialize", "Logger initialized at $fileLocation")

            if (history.isNotEmpty()) {
                val backlog = synchronized(history) { history.joinToString("\r\n") }
                writeToLog(backlog)
            }
        } catch (_: IOException) {
            noWriteMode = true
        }
    }

    private fun cleanupOldLogs(directory: File) {
        val cutoff = System.currentTimeMillis() - (3L * 24L * 60L * 60L * 1000L)

        try {
            val files = FileTool.listFiles(directory)
            for (file in files) {
                if (file.isFile && file.name.endsWith(".log")) {
                    if (file.lastModified() < cutoff) {
                        FileTool.deleteFile(file)
                    }
                }
            }
        } catch (e: IOException) {
            writeException("Logger::Cleanup", e)
        }
    }

    fun writeLine(identifier: String?, message: String?) {
        writeLineInternal("[$identifier] ${message ?: ""}")
    }

    fun writeException(identifier: String?, ex: Exception?) {
        val hResult = "0x" + Integer.toHexString((ex?.hashCode() ?: 0)).uppercase()
        // FIX: include the stack trace so errors are actually debuggable.
        writeLineInternal("[$identifier] ($hResult) ${ex ?: "null"}\n${ex?.stackTraceToString() ?: ""}")
    }

    private fun writeLineInternal(message: String) {
        val timestamp = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.US).format(Date())
        val output = "$timestamp $message"

        // FIX: use logcat instead of println.
        android.util.Log.i("Chevstrap", output)

        // FIX: cap the in-memory history so it can't grow forever.
        synchronized(history) {
            history.add(output)
            if (history.size > MAX_HISTORY) {
                history.subList(0, history.size - MAX_HISTORY).clear()
            }
        }

        writeToLog(output)
    }

    private fun writeToLog(message: String) {
        if (!initialized || filestream == null || noWriteMode) return

        synchronized(lock) {
            try {
                val bytes = "$message\r\n".toByteArray(StandardCharsets.UTF_8)
                filestream?.write(bytes)
                filestream?.flush()
            } catch (_: IOException) {
            }
        }
    }

    companion object {
        private const val MAX_HISTORY = 2000

        @Volatile
        private var instanceDelegate: Logger? = null

        @JvmStatic
        val instance: Logger
            get() {
                return instanceDelegate ?: synchronized(this) {
                    instanceDelegate ?: Logger().also { instanceDelegate = it }
                }
            }
    }
}
