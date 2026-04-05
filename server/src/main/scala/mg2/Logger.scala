package mg2

import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter

/**
 * Minimalist logger with ISO timestamps for Scala
 * Wraps println with timestamp prefixes
 * Follows AGENTS.md: strict typing, zero dependencies, minimal code
 */
object Logger {
  private val formatter = DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.systemDefault())

  private def getTimestamp(): String = {
    formatter.format(Instant.now())
  }

  def debug(msg: String): Unit = {
    println(s"[${getTimestamp()}] $msg")
  }

  def info(msg: String): Unit = {
    println(s"[${getTimestamp()}] $msg")
  }

  def warn(msg: String): Unit = {
    println(s"[${getTimestamp()}] $msg")
  }

  def error(msg: String): Unit = {
    println(s"[${getTimestamp()}] $msg")
  }
}
