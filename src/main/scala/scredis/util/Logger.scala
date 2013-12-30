package scredis.util

import org.slf4j.{ LoggerFactory, Logger => SLF4JLogger }

class Logger private (logger: SLF4JLogger) {
  
  def this(clazz: Class[_]) = this(LoggerFactory.getLogger(clazz))
  def this(name: String) = this(LoggerFactory.getLogger(name))
  
  private val Null = "null"
  val name: String = logger.getName
  
  @inline
  private def stringify(value: Any): String = if (value == null) {
    Null
  } else {
    value.toString
  }
  
  def trace(message: => Any): Unit = if (logger.isTraceEnabled) {
    logger.trace(stringify(message))
  }
  def trace(throwable: Throwable): Unit = if (logger.isTraceEnabled) {
    logger.trace(null, throwable)
  }
  def trace(message: => Any, throwable: => Throwable): Unit = if (logger.isTraceEnabled) {
    logger.trace(stringify(message), throwable)
  }
  
  def debug(message: => Any): Unit = if (logger.isDebugEnabled) {
    logger.debug(stringify(message))
  }
  def debug(throwable: Throwable): Unit = if (logger.isDebugEnabled) {
    logger.debug(null, throwable)
  }
  def debug(message: => Any, throwable: => Throwable): Unit = if (logger.isDebugEnabled) {
    logger.debug(stringify(message), throwable)
  }
  
  def info(message: => Any): Unit = if (logger.isInfoEnabled) {
    logger.info(stringify(message))
  }
  def info(throwable: Throwable): Unit = if (logger.isInfoEnabled) {
    logger.info(null, throwable)
  }
  def info(message: => Any, throwable: => Throwable): Unit = if (logger.isInfoEnabled) {
    logger.info(stringify(message), throwable)
  }
  
  def warn(message: => Any): Unit = if (logger.isWarnEnabled) {
    logger.warn(stringify(message))
  }
  def warn(throwable: Throwable): Unit = if (logger.isWarnEnabled) {
    logger.warn(null, throwable)
  }
  def warn(message: => Any, throwable: => Throwable): Unit = if (logger.isWarnEnabled) {
    logger.warn(stringify(message), throwable)
  }
  
  def error(message: => Any): Unit = if (logger.isErrorEnabled) {
    logger.error(stringify(message))
  }
  def error(throwable: Throwable): Unit = if (logger.isErrorEnabled) {
    logger.error(null, throwable)
  }
  def error(message: => Any, throwable: => Throwable): Unit = if (logger.isErrorEnabled) {
    logger.error(stringify(message), throwable)
  }
  
}

object Logger {
  def apply(clazz: Class[_]): Logger = new Logger(clazz)
  def apply(name: String): Logger = new Logger(name)
}
