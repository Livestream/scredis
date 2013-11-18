package scredis.util

import org.slf4j.{ LoggerFactory, Logger => SLF4JLogger }

class Logger private (logger: SLF4JLogger) {
  
  def this(clazz: Class[_]) = this(LoggerFactory.getLogger(clazz))
  def this(name: String) = this(LoggerFactory.getLogger(name))
  
  val name: String = logger.getName
  
  def trace(message: => String): Unit = if (logger.isTraceEnabled) {
    logger.trace(message)
  }
  def trace(throwable: Throwable): Unit = if (logger.isTraceEnabled) {
    logger.trace(null, throwable)
  }
  def trace(message: => String, throwable: => Throwable): Unit = if (logger.isTraceEnabled) {
    logger.trace(message, throwable)
  }
  
  def debug(message: => String): Unit = if (logger.isDebugEnabled) {
    logger.debug(message)
  }
  def debug(throwable: Throwable): Unit = if (logger.isDebugEnabled) {
    logger.debug(null, throwable)
  }
  def debug(message: => String, throwable: => Throwable): Unit = if (logger.isDebugEnabled) {
    logger.debug(message, throwable)
  }
  
  def info(message: => String): Unit = if (logger.isInfoEnabled) {
    logger.info(message)
  }
  def info(throwable: Throwable): Unit = if (logger.isInfoEnabled) {
    logger.info(null, throwable)
  }
  def info(message: => String, throwable: => Throwable): Unit = if (logger.isInfoEnabled) {
    logger.info(message, throwable)
  }
  
  def warn(message: => String): Unit = if (logger.isWarnEnabled) {
    logger.warn(message)
  }
  def warn(throwable: Throwable): Unit = if (logger.isWarnEnabled) {
    logger.warn(null, throwable)
  }
  def warn(message: => String, throwable: => Throwable): Unit = if (logger.isWarnEnabled) {
    logger.warn(message, throwable)
  }
  
  def error(message: => String): Unit = if (logger.isErrorEnabled) {
    logger.error(message)
  }
  def error(throwable: Throwable): Unit = if (logger.isErrorEnabled) {
    logger.error(null, throwable)
  }
  def error(message: => String, throwable: => Throwable): Unit = if (logger.isErrorEnabled) {
    logger.error(message, throwable)
  }
  
}

object Logger {
  def apply(clazz: Class[_]): Logger = new Logger(clazz)
  def apply(name: String): Logger = new Logger(name)
}
