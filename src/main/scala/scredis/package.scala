package object scredis {
  private var poolNumber = 0
  
  private[scredis] def newPoolNumber: Int = synchronized {
    poolNumber += 1
    poolNumber
  }
  
  val DefaultCommandOptions = CommandOptions()
}