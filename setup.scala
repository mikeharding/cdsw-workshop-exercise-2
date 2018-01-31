import sys.process._

//terminal commands
"hadoop fs -mkdir /tmp/BasketballStats"!
val status = Seq("/bin/sh", "-c", "hadoop fs -put ./data/* /tmp/BasketballStats").!

