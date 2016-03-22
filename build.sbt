organization  := "com.mesos.framework"

name :=  "fenzo.framework.docker"

version       := "0.1"

scalaVersion  := "2.10.5"


resolvers ++= Seq(
  "spray repo" at "http://repo.spray.io/",
  "typesafe repo" at "http://repo.typesafe.com/typesafe/releases/"
)


fork in run := true

libraryDependencies ++= {
  val fenzoV = "0.8.6"
  val mesosV = "latest.release"
  val logbackV = "1.1.6"
  val slf4jV = "1.7.18"
  val log4jV = "2.5"
  val softlayerV = "0.2.2"
  val gsonV = "2.6.2"
  Seq(
  	"org.apache.mesos" 	    % "mesos" 				% mesosV,
    "com.netflix.fenzo"     % "fenzo-core"	 	    % fenzoV,
    "com.netflix.fenzo"     % "fenzo-triggers"	    % fenzoV,
    "com.softlayer.api"     % "softlayer-api-client" % softlayerV,
	"org.slf4j"				% "slf4j-api"			% slf4jV,
	"com.google.code.gson"  % "gson"				% gsonV,
	"org.apache.logging.log4j"	% "log4j-api"		% log4jV,
	"org.apache.logging.log4j"	% "log4j-core"		% log4jV,
	"org.apache.logging.log4j"	% "log4j-slf4j-impl"	% log4jV
  )
}

assemblyJarName in assembly := "fenzo-framework.jar"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly := {
case PathList("scala", xs @ _*) => MergeStrategy.discard
case x =>
  val oldStrategy = (assemblyMergeStrategy in assembly).value
  oldStrategy(x)
}


scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")
