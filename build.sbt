name := "AkkaTransportNetty4"

version := "0.1"

scalaVersion := "2.12.6"

val akkaVersion = "2.5.12"
val nettyVersion = "4.1.24.Final"

libraryDependencies += "com.typesafe.akka" %% "akka-actor"                   % akkaVersion
libraryDependencies += "com.typesafe.akka" %% "akka-remote"                  % akkaVersion   //exclude ("io.netty", "netty")
libraryDependencies += "io.netty"           % "netty-handler"                % nettyVersion
libraryDependencies += "io.netty"           % "netty-common"                 % nettyVersion
libraryDependencies += "io.netty"           % "netty-buffer"                 % nettyVersion
libraryDependencies += "io.netty"           % "netty-codec"                  % nettyVersion
libraryDependencies += "io.netty"           % "netty-transport-native-epoll" % nettyVersion
libraryDependencies += "io.netty"           % "netty-resolver"               % nettyVersion



