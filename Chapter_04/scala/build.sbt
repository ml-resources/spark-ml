name := "chapter04"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0"
libraryDependencies += "org.scalanlp" %% "breeze" % "0.12"
// native libraries are not included by default. add this if you want them (as of 0.7)
// native libraries greatly improve performance, but increase jar sizes.
// It also packages various blas implementations, which have licenses that may or may not
// be compatible with the Apache License. No GPL code, as best I know.
libraryDependencies +="org.scalanlp" %% "breeze-natives" % "0.12"
libraryDependencies +="org.jfree" % "jfreechart" % "1.0.14"
libraryDependencies += "com.github.wookietreiber" %% "scala-chart" % "0.3.0"
