name := "scala-spark-app-chapter08"
version := "1.0"
scalaVersion := "2.11.7"
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.6.2"
//libraryDependencies +=  "org.scalanlp" %% "breeze" % "0.12"
//libraryDependencies +=  "org.scalanlp" %% "breeze-natives" % "0.12"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "1.6.2"
libraryDependencies +="org.jfree" % "jfreechart" % "1.0.14"
libraryDependencies += "com.github.wookietreiber" % "scala-chart_2.11" % "0.5.0"


resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"