name := "vu.wdps.group09.EntityVectorExtractor"

version := "1.0"

scalaVersion := "2.11.8"

// Conflict manager. Can be removed when there are no conflicts left which have to be resolved manually / warnings disappear.
conflictManager := ConflictManager.latestRevision

// Surfsara Resolver for WARC stuff
resolvers += "SURFSARA repository" at "http://beehub.nl/surfsara-repo/releases"

libraryDependencies ++= Seq(
  // General stuff
  "xerces" % "xercesImpl" % "2.8.0",
  //Spark Dependencies
  "org.apache.spark" %% "spark-core" % "2.0.2" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.0.2" % "provided",
  //NLP Dependencies
  "edu.stanford.nlp" % "stanford-corenlp" % "3.4.1",
  //Language detection
  "com.optimaize.languagedetector" % "language-detector" % "0.6",
  //Misc
  "com.google.protobuf" % "protobuf-java" % "2.6.1",
  "org.jwat" % "jwat-warc" % "1.0.1",
  "SURFsara" % "warcutils" % "1.2",
  "com.syncthemall" % "boilerpipe" % "1.2.2",
  "com.sksamuel.elastic4s" %% "elastic4s-core" % "2.4.0",
  "com.github.scala-incubator.io" %% "scala-io-core" % "0.4.3",
  "com.github.scala-incubator.io" %% "scala-io-file" % "0.4.3",
  "org.apache.jena" % "jena-arq" % "3.1.1",
  "com.hp.hpl.jena" % "arq" % "2.8.8"
)

assemblyShadeRules in assembly :=Seq(
  ShadeRule.rename("com.google.**" -> "googlecommona.@1").inAll
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}