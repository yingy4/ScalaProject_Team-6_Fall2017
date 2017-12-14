lazy val Team6_Code = project
lazy val Team6_Play = project
lazy val root = (project in file("."))
  .aggregate(Team6_Code, Team6_Play)
  .dependsOn(Team6_Code, Team6_Play)
