uniform.project("piped", "au.com.cba.omnia.piped")

uniformDependencySettings

libraryDependencies :=
  depend.scaldingproject() ++
  depend.scalaz() ++
  Seq(
    "au.com.cba.omnia" %% "omnitool-core"             % "1.1.0-20140604045346-81d13eb",
    "net.rforge"        % "REngine"                   % "1.8.0",
    "net.rforge"        % "Rserve"                    % "1.8.0",
    "com.twitter"      %% "algebird-test"             % depend.versions.algebird % "test",
    "org.scalaz"       %% "scalaz-scalacheck-binding" % depend.versions.scalaz   % "test")

uniformAssemblySettings
