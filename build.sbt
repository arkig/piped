uniform.project("piped", "au.com.cba.omnia.piped")

uniformDependencySettings

libraryDependencies :=
  depend.scaldingproject() ++
  depend.scalaz() ++
  Seq(
    "au.com.cba.omnia" %% "omnitool-core"             % "1.0.0-20140602060732-2073af8",
    "net.rforge"        % "REngine"                   % "1.8.0",
    "net.rforge"        % "Rserve"                    % "1.8.0",
    "com.twitter"      %% "algebird-test"             % depend.versions.algebird % "test",
    "org.scalaz"       %% "scalaz-scalacheck-binding" % depend.versions.scalaz   % "test")

uniformAssemblySettings
