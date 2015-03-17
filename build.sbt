//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

uniform.project("piped", "au.com.cba.omnia.piped")

uniformDependencySettings

libraryDependencies :=
  depend.scaldingproject() ++
  Seq(
    "au.com.cba.omnia" %% "omnitool-core"             % "1.7.0-20150316053109-4b4b011",
    "net.rforge"        % "REngine"                   % "1.8.0",
    "net.rforge"        % "Rserve"                    % "1.8.0",
    "com.twitter"      %% "algebird-test"             % depend.versions.algebird % "test"
  )

uniform.docSettings("https://github.com/CommBank/piped")

uniform.ghsettings
