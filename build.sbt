import precog.PlatformBuild._

lazy val platform = project.root aggregate (blueeyes, common, yggdrasil, mimir)

/** This used to be the evaluator project.
 */
lazy val mimir = project.setup dependsOn yggdrasil.inBothScopes

lazy val yggdrasil = project.setup dependsOn common.inBothScopes deps (
  blueeyesModule("mongo"),
  "org.objectweb.howl"    % "howl"               %    "1.0.1-1",
  "org.slamdata"          % "jdbm"               %     "3.0.0",
  "commons-primitives"    % "commons-primitives" %      "1.0",
  "org.quartz-scheduler"  % "quartz"             %     "2.2.3",
  "org.spire-math"        % "spire_2.9.2"        %     "0.3.0"
)
lazy val common = project.setup dependsOn blueeyes deps (
  "com.chuusai"              %% "shapeless"        % "1.2.3",
  "org.slf4s"                %% "slf4s-api"        % "1.7.13",
  "org.scalaz"               %% "scalaz-core"      % "7.0.9",
  "org.scalacheck"           %% "scalacheck"       % "1.10.1"  % Test,
  "com.google.code.findbugs"  % "jsr305"           % "3.0.1",
  "joda-time"                 % "joda-time"        % "1.6.2",
  "javolution"                % "javolution"       % "5.5.1",
  "com.google.guava"          % "guava"            % "12.0.1",
  "commons-io"                % "commons-io"       %  "2.5",
  "com.rubiconproject.oss"    % "jchronic"         % "0.2.6",
  "javax.mail"                % "mail"             % "1.4.7",
  "org.fusesource.scalate"    % "scalate-core_2.9" % "1.6.1"
)
lazy val blueeyes = project.setup deps blueeyesModule("core") deps (
  // Neeed explicit versions to avoid version ranges burbling up
  // and choking coursier.
  "org.xsocket"   % "xSocket"   % "2.8.15",
  "org.xlightweb" % "xlightweb" % "2.13.2"
)
