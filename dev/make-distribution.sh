#!/usr/bin/env bash

set -o pipefail
set -e
set -x

# Figure out where the ESS framework is installed
ESS_HOME="$(cd "`dirname "$0"`/.."; pwd)"
DISTDIR="$ESS_HOME/dist"

MVN="mvn"

if [ -z "$JAVA_HOME" ]; then
  # Fall back on JAVA_HOME from rpm, if found
  if [ $(command -v  rpm) ]; then
    RPM_JAVA_HOME="$(rpm -E %java_home 2>/dev/null)"
    if [ "$RPM_JAVA_HOME" != "%java_home" ]; then
      JAVA_HOME="$RPM_JAVA_HOME"
      echo "No JAVA_HOME set, proceeding with '$JAVA_HOME' learned from rpm"
    fi
  fi

  if [ -z "$JAVA_HOME" ]; then
    if [ `command -v java` ]; then
      # If java is in /usr/bin/java, we want /usr
      JAVA_HOME="$(dirname $(dirname $(which java)))"
    fi
  fi
fi

if [ -z "$JAVA_HOME" ]; then
  echo "Error: JAVA_HOME is not set, cannot proceed."
  exit -1
fi

if [ ! "$(command -v "$MVN")" ] ; then
    echo -e "Could not locate Maven command: '$MVN'."
    exit -1;
fi

if [ $(command -v git) ]; then
    GITREV=$(git rev-parse --short HEAD 2>/dev/null || :)
    if [ ! -z "$GITREV" ]; then
        GITREVSTRING=" (git revision $GITREV)"
    fi
    unset GITREV
fi

VERSION=$("$MVN" help:evaluate -Dexpression=project.version $@ 2>/dev/null\
    | grep -v "INFO"\
    | grep -v "WARNING"\
    | tail -n 1)
SCALA_VERSION=$("$MVN" help:evaluate -Dexpression=scala.binary.version $@ 2>/dev/null\
    | grep -v "INFO"\
    | grep -v "WARNING"\
    | tail -n 1)
SHUFFLE_MANAGER_DIR=$("$MVN" help:evaluate -Dexpression=ess.shuffle.manager $@ 2>/dev/null\
    | grep -v "INFO"\
    | grep -v "WARNING"\
    | tail -n 1)

echo "ESS version is $VERSION"

NAME="release"

echo "Making ess-$VERSION-bin-$NAME.tgz"

# Build uber fat JAR
cd "$ESS_HOME"

export MAVEN_OPTS="${MAVEN_OPTS:--Xmx2g -XX:ReservedCodeCacheSize=1g}"

# Store the command as an array because $MVN variable might have spaces in it.
# Normal quoting tricks don't work.
# See: http://mywiki.wooledge.org/BashFAQ/050
BUILD_COMMAND=("$MVN" -T 1C clean package -DskipTests $@)

# Actually build the jar
echo -e "\nBuilding with..."
echo -e "\$ ${BUILD_COMMAND[@]}\n"

"${BUILD_COMMAND[@]}"

# Make directories
rm -rf "$DISTDIR"
mkdir -p "$DISTDIR/jars"
mkdir -p "$DISTDIR/spark"
echo "ESS $VERSION$GITREVSTRING built for Hadoop $HADOOP_VERSION" > "$DISTDIR/RELEASE"
echo "Build flags: $@" >> "$DISTDIR/RELEASE"

# Copy jars
cp "$ESS_HOME"/service/target/ess-service-"$VERSION"-shaded.jar "$DISTDIR/jars/"
cp "$ESS_HOME"/${SHUFFLE_MANAGER_DIR}/target/ess-shuffle-manager-"$VERSION"-shaded.jar "$DISTDIR/spark/"

# Copy other things
mkdir "$DISTDIR/conf"
cp "$ESS_HOME"/conf/*.template "$DISTDIR/conf"
cp -r "$ESS_HOME/bin" "$DISTDIR"
cp -r "$ESS_HOME/sbin" "$DISTDIR"

TARDIR_NAME="ess-$VERSION-bin-$NAME"
TARDIR="$ESS_HOME/$TARDIR_NAME"
rm -rf "$TARDIR"
cp -r "$DISTDIR" "$TARDIR"
tar czf "ess-$VERSION-$NAME.tgz" -C "$ESS_HOME" "$TARDIR_NAME"
rm -rf "$TARDIR"



