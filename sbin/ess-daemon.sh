#!/usr/bin/env bash

# Runs a ESS command as a daemon.
#
# Environment Variables
#
#   ESS_CONF_DIR  Alternate conf dir. Default is ${ESS_HOME}/conf.
#   ESS_LOG_DIR   Where log files are stored. ${ESS_HOME}/logs by default.
#   ESS_PID_DIR   The pid files are stored. /tmp by default.
#   ESS_IDENT_STRING   A string representing this instance of ess. $USER by default
#   ESS_NICENESS The scheduling priority for daemons. Defaults to 0.
#   ESS_NO_DAEMONIZE   If set, will run the proposed command in the foreground. It will not output a PID file.
##

usage="Usage: ess-daemon.sh [--config <conf-dir>] (start|stop|status) <ess-command> <ess-instance-number> <args...>"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

if [ -z "${ESS_HOME}" ]; then
  export ESS_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

. "${ESS_HOME}/sbin/ess-config.sh"

# get arguments

# Check if --config is passed as an argument. It is an optional parameter.
# Exit if the argument is not a directory.

if [ "$1" == "--config" ]
then
  shift
  conf_dir="$1"
  if [ ! -d "$conf_dir" ]
  then
    echo "ERROR : $conf_dir is not a directory"
    echo $usage
    exit 1
  else
    export ESS_CONF_DIR="$conf_dir"
  fi
  shift
fi

option=$1
shift
command=$1
shift
instance=$1
shift

ess_rotate_log ()
{
    log=$1;
    num=5;
    if [ -n "$2" ]; then
	num=$2
    fi
    if [ -f "$log" ]; then # rotate logs
	while [ $num -gt 1 ]; do
	    prev=`expr $num - 1`
	    [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
	    num=$prev
	done
	mv "$log" "$log.$num";
    fi
}

if [ "$ESS_IDENT_STRING" = "" ]; then
  export ESS_IDENT_STRING="$USER"
fi


export ESS_PRINT_LAUNCH_COMMAND="1"

# get log directory
if [ "$ESS_LOG_DIR" = "" ]; then
  export ESS_LOG_DIR="${ESS_HOME}/logs"
fi
mkdir -p "$ESS_LOG_DIR"
touch "$ESS_LOG_DIR"/.ess_test > /dev/null 2>&1
TEST_LOG_DIR=$?
if [ "${TEST_LOG_DIR}" = "0" ]; then
  rm -f "$ESS_LOG_DIR"/.ess_test
else
  chown "$ESS_IDENT_STRING" "$ESS_LOG_DIR"
fi

if [ "$ESS_PID_DIR" = "" ]; then
  ESS_PID_DIR=/tmp
fi

# some variables
log="$ESS_LOG_DIR/ess-$ESS_IDENT_STRING-$command-$instance-$HOSTNAME.out"
pid="$ESS_PID_DIR/ess-$ESS_IDENT_STRING-$command-$instance.pid"

# Set default scheduling priority
if [ "$ESS_NICENESS" = "" ]; then
    export ESS_NICENESS=0
fi

execute_command() {
  if [ -z ${ESS_NO_DAEMONIZE+set} ]; then
      nohup -- "$@" >> $log 2>&1 < /dev/null &
      newpid="$!"

      echo "$newpid" > "$pid"

      # Poll for up to 5 seconds for the java process to start
      for i in {1..10}
      do
        if [[ $(ps -p "$newpid" -o comm=) =~ "java" ]] || [[ $(ps -p "$newpid" -o comm=) =~ "jboot" ]]; then
           break
        fi
        sleep 0.5
      done

      sleep 2
      # Check if the process has died; in that case we'll tail the log so the user can see
      if [[ ! $(ps -p "$newpid" -o comm=) =~ "java" ]] && [[ ! $(ps -p "$newpid" -o comm=) =~ "jboot" ]]; then
        echo "failed to launch: $@"
        tail -10 "$log" | sed 's/^/  /'
        echo "full log in $log"
      fi
  else
      "$@"
  fi
}

run_command() {
  mode="$1"
  shift

  mkdir -p "$ESS_PID_DIR"

  if [ -f "$pid" ]; then
    TARGET_ID="$(cat "$pid")"
    if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]] || [[ $(ps -p "$TARGET_ID" -o comm=) =~ "jboot" ]]; then
      echo "$command running as process $TARGET_ID.  Stop it first."
      exit 1
    fi
  fi

  ess_rotate_log "$log"
  echo "starting $command, logging to $log"

  case "$mode" in
    (class)
      execute_command nice -n "$ESS_NICENESS" "${ESS_HOME}"/bin/ess-class "$command" "$@"
      ;;

    (*)
      echo "unknown mode: $mode"
      exit 1
      ;;
  esac

}

case $option in

  (start)
    run_command class "$@"
    ;;

  (stop)

    if [ -f $pid ]; then
      TARGET_ID="$(cat "$pid")"
      if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]] || [[ $(ps -p "$TARGET_ID" -o comm=) =~ "jboot" ]]; then
        echo "stopping $command"
        kill "$TARGET_ID" && rm -f "$pid"
      else
        echo "no $command to stop"
      fi
    else
      echo "no $command to stop"
    fi
    ;;

  (status)

    if [ -f $pid ]; then
      TARGET_ID="$(cat "$pid")"
      if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]] || [[ $(ps -p "$TARGET_ID" -o comm=) =~ "jboot" ]]; then
        echo $command is running.
        exit 0
      else
        echo $pid file is present but $command not running
        exit 1
      fi
    else
      echo $command not running.
      exit 2
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac


