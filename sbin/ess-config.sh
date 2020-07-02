#!/usr/bin/env bash

# included in all the ess scripts with source command
# should not be executable directly
# also should not be passed any arguments, since we need original $*

# symlink and absolute path should rely on ESS_HOME to resolve
if [ -z "${ESS_HOME}" ]; then
  export ESS_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

export ESS_CONF_DIR="${ESS_CONF_DIR:-"${ESS_HOME}/conf"}"

if [ -z "$ESS_ENV_LOADED" ]; then
  export ESS_ENV_LOADED=1

  if [ -f "${ESS_CONF_DIR}/ess-env.sh" ]; then
    # Promote all variable declarations to environment (exported) variables
    set -a
    . "${ESS_CONF_DIR}/ess-env.sh"
    set +a
  fi
fi