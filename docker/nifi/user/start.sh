#!/bin/sh -e

scripts_dir="/opt/nifi/scripts"

[ -f "${scripts_dir}/common.sh" ] && . "${scripts_dir}/common.sh"

prop_replace "nifi.flow.configuration.file" "${NIFI_HOME}/user/conf/flow.xml.gz"
echo "nifi.nar.library.directory.custom=${NIFI_HOME}/user/nars" >> "${NIFI_HOME}/conf/nifi.properties"


"${scripts_dir}/start.sh"
