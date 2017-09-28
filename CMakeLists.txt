# -*- coding: utf-8 -*-
# -------------------------------------------------------------------------- #
# CPack-aging instruction file
#
# -------------------------------------------------------------------------- #
# to be used from THIS directory. then do something like this
# (NOTE: SVN_REVISION variable is expected to be exported by build systems)
#
#
#> cmake .
#> make package
#
# NOTE: the following ENV variables are supported:
#
# Variable                 | default value
# -------------------------+-------------------------------------
# HIRO_RPM_VERSION         | n/a
# HIRO_RPM_RELEASE         | n/a
# 
# afterwards you need to cleanup like this
#
#> rm -rf ${tmp_subdir}
#> rm -rf _CPack_Packages
#
# -------------------------------------------------------------------------- #
# FCO 05/2013
# -------------------------------------------------------------------------- #
cmake_minimum_required(VERSION 2.8.10)

set(tmp_subdir "_tmp")
set(rpm_name "cloudwatch-connector")
set(rpm_summary "arago GmbH Connector for CloudWatch")
set(rpm_target_dir "/opt/autopilot/cloudwatch/")
set(rpm_desc "${rpm_summary}")

# set RPM arch:
set(rpm_arch "noarch")

#
# get version variables from environment
# 
if(NOT DEFINED ENV{HIRO_RPM_VERSION})
  message(FATAL_ERROR "env var HIRO_RPM_VERSION not set")
endif()
if(NOT DEFINED ENV{HIRO_RPM_RELEASE})
  message(FATAL_ERROR "env var HIRO_RPM_RELEASE not set")
endif()


message("----------------------------------------")
message("building version: $ENV{HIRO_RPM_VERSION}-$ENV{HIRO_RPM_RELEASE}")
message("for: ${rpm_arch}")
message("----------------------------------------")

# set name/versions
SET(CPACK_PACKAGE_NAME ${rpm_name})
SET(CPACK_PACKAGE_VERSION_MAJOR $ENV{HIRO_RPM_VERSION})
SET(CPACK_PACKAGE_VERSION_MINOR $ENV{HIRO_RPM_RELEASE})
SET(CPACK_PACKAGE_VERSION_PATCH "0")
SET(CPACK_PACKAGE_DESCRIPTION ${rpm_summary})
# and corresponding RPM stuff, too
SET(CPACK_RPM_PACKAGE_URL "https://www.arago.co")
SET(CPACK_RPM_PACKAGE_SUMMARY "${rpm_summary}")
SET(CPACK_RPM_PACKAGE_NAME ${rpm_name})
SET(CPACK_RPM_PACKAGE_VERSION $ENV{HIRO_RPM_VERSION})
SET(CPACK_RPM_PACKAGE_ARCHITECTURE ${rpm_arch})
SET(CPACK_RPM_PACKAGE_RELEASE $ENV{HIRO_RPM_RELEASE})
SET(CPACK_RPM_PACKAGE_LICENSE "Proprietary")
SET(CPACK_RPM_PACKAGE_GROUP "Applications/Other")
SET(CPACK_RPM_PACKAGE_VENDOR "arago GmbH")
SET(CPACK_RPM_PACKAGE_DESCRIPTION "${rpm_desc}")
SET(CPACK_PACKAGE_FILE_NAME "${rpm_name}-$ENV{HIRO_RPM_VERSION}-$ENV{HIRO_RPM_RELEASE}.${rpm_arch}")
message("build ${CPACK_PACKAGE_FILE_NAME}")

SET(CPACK_RPM_PACKAGE_REQUIRES "autopilot-backend-base, autopilot-config")

SET(CPACK_GENERATOR "RPM")
SET(CPACK_RPM_PACKAGE_DEBUG "0")

SET(CPACK_RPM_PACKAGE_RELOCATABLE OFF)
SET(CPACK_INCLUDE_TOPLEVEL_DIRECTORY "0")

# set basedir from current path
# every path specification will be done relative to this
execute_process(COMMAND pwd
			OUTPUT_VARIABLE basedir
			OUTPUT_STRIP_TRAILING_WHITESPACE)
set(builddir "${basedir}/${tmp_subdir}")
file(MAKE_DIRECTORY "${builddir}")
message("using builddir ${builddir}")

file(COPY ${basedir}/target/cloudwatch-connector-dist.jar DESTINATION ${builddir})

# the following strange looking lines will ensure that
# unwanted directories won't become part of package
set(CPACK_RPM_SPEC_MORE_DEFINE  "%define ignore \#
%define _tmppath  %{_topdir}/tmp
%define __jar_repack %{nil}"
)
set(CPACK_RPM_USER_FILELIST
"%ignore /etc"
"%ignore /etc/init.d"
"%ignore /opt"
"%ignore /opt/autopilot"
"%ignore /opt/autopilot/conf"
"%ignore /opt/autopilot/conf/default"
"%ignore /usr"
"%config(noreplace) /etc/init.d/*"
"%config(noreplace) /opt/autopilot/conf/*"
)
SET(CPACK_INSTALLED_DIRECTORIES
    "${builddir}" "${rpm_target_dir}"
    "${basedir}/init.d" "/etc/init.d"
    "${basedir}/conf" "/opt/autopilot/conf"
)

# set postinstall script
SET(CPACK_RPM_POST_INSTALL_SCRIPT_FILE "${basedir}/postinstall.sh")
#SET(CPACK_RPM_PRE_UNINSTALL_SCRIPT_FILE "${basedir}/preuninstall.sh")

# now process CPACK settings
INCLUDE(CPack)
