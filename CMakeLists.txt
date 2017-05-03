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
# AUTOPILOT_VERSION_MAJOR  | VERSION_MAJOR (defined in this file)
# AUTOPILOT_VERSION_MINOR  | VERSION_MINOR (defined in this file)
# AUTOPILOT_VERSION_SP     | 0
# USE_SP_VERSION           | if not defined AUTOPILOT_VERSION_SP will be ignored
# AUTOPILOT_VERSION_PATCH  | 0
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
set(rpm_desc "arago GmbH Connector for CloudWatch")
set(rpm_target_dir "/opt/autopilot/cloudwatch/")

# set RPM arch:
set(rpm_arch "noarch")

# major/minor: if set via ENV then compare
set(VERSION_MAJOR 5)
set(VERSION_MINOR 5)
if(DEFINED ENV{AUTOPILOT_VERSION_MAJOR})
  if(NOT $ENV{AUTOPILOT_VERSION_MAJOR} EQUAL ${VERSION_MAJOR})
     message("Version conflict: VERSION_MAJOR from file: ${VERSION_MAJOR}. Env: AUTOPILOT_VERSION_MAJOR=$ENV{AUTOPILOT_VERSION_MAJOR}")
     return()
  endif()
  set(version_major "$ENV{AUTOPILOT_VERSION_MAJOR}")
else()
  set(version_major "${VERSION_MAJOR}")
endif()
if(DEFINED ENV{AUTOPILOT_VERSION_MINOR})
  if(NOT $ENV{AUTOPILOT_VERSION_MINOR} EQUAL ${VERSION_MINOR})
     message("Version conflict: VERSION_MINOR from file: ${VERSION_MINOR}. Env: AUTOPILOT_VERSION_MINOR=$ENV{AUTOPILOT_VERSION_MINOR}")
     return()
  endif()
  set(version_minor "$ENV{AUTOPILOT_VERSION_MINOR}")
else()
  set(version_minor ${VERSION_MINOR})
endif()
# patch: if not set via ENV take some default
if(DEFINED ENV{AUTOPILOT_VERSION_PATCH})
 set(version_patch "$ENV{AUTOPILOT_VERSION_PATCH}")
else()
  message("env var AUTOPILOT_VERSION_PATCH not set (will apply default). RPM numbering might be wrong")
  set(version_patch 0)
endif()
if(DEFINED ENV{USE_SP_VERSION})
  if(DEFINED ENV{AUTOPILOT_VERSION_SP})
    set(version_sp "$ENV{AUTOPILOT_VERSION_SP}")
  else()
    set(version_sp "0")
  endif()
  set(version_minor "${version_minor}.${version_sp}")
endif()

message("----------------------------------------")
message("building version: ${version_major}.${version_minor}-${version_patch}")
message("for: ${rpm_arch}")
message("----------------------------------------")

# set name/versions
SET(CPACK_PACKAGE_NAME ${rpm_name})
SET(CPACK_PACKAGE_VERSION_MAJOR ${version_major})
SET(CPACK_PACKAGE_VERSION_MINOR ${version_minor})
SET(CPACK_PACKAGE_VERSION_PATCH "${version_patch}")
SET(CPACK_PACKAGE_DESCRIPTION ${rpm_desc})
# and corresponding RPM stuff, too
SET(CPACK_RPM_PACKAGE_URL "https://www.arago.co")
SET(CPACK_RPM_PACKAGE_SUMMARY "${rpm_desc}")
SET(CPACK_RPM_PACKAGE_NAME ${rpm_name})
SET(CPACK_RPM_PACKAGE_VERSION "${version_major}.${version_minor}")
SET(CPACK_RPM_PACKAGE_ARCHITECTURE ${rpm_arch})
SET(CPACK_RPM_PACKAGE_RELEASE "${version_patch}")
SET(CPACK_RPM_PACKAGE_LICENSE "Proprietary")
SET(CPACK_RPM_PACKAGE_GROUP "Applications/Other")
SET(CPACK_RPM_PACKAGE_VENDOR "arago GmbH")
SET(CPACK_RPM_PACKAGE_DESCRIPTION "${CPACK_PACKAGE_DESCRIPTION}")
SET(CPACK_PACKAGE_FILE_NAME "${rpm_name}-${version_major}.${version_minor}-${version_patch}.${rpm_arch}")
message("build ${CPACK_PACKAGE_FILE_NAME}")
SET(CPACK_RPM_PACKAGE_REQUIRES "autopilot-backend-base, autopilot-config")

SET(CPACK_GENERATOR "RPM")
SET(CPACK_RPM_PACKAGE_DEBUG "0")

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
set(CPACK_RPM_SPEC_MORE_DEFINE "%define ignore \#")
set(CPACK_RPM_USER_FILELIST
"%ignore /etc"
"%ignore /etc/init.d"
"%ignore /opt"
"%ignore /opt/autopilot"
"%ignore /opt/autopilot/conf"
"%ignore /opt/autopilot/conf/default"
"%ignore /usr"
"%config(noreplace) /etc/init.d/*"
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
