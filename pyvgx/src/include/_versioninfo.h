#ifndef VERSIONINFO_H
#define VERSIONINFO_H

/* http://semver.org/ */
/* Find a better way to manage this than modifying source code */
#define PYVGX_VERSION_MAJOR "1"
#define PYVGX_VERSION_MINOR "0"
#define PYVGX_VERSION_PATCH "0"

#define PYVGX_VERSION_PRE_REL ""

#define PYVGX_VERSION_BUILD "+build.##PYVGX_VERSION_BUILD##"
#define PYVGX_VERSION_ENV "##PYVGX_VERSION_ENV##"

#define PYVGX_VERSION_MAX_CHARS 254


#endif
