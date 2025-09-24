#ifndef VERSIONINFO_H
#define VERSIONINFO_H

/* http://semver.org/ */
/* Find a better way to manage this than modifying source code */
#define CXLIB_VERSION_MAJOR "1"
#define CXLIB_VERSION_MINOR "0"
#define CXLIB_VERSION_PATCH "0"

#define CXLIB_VERSION_PRE_REL ""

#define CXLIB_VERSION_BUILD "+build.##CXLIB_VERSION_BUILD##"
#define CXLIB_VERSION_ENV "##CXLIB_VERSION_ENV##"

#define CXLIB_VERSION_MAX_CHARS 254


#endif
