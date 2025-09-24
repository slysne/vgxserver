#ifndef VERSIONINFO_H
#define VERSIONINFO_H

/* http://semver.org/ */
/* Find a better way to manage this than modifying source code */
#define VGX_VERSION_MAJOR "1"
#define VGX_VERSION_MINOR "0"
#define VGX_VERSION_PATCH "0"

#define VGX_VERSION_PRE_REL ""

#define VGX_VERSION_BUILD "+build.##VGX_VERSION_BUILD##"
#define VGX_VERSION_ENV "##VGX_VERSION_ENV##"

#define VGX_VERSION_MAX_CHARS 254


#endif
