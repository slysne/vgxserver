/*######################################################################
 *#
 *# vgx_server_plugin.c
 *#
 *#
 *######################################################################
 */


#include "_vgx.h"
#include "_vxserver.h"


/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
DLL_HIDDEN int vgx_server_plugin__none( vgx_Graph_t *sysgraph, const char *plugin, vgx_URIQueryParameters_t *params, vgx_VGXServerRequest_t *request, vgx_VGXServerResponse_t *response, CString_t **CSTR__error ) {
  return -1;
}


