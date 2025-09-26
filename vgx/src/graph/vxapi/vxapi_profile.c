/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxapi_profile.c
 * Author:  Stian Lysne <...>
 * 
 * Copyright © 2025 Rakuten, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 *****************************************************************************/

#include "_vgx.h"

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );

static CString_t *  VGXProfile__CPU_GetBrandString( void );
static int          VGXProfile__CPU_GetAVXVersion( void );
static int          VGXProfile__CPU_HasFeatureFMA( void );
static CString_t *  VGXProfile__CPU_GetInstructionSetExtensions( int *avxcompat );
#if defined CXPLAT_ARCH_X64
static int          VGXProfile__CPU_GetCoreCount( int *cores, int *threads );
#elif defined CXPLAT_ARCH_ARM64
static int          VGXProfile__CPU_GetCoreCount( int *P_cores, int *E_cores );
#endif
static CString_t *  VGXProfile__CPU_GetCacheInfo( void );
static CString_t *  VGXProfile__CPU_GetTLBInfo( void );
static int          VGXProfile__CPU_GetL2Size( void );
static int          VGXProfile__CPU_GetL2Associativity( void );
static bool         VGXProfile__CPU_IsAVXCompatible( int cpu_avx_version );
static int          VGXProfile__CPU_GetRequiredAVXVersion( void );


DLL_EXPORT vgx_IVGXProfile_t iVGXProfile = {


  .CPU = {
    .GetBrandString               = VGXProfile__CPU_GetBrandString,
    .GetAVXVersion                = VGXProfile__CPU_GetAVXVersion,
    .HasFeatureFMA                = VGXProfile__CPU_HasFeatureFMA,
    .GetInstructionSetExtensions  = VGXProfile__CPU_GetInstructionSetExtensions,
    .GetCoreCount                 = VGXProfile__CPU_GetCoreCount,
    .GetCacheInfo                 = VGXProfile__CPU_GetCacheInfo,
    .GetTLBInfo                   = VGXProfile__CPU_GetTLBInfo,
    .GetL2Size                    = VGXProfile__CPU_GetL2Size,
    .GetL2Associativity           = VGXProfile__CPU_GetL2Associativity,
    .IsAVXCompatible              = VGXProfile__CPU_IsAVXCompatible,
    .GetRequiredAVXVersion        = VGXProfile__CPU_GetRequiredAVXVersion
  }

};


#ifdef __cxlib_AVX512_MINIMUM__
static const int g_AVX_version = 512;
#elif defined __AVX2__
static const int g_AVX_version = 2;
#else
static const int g_AVX_version = -1;
#endif





/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static CString_t *  VGXProfile__CPU_GetBrandString( void ) {
  CString_t *CSTR__brand = NULL;
  char *brand = get_new_cpu_brand_string();
  if( brand ) {
    const char *p = brand;
    while( isblank(*p) && *p != '\0' ) {
      ++p;
    }
    CSTR__brand = CStringNew( p );
    free( brand ); 
  }
  return CSTR__brand;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int VGXProfile__CPU_GetAVXVersion( void ) {
#if defined CXPLAT_ARCH_X64
  return get_cpu_AVX_version();
#else
  return 0; // No AVX for non-x64 arch.
#endif
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int VGXProfile__CPU_HasFeatureFMA( void ) {
#if defined CXPLAT_ARCH_HASFMA
  return has_cpu_feature_FMA();
#else
  return 0;
#endif
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static CString_t *  VGXProfile__CPU_GetInstructionSetExtensions( int *avxcompat ) {
  CString_t *CSTR__cpuext = NULL;
  char *info = get_new_cpu_instruction_set_extensions( avxcompat );
  if( info ) {
    CSTR__cpuext = CStringNew( info );
    free( info );
  }
  return CSTR__cpuext;
}




/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#if defined CXPLAT_ARCH_X64
static int VGXProfile__CPU_GetCoreCount( int *cores, int *threads ) {
  return get_cpu_cores( cores, threads );
}
#elif defined CXPLAT_ARCH_ARM64
static int VGXProfile__CPU_GetCoreCount( int *P_cores, int *E_cores ) {
  return get_cpu_cores( P_cores, E_cores );
}
#endif



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static CString_t *  VGXProfile__CPU_GetCacheInfo( void ) {
  CString_t *CSTR__info = NULL;
  char *info = get_new_cpu_cache_info();
  if( info ) {
    CSTR__info = CStringNew( info );
    free( info );
  }
  return CSTR__info;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static CString_t *  VGXProfile__CPU_GetTLBInfo( void ) {
  CString_t *CSTR__info = NULL;
  char *info = get_new_cpu_tlb_info();
  if( info ) {
    CSTR__info = CStringNew( info );
    free( info );
  }
  return CSTR__info;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int VGXProfile__CPU_GetL2Size( void ) {
  return get_cpu_L2_size();
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int VGXProfile__CPU_GetL2Associativity( void ) {
  return get_cpu_L2_associativity();
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static bool VGXProfile__CPU_IsAVXCompatible( int cpu_avx_version ) {
  if( cpu_avx_version >= iVGXProfile.CPU.GetRequiredAVXVersion() ) {
    return true;
  }
  else {
    return false;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int VGXProfile__CPU_GetRequiredAVXVersion( void ) {
  return g_AVX_version;
}




#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxapi_profile.h"

test_descriptor_t _vgx_vxapi_profile_tests[] = {
  { "VGX Profile API Tests", __utest_vxapi_profile },
  {NULL}
};
#endif
