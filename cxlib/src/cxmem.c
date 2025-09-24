/*
 * cxmem.c
 *
 *
*/

#include "cxmem.h"




#if defined CXPLAT_ARCH_X64
/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
void stream_memset( cacheline_t *dst, const cacheline_t *src, const size_t cnt ) {
  for( size_t i=0; i<cnt; i++ ) {
    // Ensure streaming to memory using non-temporal hint and trigger write combining in the hardware
    _mm256_stream_si256( &dst[i].m256i[0], src->m256i[0] ); // e.g. "vmovntdq  ymmword ptr[rbx], ymm0"
    _mm256_stream_si256( &dst[i].m256i[1], src->m256i[1] ); // e.g. "vmovntdq  ymmword ptr[rbx+20h], ymm1"
  }
}
#endif


