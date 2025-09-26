/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  cxlib
 * File:    cxplat.c
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

#include "cxfileio.h"


#if defined CXPLAT_LINUX_ANY
#include <cpuid.h>
#endif


#if defined CXPLAT_ARCH_X64
static int cxplat_cpuid( unsigned int leaf, int *eax, int *ebx, int *ecx, int *edx );


typedef union __u_cpuregs_t {
  int data[4];
  char strdata[16];
  struct {
    int eax;
    int ebx;
    int ecx;
    int edx;
  };
} __cpuregs_t;



#define hasbit( Reg, B )          (((Reg) & ( 1 << (B) )) != 0)
#define feature( Reg, B, Name )   (hasbit( Reg, B ) ? (Name) : "")



static int cxplat_cpuid( unsigned int leaf, int *eax, int *ebx, int *ecx, int *edx ) {
#if defined CXPLAT_WINDOWS_X64
  __cpuregs_t regs;
  __cpuid( regs.data, leaf );
  *eax = regs.eax;
  *ebx = regs.ebx;
  *ecx = regs.ecx;
  *edx = regs.edx;
  return 0;
#elif defined CXPLAT_LINUX_ANY
  return __get_cpuid( leaf, eax, ebx, ecx, edx );
#else
  return -1;
#endif
}



int cxplat_cpuidex( unsigned int leaf, unsigned int subleaf, int *eax, int *ebx, int *ecx, int *edx ) {
#if defined CXPLAT_WINDOWS_X64
  __cpuregs_t regs;
  __cpuidex( regs.data, leaf, subleaf );
  *eax = regs.eax;
  *ebx = regs.ebx;
  *ecx = regs.ecx;
  *edx = regs.edx;
  return 0;
#elif defined CXPLAT_LINUX_ANY
  unsigned int maxleaf = __get_cpuid_max( leaf & 0x8000000, 0 );
  if( maxleaf == 0 || maxleaf < leaf ) {
    return -1;
  }
  __cpuid_count( leaf, subleaf, *eax, *ebx, *ecx, *edx );
  return 0;
#else
  return -1;
#endif
}
#endif


#if defined CXPLAT_LINUX_ANY
/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
__inline static bool __proc_value_for( const char *key, const char *line, int64_t *rvalue ) {
#define __skip_spaces( Cursor ) while( *(Cursor) && *(Cursor) <= 32 ) { ++(Cursor); }

  const char *p = line;
  const char *k = key;
  // Match key to line entry
  while( *p && (*p != ':' && *p > 32) ) {
    if( *k && *k++ != *p++ ) {
      return false;
    }
  }
  // Make sure we matched the entire key
  if( *k != '\0' ) {
    return false;
  }
  // Skip past the colon
  if( *p == ':' ) {
    ++p;
  }
  __skip_spaces( p )
  // Parse decimal
  int64_t v = 0;
  int d;
  while( (d = *p++ - '0') >= 0 && d <= 9 ) {
    v = 10 * v + d;
  }
  __skip_spaces( p )
  // Parse unit
  int64_t u = !strncmp( p, "kB", 2 ) ? 1024 : 1;
  v *= u;
  *rvalue = v;
  return true;
}
#endif



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
int get_system_physical_memory( int64_t *global_physical, int64_t *global_use_percent, int64_t *process_physical ) {

#if defined CXPLAT_WINDOWS_X64
  // Get global memory info
  if( global_physical || global_use_percent ) {
    MEMORYSTATUSEX memstatus;
    memstatus.dwLength = sizeof( MEMORYSTATUSEX );
    if( GlobalMemoryStatusEx( &memstatus ) ) {
      if( global_use_percent ) {
        *global_use_percent = memstatus.dwMemoryLoad;
      }
      if( global_physical ) {
        *global_physical = memstatus.ullTotalPhys;
      }
    }
    else {
      return -1;
    }
  }

  // Get process memory info
  if( process_physical ) {
    HANDLE process = GetCurrentProcess();
    PROCESS_MEMORY_COUNTERS_EX pmc;
    if( GetProcessMemoryInfo( process, (PROCESS_MEMORY_COUNTERS*)&pmc, sizeof(pmc) ) != 0 ) {
      *process_physical = pmc.WorkingSetSize;
    }
    else {
      return -1;
    }
  }

  return 0;


#elif defined CXPLAT_LINUX_ANY

  char line[ 128 ] = {0};
  FILE *f = NULL;

  // Get global memory info
  if( global_physical || global_use_percent ) {
    if( (f = CX_FOPEN( "/proc/meminfo", "r" )) == NULL ) {
      return -1;
    }
    int64_t total = 0;
    int64_t avail = 0;
    int n = 0;
    while( n < 2 && fgets( line, 128, f ) != NULL ) {
      if( __proc_value_for( "MemTotal", line, &total ) ) {
        ++n;
      }
      else if( __proc_value_for( "MemAvailable", line, &avail ) ) {
        ++n;
      }
    }
    if( global_physical ) {
      *global_physical = total;
    }
    if( global_use_percent && total > 0 ) {
      *global_use_percent = (int64_t)round( 100.0 * (total - avail)  / (double)total );
    }
    CX_FCLOSE( f );
  }

  // Get process memory info
  if( process_physical ) {
    if( (f = CX_FOPEN( "/proc/self/status", "r" )) == NULL ) {
      return -1;
    }
    while( fgets( line, 128, f ) != NULL ) {
      if( __proc_value_for( "VmRSS", line, process_physical ) ) {
        break;
      }
    }
    CX_FCLOSE( f );
  }

  return 0;

#elif defined CXPLAT_MAC_ARM64

  if (global_physical || global_use_percent) {
    // Get total physical memory
    int64_t memsize = 0;
    size_t size = sizeof(memsize);
    if (sysctlbyname("hw.memsize", &memsize, &size, NULL, 0) != 0) {
      //perror("sysctlbyname hw.memsize failed");
      return -1;
    }

    if (global_physical) {
      *global_physical = memsize;
    }

    if (global_use_percent) {
      // Get VM statistics to estimate used memory
      mach_msg_type_number_t count = HOST_VM_INFO64_COUNT;
      vm_statistics64_data_t vm_stats;
      if (host_statistics64(mach_host_self(), HOST_VM_INFO64, (host_info64_t)&vm_stats, &count) != KERN_SUCCESS) {
        //perror("host_statistics64 failed");
        return -1;
      }

      // Grab the details
      int64_t n_active = (int64_t)vm_stats.active_count;
      int64_t n_inactive = (int64_t)vm_stats.inactive_count;
      int64_t n_speculative = (int64_t)vm_stats.speculative_count;
      int64_t n_wired = (int64_t)vm_stats.wire_count;
      int64_t n_compressed = (int64_t)vm_stats.compressor_page_count;
      int64_t n_purgeable = (int64_t)vm_stats.purgeable_count;
      int64_t n_external = (int64_t)vm_stats.external_page_count;
      int64_t n_free = (int64_t)vm_stats.free_count;

      // Memory unavailable for any malloc() on the system
      int64_t pagesize = (int64_t)sysconf(_SC_PAGESIZE);
      int64_t n_used = n_active + n_inactive + n_speculative + n_wired + n_compressed - n_purgeable - n_external;
      int64_t used_memory = n_used * pagesize;

      // Calculate percent used (used / total)
      *global_use_percent = (int64_t)round(100.0 * (double)used_memory / (double)memsize);
    }
  }

  if (process_physical) {
    // Get process physical memory usage
    struct task_vm_info info;
    mach_msg_type_number_t count = TASK_VM_INFO_COUNT;

    if (task_info(mach_task_self(), TASK_VM_INFO, (task_info_t)&info, &count) != KERN_SUCCESS) {
      return -1;
    }

    *process_physical = (int64_t)info.phys_footprint;
  }

  return 0;

#else
  return -1;
#endif
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
char * get_new_cpu_brand_string( void ) {
  char *string = NULL;
#if defined CXPLAT_ARCH_X64
  int sz = 0;

  __cpuregs_t regs = {0};

  // Check if brand string is supported
  cxplat_cpuid( 0x80000000, &regs.eax, &regs.ebx, &regs.ecx, &regs.edx );

  // Brand string supported
  if( regs.eax & 0x80000000 ) {
    // Get data
    unsigned first = 0x80000002;
    unsigned last = regs.eax;

    sz = (last - first + 1) * 16;

    if( (string = calloc( 1, sz )) != NULL ) {
      char *dest = string;
      for( unsigned i=first; i<=last; i++ ) {
        const char *src = regs.strdata;
        cxplat_cpuid( i, &regs.eax, &regs.ebx, &regs.ecx, &regs.edx );
        for( int n=0; n<16; n++ ) {
          *dest++ = *src++;
        }
      }
    }
  }
#elif defined CXPLAT_ARCH_ARM64

  // TODO: Get the CPU name

  #if defined CXPLAT_MAC_ARM64
  size_t len = 0;

  // Try Intel-style brand string first
  if (sysctlbyname("machdep.cpu.brand_string", NULL, &len, NULL, 0) == 0) {
    string = calloc(len,1);
    if (string && sysctlbyname("machdep.cpu.brand_string", string, &len, NULL, 0) == 0) {
      return string;
    }
    free(string);
  }

  // Fall back to hw.model on Apple Silicon
  if (sysctlbyname("hw.model", NULL, &len, NULL, 0) == 0) {
    string = calloc(len,1);
    if (string && sysctlbyname("hw.model", string, &len, NULL, 0) == 0) {
      return string;
    }
    free(string);
  }

  #else
  if( (string = calloc( 64, sizeof(char) )) != NULL ) {
    snprintf( string, 63, "Unknown" );
  }
  #endif

#else
#error "Unsupported platform."
#endif
  return string;
}



/*


01H
  ECX

    0   SSE3        Streaming SIMD Extensions 3 (SSE3). A value of 1 indicates the processor supports this
                    technology.
    1   PCLMULQDQ   PCLMULQDQ. A value of 1 indicates the processor supports the PCLMULQDQ instruction.
    2   DTES64      64-bit DS Area. A value of 1 indicates the processor supports DS area using 64-bit layout.
    3   MONITOR     MONITOR/MWAIT. A value of 1 indicates the processor supports this feature.
    4   DS-CPL      CPL Qualified Debug Store. A value of 1 indicates the processor supports the extensions to the
                    Debug Store feature to allow for branch message storage qualified by CPL.
    5   VMX         Virtual Machine Extensions. A value of 1 indicates that the processor supports this technology.
    6   SMX         Safer Mode Extensions. A value of 1 indicates that the processor supports this technology. See
                    Chapter 6, Safer Mode Extensions Reference.
    7   EIST        Enhanced Intel SpeedStep technology. A value of 1 indicates that the processor supports this
                    technology.
    8   TM2         Thermal Monitor 2. A value of 1 indicates whether the processor supports this technology.
    9   SSSE3       A value of 1 indicates the presence of the Supplemental Streaming SIMD Extensions 3 (SSSE3). A
                    value of 0 indicates the instruction extensions are not present in the processor.
    10  CNXT-ID     L1 Context ID. A value of 1 indicates the L1 data cache mode can be set to either adaptive mode
                    or shared mode. A value of 0 indicates this feature is not supported. See definition of the
                    IA32_MISC_ENABLE MSR Bit 24 (L1 Data Cache Context Mode) for details.
    11  SDBG        A value of 1 indicates the processor supports IA32_DEBUG_INTERFACE MSR for silicon debug.
    12  FMA         A value of 1 indicates the processor supports FMA extensions using YMM state.
    13  CMPXCHG16B  CMPXCHG16B Available. A value of 1 indicates that the feature is available. See the
                    CMPXCHG8B/CMPXCHG16B—Compare and Exchange Bytes section in this chapter for a
                    description.
    14  xTPR Update
        Control     xTPR Update Control. A value of 1 indicates that the processor supports changing
                    IA32_MISC_ENABLE[bit 23].
    15  PDCM        Perfmon and Debug Capability: A value of 1 indicates the processor supports the performance
                    and debug feature indication MSR IA32_PERF_CAPABILITIES.
    16  Reserved    Reserved
    17  PCID        Process-context identifiers. A value of 1 indicates that the processor supports PCIDs and that
                    software may set CR4.PCIDE to 1.
    18  DCA         A value of 1 indicates the processor supports the ability to prefetch data from a memory mapped
                    device.
    19  SSE4_1      A value of 1 indicates that the processor supports SSE4.1.
    20  SSE4_2      A value of 1 indicates that the processor supports SSE4.2.
    21  x2APIC      A value of 1 indicates that the processor supports x2APIC feature.
    22  MOVBE       A value of 1 indicates that the processor supports MOVBE instruction.
    23  POPCNT      A value of 1 indicates that the processor supports the POPCNT instruction.
    24  TSC-Deadline A value of 1 indicates that the processor’s local APIC timer supports one-shot operation using a
                    TSC deadline value.
    25  AESNI       A value of 1 indicates that the processor supports the AESNI instruction extensions.
    26  XSAVE       A value of 1 indicates that the processor supports the XSAVE/XRSTOR processor extended states
                    feature, the XSETBV/XGETBV instructions, and XCR0.
    27  OSXSAVE     A value of 1 indicates that the OS has set CR4.OSXSAVE[bit 18] to enable XSETBV/XGETBV
                    instructions to access XCR0 and to support processor extended state management using
                    XSAVE/XRSTOR.
    28  AVX         A value of 1 indicates the processor supports the AVX instruction extensions.
    29  F16C        A value of 1 indicates that processor supports 16-bit floating-point conversion instructions.
    30  RDRAND      A value of 1 indicates that processor supports RDRAND instruction.
    31  Not Used    Always returns 0.


  EDX
    0   FPU         Floating Point Unit On-Chip. The processor contains an x87 FPU.
    1   VME         Virtual 8086 Mode Enhancements. Virtual 8086 mode enhancements, including CR4.VME for controlling the
                    feature, CR4.PVI for protected mode virtual interrupts, software interrupt indirection, expansion of the TSS
                    with the software indirection bitmap, and EFLAGS.VIF and EFLAGS.VIP flags.
    2   DE          Debugging Extensions. Support for I/O breakpoints, including CR4.DE for controlling the feature, and optional
                    trapping of accesses to DR4 and DR5.
    3   PSE         Page Size Extension. Large pages of size 4 MByte are supported, including CR4.PSE for controlling the
                    feature, the defined dirty bit in PDE (Page Directory Entries), optional reserved bit trapping in CR3, PDEs, and
                    PTEs.
    4   TSC         Time Stamp Counter. The RDTSC instruction is supported, including CR4.TSD for controlling privilege.
    5   MSR         Model Specific Registers RDMSR and WRMSR Instructions. The RDMSR and WRMSR instructions are
                    supported. Some of the MSRs are implementation dependent.
    6   PAE         Physical Address Extension. Physical addresses greater than 32 bits are supported: extended page table
                    entry formats, an extra level in the page translation tables is defined, 2-MByte pages are supported instead of
                    4 Mbyte pages if PAE bit is 1.
    7   MCE         Machine Check Exception. Exception 18 is defined for Machine Checks, including CR4.MCE for controlling the
                    feature. This feature does not define the model-specific implementations of machine-check error logging,
                    reporting, and processor shutdowns. Machine Check exception handlers may have to depend on processor
                    version to do model specific processing of the exception, or test for the presence of the Machine Check feature.
    8   CX8         CMPXCHG8B Instruction. The compare-and-exchange 8 bytes (64 bits) instruction is supported (implicitly
                    locked and atomic).
    9   APIC        APIC On-Chip. The processor contains an Advanced Programmable Interrupt Controller (APIC), responding to
                    memory mapped commands in the physical address range FFFE0000H to FFFE0FFFH (by default - some
                    processors permit the APIC to be relocated).
    10  Reserved    Reserved
    11  SEP         SYSENTER and SYSEXIT Instructions. The SYSENTER and SYSEXIT and associated MSRs are supported.
    12  MTRR        Memory Type Range Registers. MTRRs are supported. The MTRRcap MSR contains feature bits that describe
                    what memory types are supported, how many variable MTRRs are supported, and whether fixed MTRRs are
                    supported.
    13  PGE         Page Global Bit. The global bit is supported in paging-structure entries that map a page, indicating TLB entries
                    that are common to different processes and need not be flushed. The CR4.PGE bit controls this feature.
    14  MCA         Machine Check Architecture. A value of 1 indicates the Machine Check Architecture of reporting machine
                    errors is supported. The MCG_CAP MSR contains feature bits describing how many banks of error reporting
        MSRs        are supported.
    15  CMOV        Conditional Move Instructions. The conditional move instruction CMOV is supported. In addition, if x87 FPU is
                    present as indicated by the CPUID.FPU feature bit, then the FCOMI and FCMOV instructions are supported
    16  PAT         Page Attribute Table. Page Attribute Table is supported. This feature augments the Memory Type Range
                    Registers (MTRRs), allowing an operating system to specify attributes of memory accessed through a linear
                    address on a 4KB granularity.
    17  PSE-36      36-Bit Page Size Extension. 4-MByte pages addressing physical memory beyond 4 GBytes are supported with
                    32-bit paging. This feature indicates that upper bits of the physical address of a 4-MByte page are encoded in
                    bits 20:13 of the page-directory entry. Such physical addresses are limited by MAXPHYADDR and may be up to
                    40 bits in size.
    18  PSN         Processor Serial Number. The processor supports the 96-bit processor identification number feature and the
                    feature is enabled.
    19  CLFSH       CLFLUSH Instruction. CLFLUSH Instruction is supported.
    20  Reserved    Reserved
    21  DS          Debug Store. The processor supports the ability to write debug information into a memory resident buffer.
                    This feature is used by the branch trace store (BTS) and processor event-based sampling (PEBS) facilities (see
                    Chapter 23, Introduction to Virtual-Machine Extensions, in the Intel 64 and IA-32 Architectures Software
                    Developer’s Manual, Volume 3C).
    22  ACPI        Thermal Monitor and Software Controlled Clock Facilities. The processor implements internal MSRs that
                    allow processor temperature to be monitored and processor performance to be modulated in predefined duty
                    cycles under software control.
    23  MMX         Intel MMX Technology. The processor supports the Intel MMX technology.
    24  FXSR        FXSAVE and FXRSTOR Instructions. The FXSAVE and FXRSTOR instructions are supported for fast save and
                    restore of the floating point context. Presence of this bit also indicates that CR4.OSFXSR is available for an
                    operating system to indicate that it supports the FXSAVE and FXRSTOR instructions.
    25  SSE         SSE. The processor supports the SSE extensions.
    26  SSE2        SSE2. The processor supports the SSE2 extensions.
    27  SS          Self Snoop. The processor supports the management of conflicting memory types by performing a snoop of its
                    own cache structure for transactions issued to the bus.
    28  HTT         Max APIC IDs reserved field is Valid. A value of 0 for HTT indicates there is only a single logical processor in
                    the package and software should assume only a single APIC ID is reserved. A value of 1 for HTT indicates the
                    value in CPUID.1.EBX[23:16] (the Maximum number of addressable IDs for logical processors in this package) is
                    valid for the package.
    29  TM          Thermal Monitor. The processor implements the thermal monitor automatic thermal control circuitry (TCC).
    30  Reserved    Reserved
    31  PBE         Pending Break Enable. The processor supports the use of the FERR#/PBE# pin when the processor is in the
                    stop-clock state (STPCLK# is asserted) to signal the processor that an interrupt is pending and that the
                    processor should return to normal operation to handle the interrupt.



07H     Sub-leaf 0 (Input ECX = 0). *

  EAX Bits 31 - 00: Reports the maximum input value for supported leaf 7 sub-leaves.

  EBX Bit 00: FSGSBASE. Supports RDFSBASE/RDGSBASE/WRFSBASE/WRGSBASE if 1.
    Bit 01: IA32_TSC_ADJUST MSR is supported if 1.
    Bit 02: SGX. Supports Intel Software Guard Extensions (Intel SGX Extensions) if 1.
    Bit 03: BMI1.
    Bit 04: HLE.
    Bit 05: AVX2.
    Bit 06: FDP_EXCPTN_ONLY. x87 FPU Data Pointer updated only on x87 exceptions if 1.
    Bit 07: SMEP. Supports Supervisor-Mode Execution Prevention if 1.
    Bit 08: BMI2.
    Bit 09: Supports Enhanced REP MOVSB/STOSB if 1.
    Bit 10: INVPCID. If 1, supports INVPCID instruction for system software that manages process-context identifiers.
    Bit 11: RTM.
    Bit 12: RDT-M. Supports Intel Resource Director Technology (Intel RDT) Monitoring capability if 1.
    Bit 13: Deprecates FPU CS and FPU DS values if 1.
    Bit 14: MPX. Supports Intel Memory Protection Extensions if 1.
    Bit 15: RDT-A. Supports Intel Resource Director Technology (Intel RDT) Allocation capability if 1.
    Bit 16: AVX512F.
    Bit 17: AVX512DQ.
    Bit 18: RDSEED.
    Bit 19: ADX.
    Bit 20: SMAP. Supports Supervisor-Mode Access Prevention (and the CLAC/STAC instructions) if 1.
    Bit 21: AVX512_IFMA.
    Bit 22: Reserved.
    Bit 23: CLFLUSHOPT.
    Bit 24: CLWB.
    Bit 25: Intel Processor Trace.
    Bit 26: AVX512PF. (Intel Xeon Phi only.)
    Bit 27: AVX512ER. (Intel Xeon Phi only.)
    Bit 28: AVX512CD.
    Bit 29: SHA. supports Intel Secure Hash Algorithm Extensions (Intel SHA Extensions) if 1.
    Bit 30: AVX512BW.
    Bit 31: AVX512VL.

  ECX 
    Bit 00: PREFETCHWT1. (Intel Xeon Phi only.)
    Bit 01: AVX512_VBMI.
    Bit 02: UMIP. Supports user-mode instruction prevention if 1.
    Bit 03: PKU. Supports protection keys for user-mode pages if 1.
    Bit 04: OSPKE. If 1, OS has set CR4.PKE to enable protection keys (and the RDPKRU/WRPKRU instructions).
    Bit 05: WAITPKG.
    Bit 06: AVX512_VBMI2.
    Bit 07: CET_SS. Supports CET shadow stack features if 1. Processors that set this bit define bits 1:0 of the
            IA32_U_CET and IA32_S_CET MSRs. Enumerates support for the following MSRs:
            IA32_INTERRUPT_SPP_TABLE_ADDR, IA32_PL3_SSP, IA32_PL2_SSP, IA32_PL1_SSP, and
            IA32_PL0_SSP.
    Bit 08: GFNI.
    Bit 09: VAES.
    Bit 10: VPCLMULQDQ.
    Bit 11: AVX512_VNNI.
    Bit 12: AVX512_BITALG.
    Bit 13: TME_EN. If 1, the following MSRs are supported: IA32_TME_CAPABILITY, IA32_TME_ACTIVATE,
            IA32_TME_EXCLUDE_MASK, and IA32_TME_EXCLUDE_BASE.
    Bit 14: AVX512_VPOPCNTDQ.
    Bit 15: Reserved.
    Bit 16: LA57. Supports 57-bit linear addresses and five-level paging if 1.
    Bits 21 - 17: The value of MAWAU used by the BNDLDX and BNDSTX instructions in 64-bit mode.
    Bit 22: RDPID and IA32_TSC_AUX are available if 1.
    Bit 23: KL. Supports Key Locker if 1.
    Bit 24: Reserved.
    Bit 25: CLDEMOTE. Supports cache line demote if 1.
    Bit 26: Reserved.
    Bit 27: MOVDIRI. Supports MOVDIRI if 1.
    Bit 28: MOVDIR64B. Supports MOVDIR64B if 1.
    Bit 29: Reserved.
    Bit 30: SGX_LC. Supports SGX Launch Configuration if 1.
    Bit 31: PKS. Supports protection keys for supervisor-mode pages if 1.

  EDX 
    Bit 01: Reserved.
    Bit 02: AVX512_4VNNIW. (Intel Xeon Phi only.)
    Bit 03: AVX512_4FMAPS. (Intel Xeon Phi only.)
    Bit 04: Fast Short REP MOV.
    Bits 07-05: Reserved.
    Bit 08: AVX512_VP2INTERSECT.
    Bit 09: Reserved.
    Bit 10: MD_CLEAR supported.
    Bits 14-11: Reserved.
    Bit 15: Hybrid. If 1, the processor is identified as a hybrid part.
    Bits 17-16: Reserved.
    Bit 18: PCONFIG. Supports PCONFIG if 1.
    Bit 19: Reserved.
    Bit 20: CET_IBT. Supports CET indirect branch tracking features if 1. Processors that set this bit define
    bits 5:2 and bits 63:10 of the IA32_U_CET and IA32_S_CET MSRs.
    Bits 25 - 21: Reserved.
    Bit 26: Enumerates support for indirect branch restricted speculation (IBRS) and the indirect branch predictor barrier (IBPB). Processors that set this bit support the IA32_SPEC_CTRL MSR and the
            IA32_PRED_CMD MSR. They allow software to set IA32_SPEC_CTRL[0] (IBRS) and IA32_PRED_CMD[0]
            (IBPB).
    Bit 27: Enumerates support for single thread indirect branch predictors (STIBP). Processors that set this
            bit support the IA32_SPEC_CTRL MSR. They allow software to set IA32_SPEC_CTRL[1] (STIBP).
    Bit 28: Enumerates support for L1D_FLUSH. Processors that set this bit support the IA32_FLUSH_CMD
            MSR. They allow software to set IA32_FLUSH_CMD[0] (L1D_FLUSH).
    Bit 29: Enumerates support for the IA32_ARCH_CAPABILITIES MSR.
    Bit 30: Enumerates support for the IA32_CORE_CAPABILITIES MSR.
            IA32_CORE_CAPABILITIES is an architectural MSR that enumerates model-specific features. A bit being
            set in this MSR indicates that a model specific feature is supported; software must still consult CPUID
            family/model/stepping to determine the behavior of the enumerated feature as features enumerated in
            IA32_CORE_CAPABILITIES may have different behavior on different processor models.
            Additionally, on hybrid parts (CPUID.07H.0H:EDX[15]=1), software must consult the native model ID and
            core type from the Hybrid Information Enumeration Leaf.
    Bit 31: Enumerates support for Speculative Store Bypass Disable (SSBD). Processors that set this bit support the IA32_SPEC_CTRL MSR. They allow software to set IA32_SPEC_CTRL[2] (SSBD).
    
    NOTE: 
      * If ECX contains an invalid sub-leaf index, EAX/EBX/ECX/EDX return 0. Sub-leaf index n is invalid if n
        exceeds the value that sub-leaf 0 returns in EAX.

*/




/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
int get_cpu_AVX_version( void ) {
#if defined CXPLAT_ARCH_X64
  __cpuregs_t reg10 = {0};
  __cpuregs_t reg70 = {0};
    
  cxplat_cpuidex( 0x1, 0, &reg10.eax, &reg10.ebx, &reg10.ecx, &reg10.edx );
  cxplat_cpuidex( 0x7, 0, &reg70.eax, &reg70.ebx, &reg70.ecx, &reg70.edx );

  // AVX512F, AVX512DQ, AVX512CD, AVX512BW, AVX512VL (the minimum)
  if( hasbit( reg70.ebx, 16 ) && hasbit( reg70.ebx, 17 ) && hasbit( reg70.ebx, 28 ) && hasbit( reg70.ebx, 30 ) && hasbit( reg70.ebx, 31 ) ) {
    return 512;
  }

  // AVX2
  if( hasbit( reg70.ebx, 5 ) ) {
    return 2;
  }

  // AVX
  if( hasbit( reg10.ecx, 28 ) ) {
    return 1;
  }
#endif

  // No AVX
  return 0;

}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
int has_cpu_feature_FMA( void ) {
#if defined CXPLAT_ARCH_X64
  __cpuregs_t reg10 = {0};
  cxplat_cpuidex( 0x1, 0, &reg10.eax, &reg10.ebx, &reg10.ecx, &reg10.edx );

  // FMA
  return hasbit( reg10.ecx, 12 );
#else
  return 0;
#endif
}



#if defined CXPLAT_ARCH_X64
/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
char * get_new_cpu_instruction_set_extensions( int *avxcompat ) {

  __cpuregs_t reg10 = {0};
  __cpuregs_t reg70 = {0};
  __cpuregs_t reg71 = {0};

  *avxcompat = get_cpu_AVX_version();

  cxplat_cpuidex( 0x1, 0, &reg10.eax, &reg10.ebx, &reg10.ecx, &reg10.edx );
  cxplat_cpuidex( 0x7, 0, &reg70.eax, &reg70.ebx, &reg70.ecx, &reg70.edx );
  cxplat_cpuidex( 0x7, 1, &reg71.eax, &reg71.ebx, &reg71.ecx, &reg71.edx );

  const char *FEATURES[] = {
    feature( reg10.ecx, 12, "FMA" ),
    feature( reg10.ecx, 28, "AVX" ),
    feature( reg10.ecx, 29, "F16C" ),

    feature( reg70.ebx,  5, "AVX2" ),
    feature( reg70.ebx, 16, "AVX512F" ),
    feature( reg70.ebx, 17, "AVX512DQ" ),
    feature( reg70.ebx, 21, "AVX512_IFMA" ),
    feature( reg70.ebx, 26, "AVX512PF" ),
    feature( reg70.ebx, 27, "AVX512ER" ),
    feature( reg70.ebx, 28, "AVX512CD" ),
    feature( reg70.ebx, 29, "SHA" ),
    feature( reg70.ebx, 30, "AVX512BW" ),
    feature( reg70.ebx, 31, "AVX512VL" ),

    feature( reg70.ecx,  1, "AVX512_VBMI" ),
    feature( reg70.ecx,  6, "AVX512_VBMI2" ),
    feature( reg70.ecx, 11, "AVX512_VNNI" ),
    feature( reg70.ecx, 12, "AVX512_BITALG" ),
    feature( reg70.ecx, 14, "AVX512_VPOPCNTDQ" ),

    feature( reg70.edx,  2, "AVX512_4VNNIW" ),
    feature( reg70.edx,  3, "AVX512_4FMAPS" ),
    feature( reg70.edx,  8, "AVX512_VP2INTERSECT" ),

    feature( reg71.eax,  4, "AVX_VNNI" ),
    feature( reg71.eax,  5, "AVX512_BF16" ),
    feature( reg71.eax, 23, "AVX_IFMA" ),

    feature( reg71.edx,  4, "AVX_VNN_INT8" ),
    feature( reg71.edx,  5, "AVX_NE_CONVERT" ),

    NULL
  };


  const size_t N = sizeof( FEATURES )/sizeof( char* );
  char *info = calloc( N, 32 );
  if( info ) {
    char *p = info;
    const char **cursor = FEATURES;
    while( *cursor != NULL ) {
      const char *has = *cursor++;
      bool exists = has[0];
      while( *has ) {
        *p++ = *has++;
      }
      if( exists ) {
        *p++ = ' ';
      }
    }
    *p = '\0';
  }
  return info;

}




  /*

  01H
  
  EAX   Version Information: Type, Family, Model, and Stepping ID (see Figure 3-6).

  EBX   Bits 07 - 00: Brand Index.
        Bits 15 - 08: CLFLUSH line size (Value * 8 = cache line size in bytes; used also by CLFLUSHOPT).
        Bits 23 - 16: Maximum number of addressable IDs for logical processors in this physical package*.
        Bits 31 - 24: Initial APIC ID**.

  ECX   Feature Information (see Figure 3-7 and Table 3-10).

  EDX   Feature Information (see Figure 3-8 and Table 3-11).

  NOTES:
  *   The nearest power-of-2 integer that is not smaller than EBX[23:16] is
      the number of unique initial APICIDs reserved for addressing different
      logical processors in a physical package. This field is only valid if
      CPUID.1.EDX.HTT[bit 28]= 1.
  **  The 8-bit initial APIC ID in EBX[31:24] is replaced by the 32-bit
      x2APIC ID, available in Leaf 0BH and Leaf 1FH.
  */

  /*

  80000006H

  EAX   Reserved=0
  EBX   Reserved=0
  ECX   Bits 07 - 00: Cache Line size in bytes.
        Bits 11 - 08: Reserved.
        Bits 15 - 12: L2 Associativity field *.
        Bits 31 - 16: Cache size in 1K units.
  EDX   Reserved=0

        *) L2 associativity field encodings:
        00H - Disabled                          08H - 16 ways
        01H - 1 way (direct mapped)             09H - Reserved
        02H - 2 ways                            0AH - 32 ways
        03H - Reserved                          0BH - 48 ways
        04H - 4 ways                            0CH - 64 ways
        05H - Reserved                          0DH - 96 ways
        06H - 8 ways                            0EH - 128 ways
        07H - See CPUID leaf 04H, sub-leaf 2**  0FH - Fully associative

        **) CPUID leaf 04H provides details of deterministic cache parameters, including the L2 cache in sub-leaf 2

  */
#elif CXPLAT_ARCH_ARM64
char * get_new_cpu_instruction_set_extensions( int *__ign ) {

  static const char hw_optional_arm_FEAT_[] = "hw.optional.arm.FEAT_";

  static const char *FEATURE[] = {
    "CRC32",
    "FlagM",
    "FlagM2",
    "FHM",
    "DotProd",
    "SHA3",
    "RDM",
    "LSE",
    "SHA256",
    "SHA512",
    "SHA1",
    "AES",
    "PMULL",
    "SPECRES",
    "SPECRES2",
    "SB",
    "FRINTTS",
    "PACIMP",
    "LRCPC",
    "LRCPC2",
    "FCMA",
    "JSCVT",
    "PAuth",
    "PAuth2",
    "FPAC",
    "FPACCOMBINE",
    "DPB",
    "DPB2",
    "BF16",
    "EBF16",
    "I8MM",
    "WFxT",
    "RPRES",
    "CSSC",
    "HBC",
    "ECV",
    "AFP",
    "LSE2",
    "CSV2",
    "CSV3",
    "DIT",
    "FP16",
    "SSBS",
    "BTI",
    "SME",
    "SME2",
    "SME_F64F64",
    "SME_I16I64"
  };

  int N = (sizeof(FEATURE) / sizeof(char*));

  char *info = calloc( N, 16 );
  if( info == NULL ) {
    return NULL;
  }

  char *wp = info;

  char namebuf[sizeof(hw_optional_arm_FEAT_)+16];
  strcpy( namebuf, hw_optional_arm_FEAT_ );
  size_t size = sizeof(uint64_t);
  const char **cursor = FEATURE;
  for( int n=0; n<N; n++ ) {
    const char *feat = *cursor++;
    char *fp = namebuf + sizeof(hw_optional_arm_FEAT_) - 1;
    strncpy( fp, feat, 15 );
    uint64_t flag = 0;
    sysctlbyname( namebuf, &flag, &size, NULL, 0 );
    if( flag ) {
      const char *rp = feat;
      while( *rp != '\0' ) {
        *wp++ = *rp++;
      }
      *wp++ = ' ';
    }
  }
  *wp = '\0';
  return info;

}
#endif


/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
#if defined CXPLAT_ARCH_X64
int get_cpu_cores( int *cores, int *threads ) {
  __cpuregs_t regs = {0};

  static const unsigned int ext_top_enum_leaf = 0x0B;
  static const int level_type_Invalid = 0;
  static const int level_type_SMT = 1;
  static const int level_type_Core = 2;
  int level_type;
  int smt_count = 0;
  int core_count = 0;
  unsigned int i = 0;
  // Find level type = Core
  do {
    cxplat_cpuidex( ext_top_enum_leaf, i, &regs.eax, &regs.ebx, &regs.ecx, &regs.edx );
    level_type = (regs.ecx >> 8) & 0xFF;
    int count =  regs.ebx & 0xFFFF;
    if( level_type == level_type_SMT ) {
      smt_count = count;
    }
    else if( level_type == level_type_Core ) {
      core_count = count;
    }
  } while( level_type != level_type_Invalid && level_type != level_type_Core && ++i < 256 );

  if( level_type == level_type_Core ) {
    *cores = core_count / smt_count;
    *threads = core_count;
    return 0;
  }
  else {
    return -1;
  }
  return 0;
}
#elif defined CXPLAT_ARCH_ARM64
int get_cpu_cores( int *P_cores, int *E_cores ) {
  uint64_t core_count = 0;
  size_t size = sizeof(core_count);

  if( P_cores ) {
    if( sysctlbyname( "hw.perflevel0.physicalcpu", &core_count, &size, NULL, 0 ) != 0 ) {
      sysctlbyname( "hw.physicalcpu", &core_count, &size, NULL, 0 );
    }
    *P_cores = (int)core_count;
  }

  if( E_cores ) {
    sysctlbyname( "hw.perflevel1.physicalcpu", &core_count, &size, NULL, 0 );
    *E_cores = (int)core_count;
  }
  return 0;
}
#else
#error "Unsupported platform"
#endif



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
int get_cpu_L2_size( void ) {
#if defined CXPLAT_ARCH_X64
  __cpuregs_t regs = {0};
  // Get max extended index
  cxplat_cpuid( 0x80000000, &regs.eax, &regs.ebx, &regs.ecx, &regs.edx );
  if( regs.eax & 0x80000000 ) {
    unsigned cache_leaf = 0x80000006;
    unsigned last = regs.eax;
    if( last >= cache_leaf ) {
      cxplat_cpuid( cache_leaf, &regs.eax, &regs.ebx, &regs.ecx, &regs.edx );
      return regs.ecx >> 16;
    }
  }
#elif defined CXPLAT_ARCH_ARM64
  uint64_t l2_cache_size = 0;
  size_t size = sizeof(l2_cache_size);

  if( sysctlbyname( "hw.perflevel0.l2cachesize", &l2_cache_size, &size, NULL, 0 ) == 0 ) {
    return (int)l2_cache_size;
  }
  if (sysctlbyname("hw.l2cachesize", &l2_cache_size, &size, NULL, 0) == 0) {
    return (int)l2_cache_size;
  }
#endif
  return -1;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
int get_cpu_L2_associativity( void ) {
#if defined CXPLAT_ARCH_X64
  __cpuregs_t regs = {0};
  // Get max extended index
  cxplat_cpuid( 0x80000000, &regs.eax, &regs.ebx, &regs.ecx, &regs.edx );
  if( regs.eax & 0x80000000 ) {
    unsigned cache_leaf = 0x80000006;
    unsigned last = regs.eax;
    if( last >= cache_leaf ) {
      cxplat_cpuid( cache_leaf, &regs.eax, &regs.ebx, &regs.ecx, &regs.edx );

      switch( (regs.ecx >> 12) & 0xF ) {
      case 0x01:
        return 1; // direct
        break;
      case 0x02:
        return 2; // 2 ways
        break;
      case 0x04:
        return 4; // 4 ways
        break;
      case 0x06:
        return 8; // 8 ways
        break;
      case 0x08:
        return 16; // 16 ways
        break;
      case 0x0A:
        return 32; // 32 ways
        break;
      case 0x0B:
        return 48; // 48 ways
        break;
      case 0x0C:
        return 64; // 64 ways
        break;
      case 0x0D:
        return 96; // 96 ways
        break;
      case 0x0E:
        return 128; // 128 ways
        break;
      case 0x0F:
        return 0xFFFF; // fully associative
        break;
      default:
        return 0;
      }
    }
  }
#elif defined CXPLAT_ARCH_ARM64
  // TODO
  return 1;
#endif

  return -1;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
char * get_new_cpu_cache_info( void ) {
  int64_t remain = 1023;
  char *buffer = calloc( 1, remain+1 );
  if( buffer == NULL ) {
    return NULL;
  }

#if defined CXPLAT_ARCH_X64
  char *p = buffer;

  __cpuregs_t regs = {0};

  int n_cores = 0;
  int n_threads = 0;
  get_cpu_cores( &n_cores, &n_threads);

  int cache_leaf = 0x04;
  for( int cache_idx = 0; cache_idx < 5; cache_idx++ ) {
    cxplat_cpuidex( cache_leaf, cache_idx, &regs.eax, &regs.ebx, &regs.ecx, &regs.edx );

    char Lx[] = "Lxx ";

    // ----------------------------
    // EAX
    // ----------------------------
    // Bits 04 - 00: Cache Type Field.
    int cache_type = regs.eax & 0x1F;
    // 0 = Null - No more caches.
    if( cache_type == 0 ) {
      break;
    }
    // 1 = Data Cache. 
    else if( cache_type == 1 ) {
      Lx[2] = 'D';
    }
    // 2 = Instruction Cache.
    else if( cache_type == 2 ) {
      Lx[2] = 'I';
    }
    // 3 = Unified Cache
    else if( cache_type == 3 ) {
      Lx[2] = ' ';
    }
    // 4-31 = Reserved
    else {
    }

    // Bits 07 - 05: Cache Level (starts at 1). 
    int cache_level = (regs.eax >> 5) & 0x7;
    Lx[1] = '0' + (char)cache_level;

    // Bit 08: Self Initializing cache level (does not need SW initialization).
    //int cache_selfinit = (regs.eax >> 8) & 1;

    // Bit 09: Fully Associative cache.
    //int cache_fully_associative = (regs.eax >> 9) & 1;

    // Bits 13 - 10: Reserved.
    // Bits 25 - 14: Maximum number of addressable IDs for logical processors sharing this cache**,  ***.
    // Bits 31 - 26: Maximum number of addressable IDs for processor cores in the physical package**,  ****,  *****.

    // ----------------------------
    // EBX
    // ----------------------------
    // Bits 11 - 00: L = System Coherency Line Size**
    int cache_linesize = (regs.ebx & 0xFFF) + 1;

    // Bits 21 - 12: P = Physical Line partitions**.
    int cache_partitions = ((regs.ebx >> 12 ) & 0x3FF) + 1;

    // Bits 31 - 22: W = Ways of associativity**.
    int cache_ways = ((regs.ebx >> 22) & 0x3FF) + 1;

    // ----------------------------
    // ECX
    // ----------------------------
    // Bits 31-00: S = Number of Sets**.
    int cache_sets = (regs.ecx & 0x7FFFFFFF) + 1;

    // ----------------------------
    // EDX
    // ----------------------------
    // Bit 00: Write-Back Invalidate/Invalidate.
    //  0 = WBINVD/INVD from threads sharing this cache acts upon lower level caches for threads sharing this cache.
    //  1 = WBINVD/INVD is not guaranteed to act upon lower level caches of non-originating threads sharing this cache.
    //int cache_wbinvd = (regs.edx & 1);

    // Bit 01: Cache Inclusiveness.
    //  0 = Cache is not inclusive of lower cache levels.
    //  1 = Cache is inclusive of lower cache levels.
    //int cache_inclusive = (regs.edx >> 1) & 1;

    // Bit 02: Complex Cache Indexing.
    //  0 = Direct mapped cache.
    //  1 = A complex function is used to index the cache, potentially using all address bits.
    //int cache_complex = (regs.edx >> 2) & 1;

    // Bits 31 - 03: Reserved = 0.

    // NOTES:
    // *     If ECX contains an invalid sub leaf index,EAX/EBX/ECX/EDX return 0. Sub-leaf index n+1 is invalid if sub-leaf n returns EAX[4:0] as 0.
    // **    Add one to the return value to get the result.
    // ***   The nearest power-of-2 integer that is not smaller than(1 + EAX[25:14]) is the number of unique ini-tial APIC IDs reserved for addressing different logical processors sharing this cache.
    // ****  The nearest power-of-2 integer that is not smaller than(1 + EAX[31:26]) is the number of unique Core_IDs reserved for addressing different processor cores in a physical package. Core ID is a subset of bits of the initial APIC ID. 
    // ***** The returned value is constant for valid initial values in ECX. Valid ECX values start from 0.


    int cache_bytes = cache_sets * cache_ways * cache_partitions * cache_linesize;
    if( remain > 32 ) {
      int c_kib = cache_bytes >> 10;
      char u[] = "ki";
      int used;
      // TODO: Find a way to determine automatically which caches are associated with cores.
      //       Now we just assume L1 and L2 are per core and anything greater is shared.
      if( cache_level < 3 && n_cores > 0 ) {
        used = snprintf( p, remain, "%s : %5d %sB %d-way (x %d)\n", Lx, c_kib, u, cache_ways, n_cores );
      }
      else {
        used = snprintf( p, remain, "%s : %5d %sB %d-way\n", Lx, c_kib, u, cache_ways );
      }
      if( used > 0 ) {
        remain -= used;
        p += used;
      }
    }

  }

#elif defined CXPLAT_ARCH_ARM64
  /*
     L3   : 25600 kiB 10-way
     L2   :  1280 kiB 10-way (x 10)
     L1I  :    32 kiB 8-way (x 10)
     L1D  :    48 kiB 12-way (x 10)
  */
  char *p = buffer;

  size_t size = sizeof(uint64_t);
  uint64_t P_cpu_count = 0;
  uint64_t P_l2_size = 0;
  uint64_t P_l1i_size = 0;
  uint64_t P_l1d_size = 0;
  uint64_t E_cpu_count = 0;
  uint64_t E_l2_size = 0;
  uint64_t E_l1i_size = 0;
  uint64_t E_l1d_size = 0;

  sysctlbyname( "hw.perflevel0.physicalcpu", &P_cpu_count, &size, NULL, 0 );
  sysctlbyname( "hw.perflevel0.l2cachesize", &P_l2_size, &size, NULL, 0 );
  sysctlbyname( "hw.perflevel0.l1icachesize", &P_l1i_size, &size, NULL, 0 );
  sysctlbyname( "hw.perflevel0.l1dcachesize", &P_l1d_size, &size, NULL, 0 );
  if( !P_cpu_count || !P_l2_size || !P_l1i_size || !P_l1d_size ) {
    sysctlbyname( "hw.physicalcpu", &P_cpu_count, &size, NULL, 0 );
    sysctlbyname( "hw.l2cachesize", &P_l2_size, &size, NULL, 0 );
    sysctlbyname( "hw.l1icachesize", &P_l1i_size, &size, NULL, 0 );
    sysctlbyname( "hw.l1dcachesize", &P_l1d_size, &size, NULL, 0 );
  }
  else {
    sysctlbyname( "hw.perflevel1.physicalcpu", &E_cpu_count, &size, NULL, 0 );
    sysctlbyname( "hw.perflevel1.l2cachesize", &E_l2_size, &size, NULL, 0 );
    sysctlbyname( "hw.perflevel1.l1icachesize", &E_l1i_size, &size, NULL, 0 );
    sysctlbyname( "hw.perflevel1.l1dcachesize", &E_l1d_size, &size, NULL, 0 );
  }

  // bytes -> kiB
  P_l2_size >>= 10;
  P_l1i_size >>= 10;
  P_l1d_size >>= 10;
  E_l2_size >>= 10;
  E_l1i_size >>= 10;
  E_l1d_size >>= 10;

  int used = snprintf( p, remain, 
    "L1D  : %5llu kiB (x %lluP) %llu kiB (x %lluE)\n"
    "L1I  : %5llu kiB (x %lluP) %llu kiB (x %lluE)\n"
    "L2   : %5llu kiB (P) %llu kiB (E)",
            P_l1d_size, P_cpu_count, E_l1d_size, E_cpu_count,
            P_l1i_size, P_cpu_count, E_l1i_size, E_cpu_count,
            P_l2_size, E_l2_size
          );
  

#endif
  
  return buffer;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
char * get_new_cpu_tlb_info( void ) {
  int n_cores = 0;
  int n_threads = 0;
  get_cpu_cores( &n_cores, &n_threads);

  int64_t remain = 1023;
  char *buffer = calloc( 1, remain+1 );
  if( buffer == NULL ) {
    return NULL;
  }
  
  char *p = buffer;
  DWORD pageSize = 0;
  int used = 0;

#if defined CXPLAT_ARCH_X64

  __cpuregs_t regs = {0};

  cxplat_cpuid( 0x02, &regs.eax, &regs.ebx, &regs.ecx, &regs.edx );


#ifdef CXPLAT_WINDOWS_X64
  SYSTEM_INFO si;
  GetSystemInfo( &si );
  pageSize = si.dwPageSize;
#else
  pageSize = sysconf(_SC_PAGESIZE);
#endif

  struct _tlb {
    const char *name;
    int ways;
    int entries;
  };
  struct _tlb ITLB = {"ITLB",0,0};
  struct _tlb DTLB = {"DTLB",0,0};
  struct _tlb STLB = {"STLB",0,0};

  for( int i=0; i<4; i++ ) {
    int exx = regs.data[i];
    // MSB=1, ignore register
    if( exx & 0x8000000 ) {
      continue;
    }
    for( int b=0; b<4; b++ ) {
      // Ignore low byte in EAX (always 01h)
      if( i==0 && b==0 ) {
        continue;
      }
      BYTE desc = (exx >> (8*b)) & 0xFF;

      // leaf 02h does not report TLB info, use leaf 18h
      if( desc == 0xFE ) {
        goto leaf_18h;
      }

      if( pageSize == 4096 ) {
        // 4k page size TLB-relevant descriptors
        switch( desc ) {
        //
        case 0x01:
          ITLB.ways = 4;
          ITLB.entries = 32;
          break;
        //
        case 0x03:
          DTLB.ways = 4;
          DTLB.entries = 64;
          break;
        //
        case 0x4F:
          ITLB.entries = 32;
          break;
        //
        case 0x50:
          ITLB.entries = 64;
          break;
        //
        case 0x51:
          ITLB.entries = 128;
          break;
        //
        case 0x52:
          ITLB.entries = 256;
          break;
        //
        case 0x57:
          DTLB.ways = 4;
          DTLB.entries = 16;
          break;
        //
        case 0x59:
          DTLB.ways = -1; // fully associative
          DTLB.entries = 16;
          break;
        //
        case 0x5B:
          DTLB.entries = 64;
          break;
        //
        case 0x5C:
          DTLB.entries = 128;
          break;
        //
        case 0x5D:
          DTLB.entries = 256;
          break;
        //
        case 0x61:
          ITLB.ways = -1; // fully associative
          ITLB.entries = 48;
          break;
        //
        case 0x64:
          DTLB.ways = 4;
          DTLB.entries = 512;
          break;
        //
        case 0xA0:
          DTLB.ways = -1; // fully associative
          DTLB.entries = 32;
          break;
        //
        case 0xB0:
          ITLB.ways = 4;
          ITLB.entries = 128;
          break;
        //
        case 0xB2:
          ITLB.ways = 4;
          ITLB.entries = 64;
          break;
        //
        case 0xB3:
          DTLB.ways = 4;
          DTLB.entries = 128;
          break;
        //
        case 0xB4:
          DTLB.ways = 4;
          DTLB.entries = 256;
          break;
        //
        case 0xB5:
          ITLB.ways = 8;
          ITLB.entries = 64;
          break;
        //
        case 0xB6:
          ITLB.ways = 8;
          ITLB.entries = 128;
          break;
        //
        case 0xBA:
          DTLB.ways = 4;
          DTLB.entries = 64;
          break;
        //
        case 0xC0:
          DTLB.ways = 4;
          DTLB.entries = 8;
          break;
        //
        case 0xC1:
          STLB.ways = 8;
          STLB.entries = 1024;
          break;
        //
        case 0xC2:
          DTLB.ways = 4;
          DTLB.entries = 16;
          break;
        //
        case 0xC3:
          STLB.ways = 6;
          STLB.entries = 1536;
          break;
        //
        case 0xCA:
          STLB.ways = 4;
          STLB.entries = 512;
          break;
        //
        default:
          break;
        }
      }
      else if( pageSize == 2097152 || pageSize == 4194304 ) {
        // 2MiB and 4MiB page size TLB-relevant descriptors
        switch( desc ) {
        //
        case 0x02:
          ITLB.ways = -1; // fully associative
          ITLB.entries = 2;
          break;
        //
        case 0x04:
          DTLB.ways = 4;
          DTLB.entries = 8;
          break;
        //
        case 0x05:
          DTLB.ways = 4;
          DTLB.entries = 32;
          break;
        //
        case 0x0B:
          ITLB.ways = 4;
          ITLB.entries = 4;
          break;
        //
        case 0x50:
          ITLB.entries = 64;
          break;
        //
        case 0x51:
          ITLB.entries = 128;
          break;
        //
        case 0x52:
          ITLB.entries = 256;
          break;
        //
        case 0x55:
          ITLB.ways = -1; // fully associative
          ITLB.entries = 7;
          break;
        //
        case 0x56:
          DTLB.ways = 4;
          DTLB.entries = 16;
          break;
        //
        case 0x5A:
          DTLB.ways = 4;
          DTLB.entries = 32;
          break;
        //
        case 0x5B:
          DTLB.entries = 64;
          break;
        //
        case 0x5C:
          DTLB.entries = 128;
          break;
        //
        case 0x5D:
          DTLB.entries = 256;
          break;
        //
        case 0x63:
          DTLB.ways = 4;
          DTLB.entries = 32;
          break;
        //
        case 0x76:
          ITLB.ways = -1; // fully associative
          ITLB.entries = 8;
          break;
        //
        case 0xB1:
          ITLB.ways = 4;
          if( pageSize == 2097152 ) {
            ITLB.entries = 8;
          }
          else {
            ITLB.entries = 4;
          }
          break;
        //
        case 0xC0:
          DTLB.ways = 4;
          DTLB.entries = 8;
          break;
        //
        case 0xC1:
          STLB.ways = 8;
          STLB.entries = 1024;
          break;
        //
        case 0xC2:
          DTLB.ways = 4;
          DTLB.entries = 16;
          break;
        //
        case 0xC3:
          STLB.ways = 6;
          STLB.entries = 1536;
          break;
        //
        case 0xC4:
          DTLB.ways = 4;
          DTLB.entries = 32;
          break;
        //
        default:
          break;
        }
      }
      else if( pageSize == 1073741824 ) {
        // 1GiB page size TLB-relevant descriptors
        switch( desc ) {
        case 0x63:
          DTLB.ways = 4;
          DTLB.entries = 4;
          break;
        //
        case 0xC3:
          STLB.ways = 4;
          STLB.entries = 16;
          break;
        //
        case 0xC4:
          DTLB.ways = 4;
          DTLB.entries = 32;
          break;
        //
        default:
          break;
        }
      }
    }
  } 

  goto write_tlb_info;


leaf_18h:

  ;

  /*  NOTES:
      Each sub-leaf enumerates a different address translation structure. 
      
      If ECX contains an invalid sub-leaf index, EAX/EBX/ECX/EDX return 0.
      Sub-leaf index n is invalid if n exceeds the value that sub-leaf 0 returns in EAX.
      A sub-leaf index is also invalid if EDX[4:0] returns 0.
      Valid sub-leaves do not need to be contiguous or in any particular order.
      A valid sub-leaf may be in a higher input ECX value than an invalid sub-leaf
      or than a valid sub-leaf of a higher or lower-level structure.
      
      *   Some unified TLBs will allow a single TLB entry to satisfy data read/write and
          instruction fetches. Others will require separate entries (e.g., one loaded on
          data read/write and another loaded on an instruction fetch) . Please see the
          Intel 64 and IA-32 Architectures Optimization Reference Manual for details of
          a particular product.
      **  Add one to the return value to get the result. 
  */

  int tlb_leaf = 0x18;
  // Subleaf 0, get max subleaf into eax
  cxplat_cpuidex( tlb_leaf, 0, &regs.eax, &regs.ebx, &regs.ecx, &regs.edx );

  // Invalid
  if( (regs.edx & 0x1F) == 0 ) {
    // No way to get TLB info
    free( buffer );
    return NULL;
  }

  // ----------------------------
  // EAX
  // ----------------------------
  // Subleaf (ECX) 0:
  //    Bits 31 - 00: Reports the maximum input value of supported sub-leaf in leaf 18H.
  int tlb_idx_max = regs.eax;

  // All subleaves, starting at 0
  for( int tlb_idx = 0; tlb_idx <= tlb_idx_max; tlb_idx++ ) {

    // Subleaf n
    cxplat_cpuidex( tlb_leaf, tlb_idx, &regs.eax, &regs.ebx, &regs.ecx, &regs.edx );

    // ----------------------------
    // EBX
    // ----------------------------
    //    Bit 00: 4K page size entries supported by this structure.
    //    Bit 01: 2MB page size entries supported by this structure.
    //    Bit 02: 4MB page size entries supported by this structure.
    //    Bit 03: 1 GB page size entries supported by this structure.
    //    Bits 07 - 04: Reserved.
    //    Bits 10 - 08: Partitioning(0: Soft partitioning between the logical processors sharing this structure).
    //    Bits 15 - 11: Reserved.
    //    Bits 31 - 16: W = Ways of associativity.
    int W = (regs.ebx >> 16) & 0xFFFF;


    // ----------------------------
    // ECX
    // ----------------------------
    //    Bits 31 - 00: S = Number of Sets.
    int S = regs.ecx;


    // ----------------------------
    // EDX
    // ----------------------------
    //    Bits 04 - 00: Translation cache type field.
    int tlb_type = regs.edx & 0x1F;

    //    Bits 07 - 05: Translation cache level(starts at 1).
    int tlb_level = (regs.edx >> 5) & 0x7;

    //    Bit 08: Fully associative structure.
    if( regs.edx & 0x100 ) {
      W = -1; // fully associative
    }
    //    Bits 13 - 09: Reserved.
    //    Bits 25- 14: Maximum number of addressable IDs for logical processors sharing this translation cache**
    // 
    //    Bits 31 - 26: Reserved.

    switch( tlb_type ) {
    //      00001b: Data TLB.
    case 1:
      if( tlb_level == 1 ) {
        DTLB.ways = W;
        DTLB.entries = S;
      }
      else {
        // ??
      }
      break;
    //      00010b: Instruction TLB.
    case 2:
      if( tlb_level == 1 ) {
        ITLB.ways = W;
        ITLB.entries = S;
      }
      break;
    //      00011b: Unified TLB*.
    case 3:
      if( tlb_level == 2 ) {
        STLB.ways = W;
        STLB.entries =S;
      }
      break;
    //      00000b: Null(indicates this sub-leaf is not valid).
    //      All other encodings are reserved.
    default:
      continue;
    }
  }


write_tlb_info:

  ;

  struct _tlb *TLBs[] = { &ITLB, &DTLB, &STLB };

  for( int i=0; i<3; i++ ) {
    if( remain > 32 ) {
      if( TLBs[i]->ways < 0 ) {
        used = snprintf( p, remain, "%s : %5d sets fully associative (x %d)\n", TLBs[i]->name, TLBs[i]->entries, n_cores );
      }
      else {
        used = snprintf( p, remain, "%s : %5d sets %d-way (x %d)\n", TLBs[i]->name, TLBs[i]->entries, TLBs[i]->ways, n_cores );
      }
      if( used > 0 ) {
        remain -= used;
        p += used;
      }
    }
  }

#elif defined CXPLAT_ARCH_ARM64
  pageSize = sysconf(_SC_PAGESIZE);

  // We have no access to TLB info

#endif
  // Finally, page size
  if( remain > 32 ) {
    used = snprintf( p, remain, "PAGE : %5d kiB\n", pageSize >> 10 );
    remain -= used;
    p += used;
  }
 
  return buffer;
}


#if defined(CXPLAT_LINUX_ANY)
/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
char * get_error_reason( int __errnum, char *__buf, size_t __buflen ) {
  return strerror_r( __errnum, __buf, __buflen );
}

#elif defined(CXPLAT_MAC_ARM64)
/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
char * get_error_reason( int __errnum, char *__buf, size_t __buflen ) {
  if( strerror_r( __errnum, __buf, __buflen ) != 0 ) {
    strncpy( __buf, "unknown error", __buflen-1 );
  }
  return __buf;
}

#endif
