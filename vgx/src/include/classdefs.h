/*
###################################################
#
# File:   classdefs.h
#
###################################################
*/
#ifndef COMLIB_CLASSDEFS_H
#define COMLIB_CLASSDEFS_H





/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef enum e_object_class_t {
  CLASS_NONE                          = 0x00, //

  /* 0x80 - 0x8F (foundation) */
  CLASS_DemoClass_t                   = 0x80, //    DemoClass_t
  CLASS_cxmalloc_family_t             = 0x81, //    cxmalloc_family_t
  CLASS__0x82                         = 0x82, //
  CLASS__0x83                         = 0x83, //
  CLASS__0x84                         = 0x84, //
  CLASS__0x85                         = 0x85, //
  CLASS_CTokenizer_t                  = 0x86, //    CTokenizer_t
  CLASS_CString_t                     = 0x87, //    CString_t
  CLASS__0x88                         = 0x88, //
  CLASS__0x89                         = 0x89, //
  CLASS_Text_t                        = 0x8A, //    Text_t
  CLASS__0x8B                         = 0x8B, //
  CLASS__0x8C                         = 0x8C, //
  CLASS__0x8D                         = 0x8D, //
  CLASS__0x8E                         = 0x8E, //
  CLASS__0x8F                         = 0x8F, //

  /* 0x90 (Base Linear Sequence) */
  CLASS_ComlibLinearSequence_t        = 0x90, //    Base Sequence

  /* 0x91 - 0x9B (Queues) */
  CLASS_CStringQueue_t                = 0x91, //    CStringQueue_t
  CLASS_CByteQueue_t                  = 0x92, //    CByteQueue_t
  CLASS_CWordQueue_t                  = 0x93, //    CWordQueue_t
  CLASS_CDwordQueue_t                 = 0x94, //    CDwordQueue_t
  CLASS_CQwordQueue_t                 = 0x95, //    CQwordQueue_t
  CLASS_Cm128iQueue_t                 = 0x96, //    Cm128iQueue_t
  CLASS_Cm256iQueue_t                 = 0x97, //    Cm256iQueue_t
  CLASS_Cm512iQueue_t                 = 0x98, //    Cm512iQueue_t
  CLASS_CtptrQueue_t                  = 0x99, //    CtptrQueue_t
  CLASS_Cx2tptrQueue_t                = 0x9A, //    Cx2tptrQueue_t
  CLASS_CaptrQueue_t                  = 0x9B, //    CaptrQueue_t

  /* 0x9B - 0xA6 (Buffers) */
  CLASS_CStringBuffer_t               = 0x9C, //    CStringBuffer_t
  CLASS_CByteBuffer_t                 = 0x9D, //    CByteBuffer_t
  CLASS_CWordBuffer_t                 = 0x9E, //    CWordBuffer_t
  CLASS_CDwordBuffer_t                = 0x9F, //    CDwordBuffer_t
  CLASS_CQwordBuffer_t                = 0xA0, //    CQwordBuffer_t
  CLASS_Cm128iBuffer_t                = 0xA1, //    Cm128iBuffer_t
  CLASS_Cm256iBuffer_t                = 0xA2, //    Cm256iBuffer_t
  CLASS_Cm512iBuffer_t                = 0xA3, //    Cm512iBuffer_t
  CLASS_CtptrBuffer_t                 = 0xA4, //    CtptrBuffer_t
  CLASS_Cx2tptrBuffer_t               = 0xA5, //    Cx2tptrBuffer_t
  CLASS_CaptrBuffer_t                 = 0xA6, //    CaptrBuffer_t

  /* 0xA7 - 0xB0 (Heaps) */
  CLASS_CByteHeap_t                   = 0xA7, //    CByteHeap_t    
  CLASS_CWordHeap_t                   = 0xA8, //    CWordHeap_t    
  CLASS_CDwordHeap_t                  = 0xA9, //    CDwordHeap_t    
  CLASS_CQwordHeap_t                  = 0xAA, //    CQwordHeap_t    
  CLASS_Cm128iHeap_t                  = 0xAB, //    Cm128iHeap_t
  CLASS_Cm256iHeap_t                  = 0xAC, //    Cm256iHeap_t
  CLASS_Cm512iHeap_t                  = 0xAD, //    Cm512iHeap_t
  CLASS_CtptrHeap_t                   = 0xAE, //    CtptrHeap_t
  CLASS_Cx2tptrHeap_t                 = 0xAF, //    Cx2tptrHeap_t
  CLASS_CaptrHeap_t                   = 0xB0, //    CaptrHeap_t

  /* 0xB1 - 0xBA (Lists)*/
  CLASS_CByteList_t                   = 0xB1, //    CByteList_t
  CLASS_CWordList_t                   = 0xB2, //    CWordList_t
  CLASS_CDwordList_t                  = 0xB3, //    CDwordList_t
  CLASS_CQwordList_t                  = 0xB4, //    CQwordList_t
  CLASS_Cm128iList_t                  = 0xB5, //    Cm128iList_t
  CLASS_Cm256iList_t                  = 0xB6, //    Cm256iList_t
  CLASS_Cm512iList_t                  = 0xB7, //    Cm512iList_t
  CLASS_CtptrList_t                   = 0xB8, //    CtptrList_t
  CLASS_Cx2tptrList_t                 = 0xB9, //    Cx2tptrList_t
  CLASS_CaptrList_t                   = 0xBA, //    CaptrList_t

  /* 0xBB - 0xBF (reserved) */
  CLASS__0xBB                         = 0xBB, //
  CLASS__0xBC                         = 0xBC, //
  CLASS__0xBD                         = 0xBD, //
  CLASS__0xBE                         = 0xBE, //
  CLASS__0xBF                         = 0xBF, //

  /* 0xC0 - 0xC7 (framehash) */
  CLASS_framehash_t                   = 0xC0, //    Framehash_t
  CLASS_FramehashTestObject_t         = 0xC1, //    FramehashTestObject_t
  CLASS_PyFramehashObjectWrapper_t    = 0xC2, //    PyFramehashObjectWrapper_t
  CLASS__0xC3                         = 0xC3, //
  CLASS__0xC4                         = 0xC4, //
  CLASS__0xC5                         = 0xC5, //
  CLASS__0xC6                         = 0xC6, //
  CLASS__0xC7                         = 0xC7, //

  /* 0xC8 - 0xCF (graph structure) */
  CLASS_vgx_Vector_t                  = 0xC8, //    vgx_Vector_t
  CLASS_vgx_Fingerprinter_t           = 0xC9, //    vgx_Fingerprinter_t
  CLASS_vgx_Similarity_t              = 0xCA, //    vgx_Similarity_t
  CLASS_vgx_Graph_t                   = 0xCB, //    vgx_Graph_t
  CLASS_vgx_Vertex_t                  = 0xCC, //    vgx_Vertex_t
  CLASS_vgx_IndexEntry_t              = 0xCD, //    vgx_IndexEntry_t
  CLASS_vgx_ArrayVector_t             = 0xCE, //    vgx_ArrayVector_t
  CLASS_vgx_FeatureVector_t           = 0xCF, //    vgx_FeatureVector_t

  /* 0xD0 - 0xDF (graph query) */
  CLASS_vgx_Evaluator_t               = 0xD0, //    vgx_Evaluator_t
  CLASS_vgx_BaseQuery_t               = 0xD1, //    vgx_BaseQuery_t
  CLASS_vgx_AdjacencyQuery_t          = 0xD2, //    vgx_AdjacencyQuery_t
  CLASS_vgx_NeighborhoodQuery_t       = 0xD3, //    vgx_NeighborhoodQuery_t
  CLASS_vgx_GlobalQuery_t             = 0xD4, //    vgx_GlobalQuery_t
  CLASS_vgx_AggregatorQuery_t         = 0xD5, //    vgx_AggregatorQuery_t
  CLASS__0xD6                         = 0xD6, //
  CLASS__0xD7                         = 0xD7, //
  CLASS__0xD8                         = 0xD8, //
  CLASS__0xD9                         = 0xD9, //
  CLASS__0xDA                         = 0xDA, //
  CLASS__0xDB                         = 0xDB, //
  CLASS__0xDC                         = 0xDC, //
  CLASS__0xDD                         = 0xDD, //
  CLASS__0xDE                         = 0xDE, //
  CLASS__0xDF                         = 0xDF, //

  /* 0xE0 - 0xEF (reserved) */
  CLASS__0xE0                         = 0xE0, //
  CLASS__0xE1                         = 0xE1, //
  CLASS__0xE2                         = 0xE2, //
  CLASS__0xE3                         = 0xE3, //
  CLASS__0xE4                         = 0xE4, //
  CLASS__0xE5                         = 0xE5, //
  CLASS__0xE6                         = 0xE6, //
  CLASS__0xE7                         = 0xE7, //
  CLASS__0xE8                         = 0xE8, //
  CLASS__0xE9                         = 0xE9, //
  CLASS__0xEA                         = 0xEA, //
  CLASS__0xEB                         = 0xEB, //
  CLASS__0xEC                         = 0xEC, //
  CLASS__0xED                         = 0xED, //
  CLASS__0xEE                         = 0xEE, //
  CLASS__0xEF                         = 0xEF, //
  
  /* 0xF0 - 0xFE (reserved) */
  CLASS__0xF0                         = 0xF0, //
  CLASS__0xF1                         = 0xF1, //
  CLASS__0xF2                         = 0xF2, //
  CLASS__0xF3                         = 0xF3, //
  CLASS__0xF4                         = 0xF4, //
  CLASS__0xF5                         = 0xF5, //
  CLASS__0xF6                         = 0xF6, //
  CLASS__0xF7                         = 0xF7, //
  CLASS__0xF8                         = 0xF8, //
  CLASS__0xF9                         = 0xF9, //
  CLASS__0xFA                         = 0xFA, //
  CLASS__0xFB                         = 0xFB, //
  CLASS__0xFC                         = 0xFC, //
  CLASS__0xFD                         = 0xFD, //
  CLASS__0xFE                         = 0xFE, //

  /* 0xFF */
  CLASS_INVALID                       = 0xFF, //

} object_class_t;


typedef struct s_object_classname_t {
  char classname[32];
  object_class_t typecode;
} object_classname_t;

#define COMLIB_MAX_CLASS 256
DLL_COMLIB_PUBLIC extern object_classname_t OBJECT_CLASSNAME[ COMLIB_MAX_CLASS ];


#endif

