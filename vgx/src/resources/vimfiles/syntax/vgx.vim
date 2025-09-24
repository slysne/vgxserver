" Vim syntax file
" Language: VGX Transaction
" Maintainer: Stian Lysne
" Latest Revision: 11 May 2022

if exists("b:current_syntax")
  finish
endif

syn match   vgxComment                  "\(#.*\|\s\)$"

"ATTACH
syn keyword vgxKeyword_ATTACH       ATTACH      nextgroup=vgxProtocol_ATTACH skipwhite
syn match   vgxProtocol_ATTACH          "[A-F0-9]\{8}" contained nextgroup=vgxVersion_ATTACH skipwhite
syn match   vgxVersion_ATTACH           "[A-F0-9]\{8}" contained nextgroup=vgxDigest_ATTACH skipwhite
syn match   vgxDigest_ATTACH            "[a-f0-9]\{32}" contained nextgroup=vgxAdminPort_ATTACH skipwhite
syn match   vgxAdminPort_ATTACH         "[A-F0-9]\{4}" contained nextgroup=vgxComment skipwhite

"ACCEPTED
syn keyword vgxKeyword_ACCEPTED     ACCEPTED    nextgroup=vgxTransID_ACCEPTED skipwhite
syn match   vgxTransID_ACCEPTED         "[a-f0-9]\{32}" contained nextgroup=vgxCRC_ACCEPTED skipwhite
syn match   vgxCRC_ACCEPTED             "[A-F0-9]\{8}"  contained nextgroup=vgxComment skipwhite

"IDLE
syn keyword vgxKeyword_IDLE         IDLE        nextgroup=vgxTMS_IDLE skipwhite
syn match   vgxTMS_IDLE                 "[A-F0-9]\{16}" contained nextgroup=vgxDigest_IDLE skipwhite
syn match   vgxDigest_IDLE              "[a-f0-9]\{32}" contained nextgroup=vgxComment skipwhite

"RESYNC
syn keyword vgxKeyword_RESYNC       RESYNC      nextgroup=vgxTransID_RESYNC skipwhite
syn match   vgxTransID_RESYNC           "[a-f0-9]\{32}" contained nextgroup=vgxNDiscard_RESYNC skipwhite
syn match   vgxNDiscard_RESYNC          "[A-F0-9]\{16}" contained nextgroup=vgxComment skipwhite

"RETRY
syn keyword vgxKeyword_RETRY        RETRY       nextgroup=vgxTransID_RETRY skipwhite
syn match   vgxTransID_RETRY            "[a-f0-9]\{32}" contained nextgroup=vgxReason_RETRY skipwhite
syn match   vgxReason_RETRY             "[A-F0-9]\{8}"  contained nextgroup=vgxComment skipwhite

"SUSPEND
syn keyword vgxKeyword_SUSPEND      SUSPEND     nextgroup=vgxCode_SUSPEND skipwhite
syn match   vgxCode_SUSPEND             "[A-F0-9]\{4}"  contained nextgroup=vgxMS_SUSPEND skipwhite
syn match   vgxMS_SUSPEND               "[A-F0-9]\{4}"  contained nextgroup=vgxComment skipwhite

"RESUME
syn keyword vgxKeyword_RESUME       RESUME      nextgroup=vgxComment skipwhite

"REJECTED
syn keyword vgxKeyword_REJECTED     REJECTED    nextgroup=vgxTransID_REJECTED skipwhite
syn match   vgxTransID_REJECTED         "[a-f0-9]\{32}" contained nextgroup=vgxReason_REJECTED skipwhite
syn match   vgxReason_REJECTED          "[A-F0-9]\{8}"  contained nextgroup=vgxComment skipwhite

"TRANSACTION
syn keyword vgxKeyword_TRANSACTION  TRANSACTION nextgroup=vgxTransaction_ID skipwhite
syn match   vgxTransaction_ID           "[a-f0-9]\{32}"  contained nextgroup=vgxTransaction_Serial skipwhite
syn match   vgxTransaction_Serial       "[A-F0-9]\{16}"  contained nextgroup=vgxTransaction_MstrSerial skipwhite
syn match   vgxTransaction_MstrSerial   "[A-F0-9]\{16}"  contained nextgroup=vgxComment skipwhite

"COMMIT
syn keyword vgxKeyword_COMMIT       COMMIT      nextgroup=vgxCommit_ID      skipwhite
syn match   vgxCommit_ID                "[a-f0-9]\{32}"  contained nextgroup=vgxCommit_TMS skipwhite
syn match   vgxCommit_TMS               "[A-F0-9]\{16}"  contained nextgroup=vgxCommit_CRC skipwhite
syn match   vgxCommit_CRC               "[A-F0-9]\{8}"   contained nextgroup=vgxComment skipwhite

"OP
syn keyword vgxKeyword_OP           OP          nextgroup=vgxOperationType skipwhite
syn match   vgxOperationType            "[A-F0-9]\{4}"  contained nextgroup=vgxGraphID,vgxComment skipwhite
syn match   vgxGraphID                  "[a-f0-9]\{32}" contained nextgroup=vgxVertexID,vgxComment skipwhite
syn match   vgxVertexID                 "[a-f0-9]\{32}" contained nextgroup=vgxComment skipwhite

"ENDOP
syn keyword vgxKeyword_ENDOP        ENDOP       nextgroup=vgxOperationOpid,vgxOperationCRC skipwhite
syn match   vgxOperationOpid            "[A-F0-9]\{16}"   contained nextgroup=vgxOperationTMS skipwhite
syn match   vgxOperationTMS             "[A-F0-9]\{16}"   contained nextgroup=vgxOperationCRC skipwhite
syn match   vgxOperationCRC             "[A-F0-9]\{8}\(\n\|\s\)"    contained nextgroup=vgxComment skipwhite


"vxn
syn keyword vgxOperator_vxn   vxn nextgroup=vgxOpcode_vxn skipwhite
syn match   vgxOpcode_vxn           "[A-F0-9]\{8}"  contained nextgroup=vgxVertexID_vxn skipwhite
syn match   vgxVertexID_vxn         "[a-f0-9]\{32}" contained nextgroup=vgxType_vxn skipwhite
syn match   vgxType_vxn             "[A-F0-9]\{2}"  contained nextgroup=vgxTMC_vxn skipwhite
syn match   vgxTMC_vxn              "[A-F0-9]\{8}"  contained nextgroup=vgxVertexTMX_vxn skipwhite
syn match   vgxVertexTMX_vxn        "[A-F0-9]\{8}"  contained nextgroup=vgxArcTMX_vxn skipwhite
syn match   vgxArcTMX_vxn           "[A-F0-9]\{8}"  contained nextgroup=vgxRank_vxn skipwhite
syn match   vgxRank_vxn             "[A-F0-9]\{16}" contained nextgroup=vgxVertexName_vxn skipwhite
syn match   vgxVertexName_vxn       "[A-F0-9]\+"    contained nextgroup=vgxComment skipwhite

"vxd
syn keyword vgxOperator_vxd   vxd nextgroup=vgxOpcode_vxd skipwhite
syn match   vgxOpcode_vxd           "[A-F0-9]\{8}"  contained nextgroup=vgxVertexID_vxd skipwhite
syn match   vgxVertexID_vxd         "[a-f0-9]\{32}" contained nextgroup=vgxEventExec_vxd skipwhite
syn match   vgxEventExec_vxd        "[A-F0-9]\{2}"  contained nextgroup=vgxComment skipwhite

"vxr
syn keyword vgxOperator_vxr   vxr nextgroup=vgxOpcode_vxr skipwhite
syn match   vgxOpcode_vxr           "[A-F0-9]\{8}"  contained nextgroup=vgxRank_vxr skipwhite
syn match   vgxRank_vxr             "[A-F0-9]\{16}" contained nextgroup=vgxComment skipwhite

"vxt
syn keyword vgxOperator_vxt   vxt nextgroup=vgxOpcode_vxt skipwhite
syn match   vgxOpcode_vxt           "[A-F0-9]\{8}"  contained nextgroup=vgxType_vxt skipwhite
syn match   vgxType_vxt             "[A-F0-9]\{2}" contained nextgroup=vgxComment skipwhite

"vxx
syn keyword vgxOperator_vxx   vxx nextgroup=vgxOpcode_vxx skipwhite
syn match   vgxOpcode_vxx           "[A-F0-9]\{8}"  contained nextgroup=vgxVertexTMX_vxx skipwhite
syn match   vgxVertexTMX_vxx        "[A-F0-9]\{8}"  contained nextgroup=vgxComment skipwhite

"vxc
syn keyword vgxOperator_vxc   vxc nextgroup=vgxOpcode_vxc skipwhite
syn match   vgxOpcode_vxc           "[A-F0-9]\{8}"  contained nextgroup=vgxMan_vxc skipwhite
syn match   vgxMan_vxc              "[A-F0-9]\{2}"  contained nextgroup=vgxComment skipwhite

"vps
syn keyword vgxOperator_vps   vps nextgroup=vgxOpcode_vps skipwhite
syn match   vgxOpcode_vps           "[A-F0-9]\{8}"  contained nextgroup=vgxKey_vps skipwhite
syn match   vgxKey_vps              "[A-F0-9]\{16}" contained nextgroup=vgxType_vps skipwhite
syn match   vgxType_vps             "[A-F0-9]\{2}"  contained nextgroup=vgxDataH_vps skipwhite
syn match   vgxDataH_vps            "[A-F0-9]\{16}" contained nextgroup=vgxDataL_vps skipwhite
syn match   vgxDataL_vps            "[A-F0-9]\{16}" contained nextgroup=vgxComment skipwhite

"vpd
syn keyword vgxOperator_vpd   vpd nextgroup=vgxOpcode_vps skipwhite
syn match   vgxOpcode_vpd           "[A-F0-9]\{8}"  contained nextgroup=vgxKey_vpd skipwhite
syn match   vgxKey_vpd              "[A-F0-9]\{16}" contained nextgroup=vgxComment skipwhite

"vpc
syn keyword vgxOperator_vpc   vpc nextgroup=vgxOpcode_vpc skipwhite
syn match   vgxOpcode_vpc           "[A-F0-9]\{8}"  contained nextgroup=vgxComment skipwhite

"vvs
syn keyword vgxOperator_vvs   vvs nextgroup=vgxOpcode_vvs skipwhite
syn match   vgxOpcode_vvs           "[A-F0-9]\{8}"  contained nextgroup=vgxVectorData_vvs skipwhite
syn match   vgxVectorData_vvs       "[A-F0-9]\+"    contained nextgroup=vgxComment skipwhite

"vvd
syn keyword vgxOperator_vvd   vvd nextgroup=vgxOpcode_vvd skipwhite
syn match   vgxOpcode_vvd           "[A-F0-9]\{8}"  contained nextgroup=vgxComment skipwhite

"vod
syn keyword vgxOperator_vod   vod nextgroup=vgxOpcode_vod skipwhite
syn match   vgxOpcode_vod           "[A-F0-9]\{8}"  contained nextgroup=vgxRemovedArcs skipwhite

"vid
syn keyword vgxOperator_vid   vid nextgroup=vgxOpcode_vid skipwhite
syn match   vgxOpcode_vid           "[A-F0-9]\{8}"  contained nextgroup=vgxRemovedArcs skipwhite
syn match   vgxRemovedArcs          "[A-F0-9]\{16}" contained nextgroup=vgxComment skipwhite

"val
syn keyword vgxOperator_val   val nextgroup=vgxOpcode_val skipwhite
syn match   vgxOpcode_val           "[A-F0-9]\{8}"  contained nextgroup=vgxComment skipwhite

"vrl
syn keyword vgxOperator_vrl   vrl nextgroup=vgxOpcode_vrl skipwhite
syn match   vgxOpcode_vrl           "[A-F0-9]\{8}"  contained nextgroup=vgxComment skipwhite

"arc
syn keyword vgxOperator_arc   arc nextgroup=vgxOpcode_arc skipwhite
syn match   vgxOpcode_arc           "[A-F0-9]\{8}"  contained nextgroup=vgxPredicatorEph_arc skipwhite
syn match   vgxPredicatorEph_arc    "[A-F0-9]\{2}"  contained nextgroup=vgxPredicatorMod_arc
syn match   vgxPredicatorMod_arc    "[A-F0-9]\{2}"  contained nextgroup=vgxPredicatorRel_arc
syn match   vgxPredicatorRel_arc    "[A-F0-9]\{4}"  contained nextgroup=vgxPredicatorVal_arc
syn match   vgxPredicatorVal_arc    "[A-F0-9]\{8}"  contained nextgroup=vgxVertexID_arc skipwhite
syn match   vgxVertexID_arc         "[a-f0-9]\{32}" contained nextgroup=vgxComment skipwhite

"ard
syn keyword vgxOperator_ard   ard nextgroup=vgxOpcode_ard skipwhite
syn match   vgxOpcode_ard           "[A-F0-9]\{8}"  contained nextgroup=vgxEventExec_ard skipwhite
syn match   vgxEventExec_ard        "[A-F0-9]\{2}"  contained nextgroup=vgxRemovedArcs_ard skipwhite
syn match   vgxRemovedArcs_ard      "[A-F0-9]\{16}" contained nextgroup=vgxPredicatorEph_ard skipwhite
syn match   vgxPredicatorEph_ard    "[A-F0-9]\{2}"  contained nextgroup=vgxPredicatorMod_ard
syn match   vgxPredicatorMod_ard    "[A-F0-9]\{2}"  contained nextgroup=vgxPredicatorRel_ard
syn match   vgxPredicatorRel_ard    "[A-F0-9]\{4}"  contained nextgroup=vgxPredicatorVal_ard
syn match   vgxPredicatorVal_ard    "[A-F0-9]\{8}"  contained nextgroup=vgxVertexID_ard skipwhite
syn match   vgxVertexID_ard         "[a-f0-9]\{32}" contained nextgroup=vgxComment skipwhite


"sya
syn keyword vgxOperator_sya   sya nextgroup=vgxOpcode_sya skipwhite
syn match   vgxOpcode_sya           "[A-F0-9]\{8}"  contained nextgroup=vgxTMS_sya skipwhite
syn match   vgxTMS_sya              "[A-F0-9]\{16}" contained nextgroup=vgxViaURI_sya skipwhite
syn match   vgxViaURI_sya           "[A-F0-9]\+"    contained nextgroup=vgxOriginHost_sya skipwhite
syn match   vgxOriginHost_sya       "[A-F0-9]\+"    contained nextgroup=vgxOriginVer_sya skipwhite
syn match   vgxOriginVer_sya        "[A-F0-9]\+"    contained nextgroup=vgxStatus_sya skipwhite
syn match   vgxStatus_sya           "[A-F0-9]\{8}"  contained nextgroup=vgxComment skipwhite

"syd
syn keyword vgxOperator_syd   syd nextgroup=vgxOpcode_syd skipwhite
syn match   vgxOpcode_syd           "[A-F0-9]\{8}"  contained nextgroup=vgxTMS_syd skipwhite
syn match   vgxTMS_syd              "[A-F0-9]\{16}" contained nextgroup=vgxOriginHost_syd skipwhite
syn match   vgxOriginHost_syd       "[A-F0-9]\+"    contained nextgroup=vgxOriginVer_syd skipwhite
syn match   vgxOriginVer_syd        "[A-F0-9]\+"    contained nextgroup=vgxStatus_syd skipwhite
syn match   vgxStatus_syd           "[A-F0-9]\{8}"  contained nextgroup=vgxComment skipwhite


"rcl
syn keyword vgxOperator_rcl   rcl nextgroup=vgxOpcode_rcl skipwhite
syn match   vgxOpcode_rcl           "[A-F0-9]\{8}"  contained nextgroup=vgxComment skipwhite

"scf
syn keyword vgxOperator_scf   scf nextgroup=vgxOpcode_scf skipwhite
syn match   vgxOpcode_scf           "[A-F0-9]\{8}"  contained nextgroup=vgxFPNSegm_scf skipwhite
syn match   vgxFPNSegm_scf          "[A-F0-9]\{8}"  contained nextgroup=vgxFPNSign_scf skipwhite
syn match   vgxFPNSign_scf          "[A-F0-9]\{8}"  contained nextgroup=vgxVecMaxSz_scf skipwhite
syn match   vgxVecMaxSz_scf         "[A-F0-9]\{8}"  contained nextgroup=vgxVecMinIsect_scf skipwhite
syn match   vgxVecMinIsect_scf      "[A-F0-9]\{8}"  contained nextgroup=vgxVecMinCos_scf skipwhite
syn match   vgxVecMinCos_scf        "[A-F0-9]\{8}"  contained nextgroup=vgxVecMinJac_scf skipwhite
syn match   vgxVecMinJac_scf        "[A-F0-9]\{8}"  contained nextgroup=vgxVecCosExp_scf skipwhite
syn match   vgxVecCosExp_scf        "[A-F0-9]\{8}"  contained nextgroup=vgxVecJacExp_scf skipwhite
syn match   vgxVecJacExp_scf        "[A-F0-9]\{8}"  contained nextgroup=vgxHam_scf skipwhite
syn match   vgxHam_scf              "[A-F0-9]\{8}"  contained nextgroup=vgxSim_scf skipwhite
syn match   vgxSim_scf              "[A-F0-9]\{8}"  contained nextgroup=vgxComment skipwhite

"grn
syn keyword vgxOperator_grn   grn nextgroup=vgxOpcode_grn skipwhite
syn match   vgxOpcode_grn           "[A-F0-9]\{8}"  contained nextgroup=vgxBlockOrder_grn skipwhite
syn match   vgxBlockOrder_grn       "[A-F0-9]\{8}"  contained nextgroup=vgxTime0_grn skipwhite
syn match   vgxTime0_grn            "[A-F0-9]\{8}"  contained nextgroup=vgxOpcount0_grn skipwhite
syn match   vgxOpcount0_grn         "[A-F0-9]\{16}" contained nextgroup=vgxGraphID_grn skipwhite
syn match   vgxGraphID_grn          "[a-f0-9]\{32}" contained nextgroup=vgxPath_grn skipwhite
syn match   vgxPath_grn             "[A-F0-9]\+"    contained nextgroup=vgxName_grn skipwhite
syn match   vgxName_grn             "[A-F0-9]\+"    contained nextgroup=vgxComment skipwhite

"grd
syn keyword vgxOperator_grd   grd nextgroup=vgxOpcode_grd skipwhite
syn match   vgxOpcode_grd           "[A-F0-9]\{8}"  contained nextgroup=vgxGraphID_grd skipwhite
syn match   vgxGraphID_grd          "[a-f0-9]\{32}" contained nextgroup=vgxComment skipwhite

"com
syn keyword vgxOperator_com   com nextgroup=vgxOpcode_com skipwhite
syn match   vgxOpcode_com           "[A-F0-9]\{8}"  contained nextgroup=vgxString_com skipwhite
syn match   vgxString_com           "[A-F0-9]\+"    contained nextgroup=vgxComment skipwhite

"dat
syn keyword vgxOperator_dat   dat nextgroup=vgxOpcode_dat skipwhite
syn match   vgxOpcode_dat           "[A-F0-9]\{8}"  contained nextgroup=vgxNParts_dat skipwhite
syn match   vgxNParts_dat           "[A-F0-9]\{16}" contained nextgroup=vgxPartID_dat skipwhite
syn match   vgxPartID_dat           "[A-F0-9]\{16}" contained nextgroup=vgxData_dat skipwhite
syn match   vgxData_dat             "[A-F0-9]\+"    contained nextgroup=vgxSzData_dat skipwhite
syn match   vgxSzData_dat           "[A-F0-9]\{16}" contained nextgroup=vgxDataID_dat skipwhite
syn match   vgxDataID_dat           "[a-f0-9]\{32}" contained nextgroup=vgxComment skipwhite

"grt
syn keyword vgxOperator_grt   grt nextgroup=vgxOpcode_grt skipwhite
syn match   vgxOpcode_grt           "[A-F0-9]\{8}"  contained nextgroup=vgxType_grt skipwhite
syn match   vgxType_grt             "[A-F0-9]\{2}"  contained nextgroup=vgxDiscarded_grt skipwhite
syn match   vgxDiscarded_grt        "[A-F0-9]\{16}" contained nextgroup=vgxComment skipwhite

"grp
syn keyword vgxOperator_grp   grp nextgroup=vgxOpcode_grp skipwhite
syn match   vgxOpcode_grp           "[A-F0-9]\{8}"  contained nextgroup=vgxOrder_grp skipwhite
syn match   vgxOrder_grp            "[A-F0-9]\{16}" contained nextgroup=vgxSize_grp skipwhite
syn match   vgxSize_grp             "[A-F0-9]\{16}" contained nextgroup=vgxNKey_grp skipwhite
syn match   vgxNKey_grp             "[A-F0-9]\{16}" contained nextgroup=vgxNStrval_grp skipwhite
syn match   vgxNStrval_grp          "[A-F0-9]\{16}" contained nextgroup=vgxNProp_grp skipwhite
syn match   vgxNProp_grp            "[A-F0-9]\{16}" contained nextgroup=vgxNVec_grp skipwhite
syn match   vgxNVec_grp             "[A-F0-9]\{16}" contained nextgroup=vgxNDim_grp skipwhite
syn match   vgxNDim_grp             "[A-F0-9]\{8}"  contained nextgroup=vgxNRel_grp skipwhite
syn match   vgxNRel_grp             "[A-F0-9]\{4}"  contained nextgroup=vgxNType_grp skipwhite
syn match   vgxNType_grp            "[A-F0-9]\{2}"  contained nextgroup=vgxFlags_grp skipwhite
syn match   vgxFlags_grp            "[A-F0-9]\{2}"  contained nextgroup=vgxComment skipwhite

"grs
syn keyword vgxOperator_grs   grs nextgroup=vgxOpcode_grs skipwhite
syn match   vgxOpcode_grs           "[A-F0-9]\{8}"  contained nextgroup=vgxOrder_grs skipwhite
syn match   vgxOrder_grs            "[A-F0-9]\{16}" contained nextgroup=vgxSize_grs skipwhite
syn match   vgxSize_grs             "[A-F0-9]\{16}" contained nextgroup=vgxNKey_grs skipwhite
syn match   vgxNKey_grs             "[A-F0-9]\{16}" contained nextgroup=vgxNStrval_grs skipwhite
syn match   vgxNStrval_grs          "[A-F0-9]\{16}" contained nextgroup=vgxNProp_grs skipwhite
syn match   vgxNProp_grs            "[A-F0-9]\{16}" contained nextgroup=vgxNVec_grs skipwhite
syn match   vgxNVec_grs             "[A-F0-9]\{16}" contained nextgroup=vgxNDim_grs skipwhite
syn match   vgxNDim_grs             "[A-F0-9]\{8}"  contained nextgroup=vgxNRel_grs skipwhite
syn match   vgxNRel_grs             "[A-F0-9]\{4}"  contained nextgroup=vgxNType_grs skipwhite
syn match   vgxNType_grs            "[A-F0-9]\{2}"  contained nextgroup=vgxFlags_grs skipwhite
syn match   vgxFlags_grs            "[A-F0-9]\{2}"  contained nextgroup=vgxComment skipwhite

"lxw
syn keyword vgxOperator_lxw   lxw nextgroup=vgxOpcode_lxw skipwhite
syn match   vgxOpcode_lxw           "[A-F0-9]\{8}"  contained nextgroup=vgxCount_lxw skipwhite
syn match   vgxCount_lxw            "[A-F0-9]\{8}"  contained nextgroup=vgxVertexID_lxw skipwhite
syn match   vgxVertexID_lxw         "[a-f0-9]\{32}" contained nextgroup=vgxVertexID_lxw,vgxComment skipwhite

"ulv
syn keyword vgxOperator_ulv   ulv nextgroup=vgxOpcode_ulv skipwhite
syn match   vgxOpcode_ulv           "[A-F0-9]\{8}"  contained nextgroup=vgxCount_ulv skipwhite
syn match   vgxCount_ulv            "[A-F0-9]\{8}"  contained nextgroup=vgxVertexID_ulv skipwhite
syn match   vgxVertexID_ulv         "[a-f0-9]\{32}" contained nextgroup=vgxVertexID_ulv,vgxComment skipwhite

"ula
syn keyword vgxOperator_ula   ula nextgroup=vgxOpcode_ula skipwhite
syn match   vgxOpcode_ula           "[A-F0-9]\{8}"  contained nextgroup=vgxCount_ula skipwhite
syn match   vgxCount_ula            "[A-F0-9]\{8}"  contained nextgroup=vgxComment skipwhite

"grr
syn keyword vgxOperator_grr   grr nextgroup=vgxOpcode_grr skipwhite
syn match   vgxOpcode_grr           "[A-F0-9]\{8}"  contained nextgroup=vgxComment skipwhite

"grw
syn keyword vgxOperator_grw   grw nextgroup=vgxOpcode_grw skipwhite
syn match   vgxOpcode_grw           "[A-F0-9]\{8}"  contained nextgroup=vgxComment skipwhite

"gre
syn keyword vgxOperator_gre   gre nextgroup=vgxOpcode_gre skipwhite
syn match   vgxOpcode_gre           "[A-F0-9]\{8}"  contained nextgroup=vgxComment skipwhite

"gri
syn keyword vgxOperator_gri   gri nextgroup=vgxOpcode_gri skipwhite
syn match   vgxOpcode_gri           "[A-F0-9]\{8}"  contained nextgroup=vgxComment skipwhite

"tic
syn keyword vgxOperator_tic   tic nextgroup=vgxOpcode_tic skipwhite
syn match   vgxOpcode_tic           "[A-F0-9]\{8}"  contained nextgroup=vgxTMS_tic skipwhite
syn match   vgxTMS_tic              "[A-F0-9]\{16}" contained nextgroup=vgxComment skipwhite

"evx
syn keyword vgxOperator_evx   evx nextgroup=vgxOpcode_evx skipwhite
syn match   vgxOpcode_evx           "[A-F0-9]\{8}"  contained nextgroup=vgxTS_evx skipwhite
syn match   vgxTS_evx               "[A-F0-9]\{8}"  contained nextgroup=vgxTMX_evx skipwhite
syn match   vgxTMX_evx              "[A-F0-9]\{8}"  contained nextgroup=vgxComment skipwhite

"vea
syn keyword vgxOperator_vea   vea nextgroup=vgxOpcode_vea skipwhite
syn match   vgxOpcode_vea           "[A-F0-9]\{8}"  contained nextgroup=vgxTypeHash_vea skipwhite
syn match   vgxTypeHash_vea         "[A-F0-9]\{16}" contained nextgroup=vgxTypeZeros_vea skipwhite
syn match   vgxTypeZeros_vea        "0\{14}"        contained nextgroup=vgxTypeEnc_vea
syn match   vgxTypeEnc_vea          "[A-F0-9]\{2}"  contained nextgroup=vgxTypeString_vea skipwhite
syn match   vgxTypeString_vea       "[A-F0-9]\+"    contained nextgroup=vgxComment skipwhite

"ved
syn keyword vgxOperator_ved   ved nextgroup=vgxOpcode_ved skipwhite
syn match   vgxOpcode_ved           "[A-F0-9]\{8}"  contained nextgroup=vgxTypeHash_ved skipwhite
syn match   vgxTypeHash_ved         "[A-F0-9]\{16}" contained nextgroup=vgxTypeZeros_ved skipwhite
syn match   vgxTypeZeros_ved        "0\{14}"        contained nextgroup=vgxTypeEnc_ved
syn match   vgxTypeEnc_ved          "[A-F0-9]\{2}"  contained nextgroup=vgxComment skipwhite

"rea
syn keyword vgxOperator_rea   rea nextgroup=vgxOpcode_rea skipwhite
syn match   vgxOpcode_rea           "[A-F0-9]\{8}"  contained nextgroup=vgxRelHash_rea skipwhite
syn match   vgxRelHash_rea          "[A-F0-9]\{16}" contained nextgroup=vgxRelZeros_rea skipwhite
syn match   vgxRelZeros_rea         "0\{12}"        contained nextgroup=vgxRelEnc_rea
syn match   vgxRelEnc_rea           "[A-F0-9]\{4}"  contained nextgroup=vgxRelString_rea skipwhite
syn match   vgxRelString_rea        "[A-F0-9]\+"    contained nextgroup=vgxComment skipwhite

"red
syn keyword vgxOperator_red   red nextgroup=vgxOpcode_red skipwhite
syn match   vgxOpcode_red           "[A-F0-9]\{8}"  contained nextgroup=vgxRelHash_red skipwhite
syn match   vgxRelHash_red          "[A-F0-9]\{16}" contained nextgroup=vgxRelZeros_red skipwhite
syn match   vgxRelZeros_red         "0\{12}"        contained nextgroup=vgxRelEnc_red
syn match   vgxRelEnc_red           "[A-F0-9]\{4}"  contained nextgroup=vgxComment skipwhite

"dea
syn keyword vgxOperator_dea   dea nextgroup=vgxOpcode_dea skipwhite
syn match   vgxOpcode_dea           "[A-F0-9]\{8}"  contained nextgroup=vgxDimHash_dea skipwhite
syn match   vgxDimHash_dea          "[A-F0-9]\{16}" contained nextgroup=vgxDimZeros_dea skipwhite
syn match   vgxDimZeros_dea         "0\{9}"         contained nextgroup=vgxDim_dea
syn match   vgxDim_dea              "[A-F0-9]\{7}"  contained nextgroup=vgxDimString_dea skipwhite
syn match   vgxDimString_dea        "[A-F0-9]\+"    contained nextgroup=vgxComment skipwhite

"ded
syn keyword vgxOperator_ded   ded nextgroup=vgxOpcode_ded skipwhite
syn match   vgxOpcode_ded           "[A-F0-9]\{8}"  contained nextgroup=vgxDimHash_ded skipwhite
syn match   vgxDimHash_ded          "[A-F0-9]\{16}" contained nextgroup=vgxDimZeros_ded skipwhite
syn match   vgxDimZeros_ded         "0\{9}"         contained nextgroup=vgxDim_ded
syn match   vgxDim_ded              "[A-F0-9]\{7}"  contained nextgroup=vgxComment skipwhite

"kea
syn keyword vgxOperator_kea   kea nextgroup=vgxOpcode_kea skipwhite
syn match   vgxOpcode_kea           "[A-F0-9]\{8}"  contained nextgroup=vgxKeyHash_kea skipwhite
syn match   vgxKeyHash_kea          "[A-F0-9]\{16}" contained nextgroup=vgxKey_kea skipwhite
syn match   vgxKey_kea              "[A-F0-9]\{16}" contained nextgroup=vgxKeyString_kea skipwhite
syn match   vgxKeyString_kea        "[A-F0-9]\+"    contained nextgroup=vgxComment skipwhite

"ked
syn keyword vgxOperator_ked   ked nextgroup=vgxOpcode_ked skipwhite
syn match   vgxOpcode_ked           "[A-F0-9]\{8}"  contained nextgroup=vgxKeyHash_ked skipwhite
syn match   vgxKeyHash_ked          "[A-F0-9]\{16}" contained nextgroup=vgxKey_ked skipwhite
syn match   vgxKey_ked              "[A-F0-9]\{16}" contained nextgroup=vgxComment skipwhite

"sea
syn keyword vgxOperator_sea   sea nextgroup=vgxOpcode_sea skipwhite
syn match   vgxOpcode_sea           "[A-F0-9]\{8}"  contained nextgroup=vgxString_sea skipwhite
syn match   vgxString_sea           "[A-F0-9]\+"    contained nextgroup=vgxKeyH_sea skipwhite
syn match   vgxKeyH_sea             "[a-f0-9]\{16}" contained nextgroup=vgxKeyL_sea
syn match   vgxKeyL_sea             "[a-f0-9]\{16}" contained nextgroup=vgxComment skipwhite

"sed
syn keyword vgxOperator_sed   sed nextgroup=vgxOpcode_sed skipwhite
syn match   vgxOpcode_sed           "[A-F0-9]\{8}"  contained nextgroup=vgxKeyH_sed skipwhite
syn match   vgxKeyH_sed             "[a-f0-9]\{16}" contained nextgroup=vgxKeyL_sed
syn match   vgxKeyL_sed             "[a-f0-9]\{16}" contained nextgroup=vgxComment skipwhite

"nop
syn keyword vgxOperator_nop   nop nextgroup=vgxOpcode_nop skipwhite
syn match   vgxOpcode_nop           "[A-F0-9]\{8}"  contained nextgroup=vgxComment skipwhite




let b:current_syntax = "vgx"

hi def link vgxComment              highVgxComment

"ATTACH
hi def link vgxKeyword_ATTACH       highVgxKeyword
hi def link vgxProtocol_ATTACH      highVgxNumericData
hi def link vgxVersion_ATTACH       highVgxNumericData
hi def link vgxDigest_ATTACH        highVgxObjectID
hi def link vgxAdminPort_ATTACH     highVgxNumericData

"ACCEPTED
hi def link vgxKeyword_ACCEPTED     highVgxKeyword
hi def link vgxTransID_ACCEPTED     highVgxTransactionID
hi def link vgxCRC_ACCEPTED         highVgxCRC

"IDLE
hi def link vgxKeyword_IDLE         highVgxKeyword
hi def link vgxTMS_IDLE             highVgxTimestampData
hi def link vgxDigest_IDLE          highVgxObjectID

"RESYNC
hi def link vgxKeyword_RESYNC       highVgxKeyword
hi def link vgxTransID_RESYNC       highVgxTransactionID
hi def link vgxNDiscard_RESYNC      highVgxNumericData

"RETRY
hi def link vgxKeyword_RETRY        highVgxKeyword
hi def link vgxTransID_RETRY        highVgxTransactionID
hi def link vgxReason_RETRY         highVgxNumericData

"SUSPEND
hi def link vgxKeyword_SUSPEND      highVgxKeyword
hi def link vgxCode_SUSPEND         highVgxNumericData
hi def link vgxMS_SUSPEND           highVgxTimestampData

"RESUME
hi def link vgxKeyword_RESUME       highVgxKeyword

"REJECTED
hi def link vgxKeyword_REJECTED     highVgxKeyword
hi def link vgxTransID_REJECTED     highVgxTransactionID
hi def link vgxReason_REJECTED      highVgxNumericData

"TRANSACTION
hi def link vgxKeyword_TRANSACTION      highVgxKeyword
hi def link vgxTransaction_ID           highVgxTransactionID
hi def link vgxTransaction_Serial       highVgxTransactionSerial
hi def link vgxTransaction_MstrSerial   highVgxMasterSerial

"COMMIT
hi def link vgxKeyword_COMMIT       highVgxKeyword
hi def link vgxCommit_ID            highVgxTransactionID
hi def link vgxCommit_TMS           highVgxNumericData
hi def link vgxCommit_CRC           highVgxCRC

"OP
hi def link vgxKeyword_OP           highVgxKeyword
hi def link vgxOperationType        highVgxOperationType
hi def link vgxGraphID              highVgxGraphID
hi def link vgxVertexID             highVgxVertexID

"ENDOP
hi def link vgxKeyword_ENDOP        highVgxKeyword
hi def link vgxOperationOpid        highVgxGeneralData
hi def link vgxOperationTMS         highVgxGeneralData
hi def link vgxOperationCRC         highVgxCRC



"vxn
hi def link vgxOperator_vxn           highVgxOperatorNew
hi def link vgxOpcode_vxn             highVgxOpcode
hi def link vgxVertexID_vxn           highVgxVertexID
hi def link vgxType_vxn               highVgxTypeEnum
hi def link vgxTMC_vxn                highVgxTimestampData
hi def link vgxVertexTMX_vxn          highVgxTimestampData
hi def link vgxArcTMX_vxn             highVgxTimestampData
hi def link vgxRank_vxn               highVgxNumericData
hi def link vgxVertexName_vxn         highVgxGeneralData

"vxd
hi def link vgxOperator_vxd           highVgxOperatorDelete
hi def link vgxOpcode_vxd             highVgxOpcode
hi def link vgxVertexID_vxd           highVgxVertexID
hi def link vgxEventExec_vxd          highVgxEventExec

"vxr
hi def link vgxOperator_vxr           highVgxOperatorAssign
hi def link vgxOpcode_vxr             highVgxOpcode
hi def link vgxRank_vxr               highVgxNumericData

"vxt
hi def link vgxOperator_vxt           highVgxOperatorAssign
hi def link vgxOpcode_vxt             highVgxOpcode
hi def link vgxType_vxt               highVgxTypeEnum

"vxx
hi def link vgxOperator_vxx           highVgxOperatorAssign
hi def link vgxOpcode_vxx             highVgxOpcode
hi def link vgxVertexTMX_vxx          highVgxTimestampData

"vxc
hi def link vgxOperator_vxc           highVgxOperatorAssign
hi def link vgxOpcode_vxc             highVgxOpcode
hi def link vgxMan_vxc                highVgxNumericData

"vps
hi def link vgxOperator_vps           highVgxOperatorSet
hi def link vgxOpcode_vps             highVgxOpcode
hi def link vgxKey_vps                highVgxKeyEnum
hi def link vgxType_vps               highVgxType
hi def link vgxDataH_vps              highVgxStringEnum
hi def link vgxDataL_vps              highVgxStringEnum

"vpd
hi def link vgxOperator_vpd           highVgxOperatorDelete
hi def link vgxOpcode_vpd             highVgxOpcode
hi def link vgxKey_vpd                highVgxKeyEnum

"vpc
hi def link vgxOperator_vpc           highVgxOperatorDelete
hi def link vgxOpcode_vpc             highVgxOpcode

"vvs
hi def link vgxOperator_vvs           highVgxOperatorSet
hi def link vgxOpcode_vvs             highVgxOpcode
hi def link vgxVectorData_vvs         highVgxGeneralData

"vvd
hi def link vgxOperator_vvd           highVgxOperatorDelete
hi def link vgxOpcode_vvd             highVgxOpcode

"vod
hi def link vgxOperator_vod           highVgxOperatorDelete
hi def link vgxOpcode_vod             highVgxOpcode

"vid
hi def link vgxOperator_vid           highVgxOperatorDelete
hi def link vgxOpcode_vid             highVgxOpcode
hi def link vgxRemovedArcs            highVgxNumericData

"val
hi def link vgxOperator_val           highVgxOperatorState1
hi def link vgxOpcode_val             highVgxOpcode

"vrl
hi def link vgxOperator_vrl           highVgxOperatorState0
hi def link vgxOpcode_vrl             highVgxOpcode

"arc
hi def link vgxOperator_arc           highVgxOperatorNew
hi def link vgxOpcode_arc             highVgxOpcode
hi def link vgxPredicatorEph_arc      highVgxGeneralData
hi def link vgxPredicatorMod_arc      highVgxType
hi def link vgxPredicatorRel_arc      highVgxRelEnum
hi def link vgxPredicatorVal_arc      highVgxNumericData
hi def link vgxVertexID_arc           highVgxVertexID

"ard
hi def link vgxOperator_ard           highVgxOperatorDelete
hi def link vgxOpcode_ard             highVgxOpcode
hi def link vgxEventExec_ard          highVgxEventExec
hi def link vgxRemovedArcs_ard        highVgxNumericData
hi def link vgxPredicatorEph_ard      highVgxGeneralData
hi def link vgxPredicatorMod_ard      highVgxType
hi def link vgxPredicatorRel_ard      highVgxRelEnum
hi def link vgxPredicatorVal_ard      highVgxNumericData
hi def link vgxVertexID_ard           highVgxVertexID

"sya
hi def link vgxOperator_sya           highVgxOperatorState1
hi def link vgxOpcode_sya             highVgxOpcode
hi def link vgxTMS_sya                highVgxTimestampData
hi def link vgxViaURI_sya             highVgxGeneralData
hi def link vgxOriginHost_sya         highVgxGeneralData
hi def link vgxOriginVer_sya          highVgxGeneralData
hi def link vgxStatus_sya             highVgxNumericData

"syd
hi def link vgxOperator_syd           highVgxOperatorState0
hi def link vgxOpcode_syd             highVgxOpcode
hi def link vgxTMS_syd                highVgxTimestampData
hi def link vgxOriginHost_syd         highVgxGeneralData
hi def link vgxOriginVer_syd          highVgxGeneralData
hi def link vgxStatus_syd             highVgxNumericData

"rcl
hi def link vgxOperator_rcl           highVgxOperatorDelete
hi def link vgxOpcode_rcl             highVgxOpcode

"scf
hi def link vgxOperator_scf           highVgxOperatorSet
hi def link vgxOpcode_scf             highVgxOpcode
hi def link vgxFPNSegm_scf            highVgxNumericData
hi def link vgxFPNSign_scf            highVgxNumericData
hi def link vgxVecMaxSz_scf           highVgxNumericData
hi def link vgxVecMinIsect_scf        highVgxNumericData
hi def link vgxVecMinCos_scf          highVgxNumericData
hi def link vgxVecMinJac_scf          highVgxNumericData
hi def link vgxVecCosExp_scf          highVgxNumericData
hi def link vgxVecJacExp_scf          highVgxNumericData
hi def link vgxHam_scf                highVgxNumericData
hi def link vgxSim_scf                highVgxNumericData

"grn
hi def link vgxOperator_grn           highVgxOperatorNew
hi def link vgxOpcode_grn             highVgxOpcode
hi def link vgxBlockOrder_grn         highVgxNumericData
hi def link vgxTime0_grn              highVgxTimestampData
hi def link vgxOpcount0_grn           highVgxNumericData
hi def link vgxGraphID_grn            highVgxGraphID
hi def link vgxPath_grn               highVgxGeneralData
hi def link vgxName_grn               highVgxGeneralData

"grd
hi def link vgxOperator_grd           highVgxOperatorDelete
hi def link vgxOpcode_grd             highVgxOpcode
hi def link vgxGraphID_grd            highVgxGraphID

"com
hi def link vgxOperator_com           highVgxOperatorEvent
hi def link vgxOpcode_com             highVgxOpcode
hi def link vgxString_com             highVgxGeneralData

"dat
hi def link vgxOperator_dat           highVgxOperatorEvent
hi def link vgxOpcode_dat             highVgxOpcode
hi def link vgxNParts_dat             highVgxNumericData
hi def link vgxPartID_dat             highVgxNumericData
hi def link vgxData_dat               highVgxGeneralData
hi def link vgxSzData_dat             highVgxNumericData
hi def link vgxDataID_dat             highVgxObjectID

"grt
hi def link vgxOperator_grt           highVgxOperatorDelete
hi def link vgxOpcode_grt             highVgxOpcode
hi def link vgxType_grt               highVgxType
hi def link vgxDiscarded_grt          highVgxNumericData

"grp
hi def link vgxOperator_grp           highVgxOperatorEvent
hi def link vgxOpcode_grp             highVgxOpcode
hi def link vgxOrder_grp              highVgxNumericData
hi def link vgxSize_grp               highVgxNumericData 
hi def link vgxNKey_grp               highVgxNumericData 
hi def link vgxNStrval_grp            highVgxNumericData 
hi def link vgxNProp_grp              highVgxNumericData 
hi def link vgxNVec_grp               highVgxNumericData 
hi def link vgxNDim_grp               highVgxNumericData 
hi def link vgxNRel_grp               highVgxNumericData 
hi def link vgxNType_grp              highVgxNumericData 
hi def link vgxFlags_grp              highVgxNumericData 

"grs
hi def link vgxOperator_grs           highVgxOperatorEvent
hi def link vgxOpcode_grs             highVgxOpcode
hi def link vgxOrder_grs              highVgxNumericData
hi def link vgxSize_grs               highVgxNumericData 
hi def link vgxNKey_grs               highVgxNumericData 
hi def link vgxNStrval_grs            highVgxNumericData 
hi def link vgxNProp_grs              highVgxNumericData 
hi def link vgxNVec_grs               highVgxNumericData 
hi def link vgxNDim_grs               highVgxNumericData 
hi def link vgxNRel_grs               highVgxNumericData 
hi def link vgxNType_grs              highVgxNumericData 
hi def link vgxFlags_grs              highVgxNumericData 

"lxw
hi def link vgxOperator_lxw           highVgxOperatorState1
hi def link vgxOpcode_lxw             highVgxOpcode
hi def link vgxCount_lxw              highVgxNumericData
hi def link vgxVertexID_lxw           highVgxVertexID

"ulv
hi def link vgxOperator_ulv           highVgxOperatorState0
hi def link vgxOpcode_ulv             highVgxOpcode
hi def link vgxCount_ulv              highVgxNumericData
hi def link vgxVertexID_ulv           highVgxVertexID

"ula
hi def link vgxOperator_ula           highVgxOperatorState0
hi def link vgxOpcode_ula             highVgxOpcode
hi def link vgxCount_ula              highVgxNumericData

"grr
hi def link vgxOperator_grr           highVgxOperatorState0
hi def link vgxOpcode_grr             highVgxOpcode

"grw
hi def link vgxOperator_grw           highVgxOperatorState1
hi def link vgxOpcode_grw             highVgxOpcode

"gre
hi def link vgxOperator_gre           highVgxOperatorState1
hi def link vgxOpcode_gre             highVgxOpcode

"gri
hi def link vgxOperator_gri           highVgxOperatorState0
hi def link vgxOpcode_gri             highVgxOpcode

"tic
hi def link vgxOperator_tic           highVgxOperatorEvent
hi def link vgxOpcode_tic             highVgxOpcode
hi def link vgxTMS_tic                highVgxTimestampData

"evx
hi def link vgxOperator_evx           highVgxOperatorEvent
hi def link vgxOpcode_evx             highVgxOpcode
hi def link vgxTS_evx                 highVgxTimestampData
hi def link vgxTMX_evx                highVgxTimestampData

"vea
hi def link vgxOperator_vea           highVgxOperatorEnum
hi def link vgxOpcode_vea             highVgxOpcode
hi def link vgxTypeHash_vea           highVgxGeneralData
hi def link vgxTypeZeros_vea          highVgxGeneralData
hi def link vgxTypeEnc_vea            highVgxTypeEnum
hi def link vgxTypeString_vea         highVgxGeneralData

"ved
hi def link vgxOperator_ved           highVgxOperatorDelete
hi def link vgxOpcode_ved             highVgxOpcode
hi def link vgxTypeHash_ved           highVgxGeneralData
hi def link vgxTypeZeros_ved          highVgxGeneralData
hi def link vgxTypeEnc_ved            highVgxTypeEnum

"rea
hi def link vgxOperator_rea           highVgxOperatorEnum
hi def link vgxOpcode_rea             highVgxOpcode
hi def link vgxRelHash_rea            highVgxGeneralData
hi def link vgxRelZeros_rea           highVgxGeneralData
hi def link vgxRelEnc_rea             highVgxRelEnum
hi def link vgxRelString_rea          highVgxGeneralData

"red
hi def link vgxOperator_red           highVgxOperatorDelete
hi def link vgxOpcode_red             highVgxOpcode
hi def link vgxRelHash_red            highVgxGeneralData
hi def link vgxRelZeros_red           highVgxGeneralData
hi def link vgxRelEnc_red             highVgxRelEnum

"dea
hi def link vgxOperator_dea           highVgxOperatorEnum
hi def link vgxOpcode_dea             highVgxOpcode
hi def link vgxDimHash_dea            highVgxGeneralData
hi def link vgxDimZeros_dea           highVgxGeneralData
hi def link vgxDim_dea                highVgxDimEnum
hi def link vgxDimString_dea          highVgxGeneralData

"ded
hi def link vgxOperator_ded           highVgxOperatorDelete
hi def link vgxOpcode_ded             highVgxOpcode
hi def link vgxDimHash_ded            highVgxGeneralData
hi def link vgxDimZeros_ded           highVgxGeneralData
hi def link vgxDim_ded                highVgxDimEnum

"kea
hi def link vgxOperator_kea           highVgxOperatorEnum
hi def link vgxOpcode_kea             highVgxOpcode
hi def link vgxKeyHash_kea            highVgxGeneralData
hi def link vgxKey_kea                highVgxKeyEnum
hi def link vgxKeyString_kea          highVgxGeneralData

"ked
hi def link vgxOperator_ked           highVgxOperatorDelete
hi def link vgxOpcode_ked             highVgxOpcode
hi def link vgxKeyHash_ked            highVgxGeneralData
hi def link vgxKey_ked                highVgxKeyEnum

"sea
hi def link vgxOperator_sea           highVgxOperatorEnum
hi def link vgxOpcode_sea             highVgxOpcode
hi def link vgxString_sea             highVgxGeneralData
hi def link vgxKeyH_sea               highVgxStringEnum
hi def link vgxKeyL_sea               highVgxStringEnum

"sed
hi def link vgxOperator_sed           highVgxOperatorDelete
hi def link vgxOpcode_sed             highVgxOpcode
hi def link vgxKeyH_sed               highVgxStringEnum
hi def link vgxKeyL_sed               highVgxStringEnum

"nop
hi def link vgxOperator_nop           highVgxOperatorEvent
hi def link vgxOpcode_nop             highVgxOpcode











