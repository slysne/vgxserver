/*######################################################################
 *#
 *# _op_scf.h
 *#
 *#
 *######################################################################
 */


#ifndef _VXDURABLE_OP_SCF_H
#define _VXDURABLE_OP_SCF_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_system_similarity get__op_system_similarity( vgx_Similarity_config_t *simconfig ) {
  op_system_similarity opdata = {
    .op  = OPERATOR_SYSTEM_SIMILARITY,
    .sim = simconfig ? *simconfig : DEFAULT_EUCLIDEAN_SIMCONFIG
  };
  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_system_similarity( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_system_similarity, parser ) {

    DWORD dw;

    switch( parser->field++ ) {

    // opcode
    case 0:
      OPERATOR_CONTINUE( op );

    // sim.fingerprint.nsegm
    case 1:
      if( !hex_to_DWORD( parser->token.data, &dw ) ) {
        OPERATOR_ERROR( op );
      }
      op->sim.fingerprint.nsegm = (int)dw;
      OPERATOR_CONTINUE( op );

    // sim.fingerprint.nsign
    case 2:
      if( !hex_to_DWORD( parser->token.data, &dw ) ) {
        OPERATOR_ERROR( op );
      }
      op->sim.fingerprint.nsign = (int)dw;
      OPERATOR_CONTINUE( op );

    // sim.vector.max_size
    case 3:
      if( !hex_to_DWORD( parser->token.data, &dw ) ) {
        OPERATOR_ERROR( op );
      }
      op->sim.vector.max_size = (uint16_t)dw;
      OPERATOR_CONTINUE( op );

    // sim.vector.min_intersect
    case 4:
      if( !hex_to_DWORD( parser->token.data, &dw ) ) {
        OPERATOR_ERROR( op );
      }
      op->sim.vector.min_intersect = (int)dw;
      OPERATOR_CONTINUE( op );

    // sim.vector.min_cosine
    case 5:
      if( !hex_to_DWORD( parser->token.data, &dw ) ) {
        OPERATOR_ERROR( op );
      }
      *(DWORD*)&op->sim.vector.min_cosine = dw;
      OPERATOR_CONTINUE( op );

    // sim.vector.min_jaccard
    case 6:
      if( !hex_to_DWORD( parser->token.data, &dw ) ) {
        OPERATOR_ERROR( op );
      }
      *(DWORD*)&op->sim.vector.min_jaccard = dw;
      OPERATOR_CONTINUE( op );

    // sim.vector.cosine_exponent
    case 7:
      if( !hex_to_DWORD( parser->token.data, &dw ) ) {
        OPERATOR_ERROR( op );
      }
      *(DWORD*)&op->sim.vector.cosine_exponent = dw;
      OPERATOR_CONTINUE( op );

    // sim.vector.jaccard_exponent
    case 8:
      if( !hex_to_DWORD( parser->token.data, &dw ) ) {
        OPERATOR_ERROR( op );
      }
      *(DWORD*)&op->sim.vector.jaccard_exponent = dw;
      OPERATOR_CONTINUE( op );

    // sim.threshold.hamming
    case 9:
      if( !hex_to_DWORD( parser->token.data, &dw ) ) {
        OPERATOR_ERROR( op );
      }
      op->sim.threshold.hamming = (int)dw;
      OPERATOR_CONTINUE( op );

    // sim.threshold.similarity
    case 10:
      if( !hex_to_DWORD( parser->token.data, &dw ) ) {
        OPERATOR_ERROR( op );
      }
      *(DWORD*)&op->sim.threshold.similarity = dw;
      // *** EXECUTE ***
  #ifdef PARSE_DUMP
      PARSER_VERBOSE( parser, 0x001, "op_system_similarity: %s %08x nsegm=%d nsign=%d max_size=%u min_intersect=%d min_cosine=%g min_jaccard=%g cos_exp=%g jac_exp=%g ham=%d sim=%g", 
                                                            op->op.name,
                                                               op->op.code,
                                                                          op->sim.fingerprint.nsegm,
                                                                                   op->sim.fingerprint.nsign,
                                                                                               op->sim.vector.max_size,
                                                                                                                 op->sim.vector.min_intersect,
                                                                                                                              op->sim.vector.min_cosine,
                                                                                                                                              op->sim.vector.min_jaccard,
                                                                                                                                                        op->sim.vector.cosine_exponent,
                                                                                                                                                                    op->sim.vector.jaccard_exponent,
                                                                                                                                                                            op->sim.threshold.hamming,
                                                                                                                                                                                   op->sim.threshold.similarity );

  #endif
      // TODO
      OPERATOR_COMPLETE( op );

    // ERROR
    default:
      OPERATOR_ERROR( op );
    }

  } END_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __execute_op_system_similarity( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_system_similarity, parser ) {
  } END_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_system_similarity( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_system_similarity, parser ) {
  } END_OPERATOR_RETURN;
}




#endif
