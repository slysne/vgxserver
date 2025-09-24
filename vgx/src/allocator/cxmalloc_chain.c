/*######################################################################
 *#
 *# cxmalloc_chain.c
 *#
 *######################################################################
 */

#include "_cxmalloc.h"

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_CXMALLOC );



static cxmalloc_block_t * __cxmalloc_chain__append_block_ACS(       cxmalloc_block_t *last_CS, cxmalloc_block_t *block_CS );
static cxmalloc_block_t * __cxmalloc_chain__insert_block_ACS(       cxmalloc_block_t *previous_CS, cxmalloc_block_t *block_CS );
static               void __cxmalloc_chain__insert_reuse_block_ACS( cxmalloc_block_t *reuse_block_CS );
static               void __cxmalloc_chain__insert_hole_block_ACS(  cxmalloc_block_t *hole_block_CS );
static cxmalloc_block_t * __cxmalloc_chain__remove_block_ACS(       cxmalloc_allocator_t *allocator_CS, cxmalloc_block_t *block_CS );
static               void __cxmalloc_chain__manage_chain_ACS(       cxmalloc_allocator_t *allocator_CS, cxmalloc_block_t *block_CS );
static cxmalloc_block_t * __cxmalloc_chain__advance_block_ACS(      cxmalloc_allocator_t *allocator_CS );
static                int __cxmalloc_chain__remove_holes_OPEN(      cxmalloc_allocator_t *allocator );

DLL_HIDDEN _icxmalloc_chain_t _icxmalloc_chain = {
  .AppendBlock_ACS    = __cxmalloc_chain__append_block_ACS,
  .InsertBlock_ACS    = __cxmalloc_chain__insert_block_ACS,
  .RemoveBlock_ACS    = __cxmalloc_chain__remove_block_ACS,
  .ManageChain_ACS    = __cxmalloc_chain__manage_chain_ACS,
  .AdvanceBlock_ACS   = __cxmalloc_chain__advance_block_ACS,
  .RemoveHoles_OPEN   = __cxmalloc_chain__remove_holes_OPEN
};




/*******************************************************************//**
 * Append a block to the end of the doubly linked list of blocks.
 * last   : the block after which our new block will be appended, or NULL if block is head
 * block  : the block to append after last block
 *
 * Return : the appended block
 ***********************************************************************
 */
static cxmalloc_block_t * __cxmalloc_chain__append_block_ACS( cxmalloc_block_t *last_CS, cxmalloc_block_t *block_CS ) {
  // block in chain
  if( last_CS != NULL ) {
    last_CS->next_block = block_CS;
    block_CS->prev_block = last_CS;
    block_CS->next_block = NULL;
  }
  // block=head
  else {
    block_CS->prev_block = NULL;
    block_CS->next_block = NULL;
  }
  return block_CS;
}



/*******************************************************************//**
 * Insert a block (except head) into the doubly linked list of blocks
 * previous : the block after which our new block is to be inserted (not NULL!)
 * block    : the block to insert
 *
 * Return   : the inserted block
 ***********************************************************************
 */
static cxmalloc_block_t * __cxmalloc_chain__insert_block_ACS( cxmalloc_block_t *previous_CS, cxmalloc_block_t *block_CS ) {
  if( (block_CS->prev_block = previous_CS) != NULL ) {
    if( (block_CS->next_block = previous_CS->next_block) != NULL ) {
      block_CS->next_block->prev_block = block_CS;
    }
    previous_CS->next_block = block_CS;  
    return block_CS;
  }
  else {
    return NULL; // error
  }
}



/*******************************************************************//**
 * Remove a block (possibly head) from the doubly linked list of blocks
 * block    : the block to remove
 *
 * Return   : the next block in the chain, or NULL if block was tail
 ***********************************************************************
 */
static cxmalloc_block_t * __cxmalloc_chain__remove_block_ACS( cxmalloc_allocator_t *allocator_CS, cxmalloc_block_t *block_CS ) {
  cxmalloc_block_t *next_CS;

  // if we're removing the last block in a hole chain, move the end pointer back one block
  if( block_CS == allocator_CS->last_hole ) {
    if( block_CS->prev_block == allocator_CS->last_reuse ) {
      // backing up to to the end of a re-use chain, no more holes exist
      allocator_CS->last_hole = NULL;
    }
    else if( block_CS->prev_block == *allocator_CS->head ) {
      // backing up to the currently active block, no more holes exist
      allocator_CS->last_hole = NULL;
    }
    else {
      // backing up to the previous hole block
      allocator_CS->last_hole = block_CS->prev_block;
    }
  }
  // if we're removing the last block in a re-use chain, move the end pointer back one block
  else if( block_CS == allocator_CS->last_reuse ) {
    if( block_CS->prev_block == *allocator_CS->head ) {
      // backing up to the currently active block, no more re-use blocks exist
      allocator_CS->last_reuse = NULL;
    }
    else {
      // backing up to the previous re-use block
      allocator_CS->last_reuse = block_CS->prev_block;
    }
  }

  // manage linked list
  if( block_CS->prev_block != NULL ) {
    block_CS->prev_block->next_block = block_CS->next_block;
  }
  if( (next_CS = block_CS->next_block) != NULL ) {
    block_CS->next_block->prev_block = block_CS->prev_block;
  }
  block_CS->next_block = block_CS->prev_block = NULL;

  return next_CS;
}



/*******************************************************************//**
 * Insert block into the re-use chain 
 * 
 ***********************************************************************
 */
static void __cxmalloc_chain__insert_reuse_block_ACS( cxmalloc_block_t *reuse_block_CS ) {
  cxmalloc_allocator_t *allocator_CS = reuse_block_CS->parent;
  cxmalloc_block_t *antecedent_CS;
  
  if( allocator_CS->last_reuse != NULL ) {
    // other re-use blocks already exist, insert the new block after the last re-use block
    antecedent_CS = allocator_CS->last_reuse;
  }
  else {
    // no re-use blocks exist, insert the new block immediately after the currently active block
    antecedent_CS = *allocator_CS->head;
  }

  allocator_CS->last_reuse = __cxmalloc_chain__insert_block_ACS( antecedent_CS, reuse_block_CS );
}



/*******************************************************************//**
 * Insert block into the hole chain
 * 
 ***********************************************************************
 */
static void __cxmalloc_chain__insert_hole_block_ACS( cxmalloc_block_t *hole_block_CS ) {
  cxmalloc_allocator_t *allocator_CS = hole_block_CS->parent;
  cxmalloc_block_t *antecedent_CS;

  if( allocator_CS->last_hole != NULL ) {
    // other holes already exist, insert the new block after the last hole
    antecedent_CS = allocator_CS->last_hole;
  }
  else if( allocator_CS->last_reuse != NULL ) {
    // no holes exist, but a re-use chain exists, so insert new block immediately after last re-use block
    antecedent_CS = allocator_CS->last_reuse;
  }
  else {
    // no holes and no re-use blocks exist, insert the new block immediately after the currently active block
    antecedent_CS = *allocator_CS->head;
  }

  allocator_CS->last_hole = __cxmalloc_chain__insert_block_ACS( antecedent_CS, hole_block_CS );
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
__inline static void __cxmalloc_chain__manage_chain_ACS( cxmalloc_allocator_t *allocator_CS, cxmalloc_block_t *block_CS ) {
  cxmalloc_block_t **cursor_CS;
  time_t now;

  /* BLOCK_IS_STILL_IN_USE */
  if( block_CS->reg.put != NULL ) {
    /* ENTER_REUSE_CHAIN */
    if( block_CS != *allocator_CS->head && block_CS->prev_block == NULL && _icxmalloc_block.ComputeAvailable_ARO( block_CS ) > block_CS->reuse_threshold ) {
      // enter chain if block is not head, not already in a chain, and enough free space to be re-used
      __cxmalloc_chain__insert_reuse_block_ACS( block_CS );
    }
  }
  
  /* BLOCK_IS_FREE: All lines have been freed, many things can now happen */
  else {
    /* == Phase 1 == */

    /* EMPTY_IDLE_BLOCK_BECOMES_HOLE: The block is not the active one. We turn it into a hole and enter chain. */
    if( block_CS != *allocator_CS->head ) {
      // a block in the re-use chain (presumably) is fully freed
      _icxmalloc_block.DestroyData_ACS( block_CS );           // gut it
      __cxmalloc_chain__remove_block_ACS( allocator_CS, block_CS );        // yank it out of the re-use chain if it was in a re-use chain (if not, no action)
      __cxmalloc_chain__insert_hole_block_ACS( block_CS );   // enter the hole chain
    }

    /* EMPTY_ACTIVE_BLOCK_BECOMES_HOLE: Block is the active one. Another block has availability, so advance active block to that one and turn former active into a hole */
    else if( block_CS->next_block != NULL && block_CS->next_block->linedata != NULL ) { //
      _icxmalloc_block.DestroyData_ACS( block_CS ); // gut it so advance function works correctly below
      __cxmalloc_chain__advance_block_ACS( allocator_CS ); // move down the re-use chain to another block with free space
      __cxmalloc_chain__insert_hole_block_ACS( block_CS );   // enter the hole chain
    }

    /* EMPTY_ACTIVE_BLOCK_REMAINS_ACTIVE: Block is the active one. No other block has availability, i.e. either empty holes or fully utilized */
    else {
      // Check if all other blocks are holes
      cursor_CS = allocator_CS->blocks;
      while( cursor_CS < allocator_CS->space ) {
        if( *cursor_CS != NULL && *cursor_CS != block_CS && (*cursor_CS)->linedata != NULL ) { // <- check if hole (except our current block of course)
          // block (other than current block) has data, so at least one non-hole exists
          break;
        }
        cursor_CS++;
      }
      // ALLOCATOR_IS_NOT_UTILIZED: Allocator is 100% free
      if( cursor_CS == allocator_CS->space && *(cursor_CS-1) != block_CS ) { 
        // Cursor made it all the way beyond the bottom of the block register and the last slot 
        // is not the active block, i.e. we didn't find any non-holes above, so all other blocks
        // must therefore be holes. Conclusion: this entire allocator is 100% free and should be
        // considered for deletion.

        // Heuristic A: More than 1 blocks in allocator, it has existed for a while and is now unused. Delete.
        if( allocator_CS->space > allocator_CS->blocks + 1) {
          _icxmalloc_block.Delete_ACS( allocator_CS, block_CS ); // will not delete chain of holes if they exist, will do that below
        }
        // Heuristic B: This block is the only block in allocator, it has existed long enough and is unused. Delete.
        else if( time( &now ) > block_CS->min_until ) {
          _icxmalloc_block.Delete_ACS( allocator_CS, block_CS );
        }
        // Heuristic C: Only block in allocator, relatively new, let's not immediately delete it.
        else {
          /* Block will linger forever. At least until a manual scan happens, if we implement that... */
        }
      }
      // ALLOCATOR_REMAINS_UTILIZED: Allocator is still in use 
      else {
        /* do nothing */
      }
    }

    /* == Phase 2 == */
    /* Now we look for trailing holes and prune the register upwards until we hit a non-hole */
    while( allocator_CS->space > allocator_CS->blocks && (block_CS=*(allocator_CS->space-1))->linedata == NULL ) {
      __cxmalloc_chain__remove_block_ACS( allocator_CS, block_CS );   // yank the hole out
      _icxmalloc_block.Delete_ACS( allocator_CS, block_CS );  // get rid of block completely
      *(--(allocator_CS->space)) = NULL;   // move up the register and clear pointer to block we just deleted
    }

    //
    // Pick it up here...
    // we need to ensure the consistency of the block register
    // we need to look for NULL holes in the register
    // we need to compactify register
    // can we move blocks around?
    //
    // .. getting closer now... almost done
    //


    /* == Phase 4 == */
    /* If no other blocks than our current (and fully free) block exist at this point, the allocator is completely un-utilized */
    if( allocator_CS->head == allocator_CS->blocks ) {
      // didn't we just do this with heuristics above???
    }

  }

}



/*******************************************************************//**
 * Move the current block pointer to another block with free space.
 * The block advanced to may be a brand new block, an old block where
 * some space has been freed since it was last active (a re-use block),
 * or an old block where all space has been freed since it was last 
 * active (a hole). If the current block pointer is NULL we just create
 * a new block.
 *
 * allocator  : the allocator for which we want to advance the block pointer
 *
 * Returns    : the block pointed to after advance
 * 
 ***********************************************************************
 */
static cxmalloc_block_t * __cxmalloc_chain__advance_block_ACS( cxmalloc_allocator_t *allocator_CS ) {
  cxmalloc_block_t *previous_CS = NULL;
  cxmalloc_block_t *block_CS = NULL;

  XTRY {
    // Grab the current block
    previous_CS = *allocator_CS->head;
    block_CS = *allocator_CS->head;

    /* --- Situation A: Nonsense, no need to advance */
    if( block_CS && block_CS->reg.get ) {
      /* */
    }
    /* --- Situation B: Block exists and chain exists, follow it --- */
    else if( block_CS && block_CS->next_block != NULL ) {
      // Follow chain to next block, which may be one of two things: 1) a "hole" or 2) a re-use block.
      block_CS = block_CS->next_block;

      /* Chain Scenario 1: We've entered a hole. Re-create data for this block */
      if( block_CS->linedata == NULL ) {
        // Since we're in a hole we must re-create the data
        if( _icxmalloc_block.CreateData_ACS( block_CS ) != 0 ) {
          THROW_CRITICAL( CXLIB_ERR_MEMORY, 0x601 ); // Sorry, we're done.
        }
        // If this was the last block in the hole-chain, erase the hole-chain's end point.
        if( block_CS == allocator_CS->last_hole ) {
          allocator_CS->last_hole = NULL;
        }
#ifndef NDEBUG 
        DEBUG( 0, "ADVANCE: Recreate data in hole-block %u in allocator %u in family: %s",  block_CS->bidx, allocator_CS->aidx, _cxmalloc_id_string(allocator_CS->family) );
#endif
      }

      /* Chain Scenario 2: We're in a re-use chain, so block should have spare capacity */
      else {
        // (A somewhat hysterical reaction if we don't have space capacity, but things are in fact corrupt if this situation arises.)
        if( block_CS->reg.get == NULL ) {
          THROW_FATAL( CXLIB_ERR_CORRUPTION, 0x602 ); // Unwise to continue, so we don't.
        }
        // If this was the last block in the re-use chain, erase the re-use chain's end point.
        if( block_CS == allocator_CS->last_reuse ) {
          allocator_CS->last_reuse = NULL;
        }
#ifndef NDEBUG 
        DEBUG( 0, "ADVANCE: Reuse spare capacity in block %u in allocator %u in family: %s",  block_CS->bidx, allocator_CS->aidx, _cxmalloc_id_string(allocator_CS->family) );
#endif
      }

      // Success, now erase link and adjust block register cursor
      previous_CS->next_block = NULL;  // our predecessor forgets us
      block_CS->prev_block = NULL;     // we forget our predecessor
      allocator_CS->head = allocator_CS->blocks + block_CS->bidx; // update block register cursor to point to the new current block
    }
    /* --- Situation C: No re-use or hole chain, create brand new block if we have capacity --- */
    else if( allocator_CS->space < allocator_CS->end ) { // block register has more capacity
      // Compute bidx at the remain slot and create new block
      cxmalloc_bidx_t bidx = (cxmalloc_bidx_t)(allocator_CS->space - allocator_CS->blocks);
      // Create a new block at this bidx
      if( (block_CS = _icxmalloc_block.New_ACS( allocator_CS, bidx, true )) == NULL ) {
        THROW_CRITICAL( CXLIB_ERR_MEMORY, 0x603 );
      }
      // Adjust block register cursor and advance the remain pointer
      allocator_CS->head = allocator_CS->space++;  // Move cursor directly to end of block register
      *allocator_CS->head = block_CS;               // Hook up block register slot to the new block

#ifndef NDEBUG 
      DEBUG( 0, "ADVANCE: New block %u in allocator %u in family: %s",  bidx, allocator_CS->aidx, _cxmalloc_id_string(allocator_CS->family) );
#endif
    }
    /* --- Situation D: Design limit reached */
    else {
      // Will probably not happen soon. For instance, if blocks are 512MB then
      // we're at 32 Terabytes for a single allocator aidx at this point. (45 bits address.)
      THROW_CRITICAL_MESSAGE( CXLIB_ERR_CAPACITY, 0x604, "Allocator capacity reached (%llu blocks), cannot continue in family: %s", CXMALLOC_BLOCK_REGISTER_SIZE, _cxmalloc_id_string(allocator_CS->family) );
      // TODO: Handle this. Allow multiple allocators at same aidx? (bidx is 16-bit and part of linehead struct, 
      // NOTE that limit applies per aidx per allocator family. In applications there will be many allocator
      // families, each with a set of allocators, easily able to use 256 TB total, which as of June 2014 is
      // the theoretical limit for AMD64 architecture. If it's 2020 and you're reading this, feel free to laugh.
    }

    // Just to be clear...
    if( *allocator_CS->head != block_CS ) {
      THROW_FATAL( CXLIB_ERR_BUG, 0x605 );
    }

  }
  XCATCH( errcode ) {
    if( block_CS && block_CS != previous_CS ) {
      _icxmalloc_block.Delete_ACS( allocator_CS, block_CS ); 
    }
    block_CS = NULL;
  }
  XFINALLY {
  }

  return block_CS;
}



/*******************************************************************//**
 * Run allocator cleanup. All holes that have bidx greater than any
 * block that is not a hole will be removed. Holes that have bidx less
 * than a populated block will not be removed. (This is necessary to
 * ensure a contiguous set of blocks 0-N which the allocator requires.)
 * 
 ***********************************************************************
 */
static int __cxmalloc_chain__remove_holes_OPEN( cxmalloc_allocator_t *allocator ) {
  int removed_holes = 0;

  BEGIN_CXMALLOC_ALLOCATOR_READONLY( allocator ) {
    // Find the bidx for the last block that is not a hole
    cxmalloc_bidx_t last_populated_bidx = 0;
    cxmalloc_block_t **cursor = allocator_RO->blocks;
    cxmalloc_block_t **end = allocator_RO->space;
    while( cursor < end ) {
      if( (*cursor)->linedata ) {
        last_populated_bidx = (*cursor)->bidx;
      }
      cursor++;
    }

    // Traverse chain and remove holes beyond the last populated bidx
    cxmalloc_block_t *block_RO = *allocator_RO->head;
    while( block_RO ) {
      cxmalloc_block_t *next_RO = block_RO->next_block;

      // hole beyond last populated block
      if( block_RO->linedata == NULL && block_RO->bidx > last_populated_bidx ) {
IGNORE_WARNING_DEREFERENCING_NULL_POINTER
        SYNCHRONIZE_CXMALLOC_ALLOCATOR( allocator_RO ) {
          cxmalloc_block_t *block_CS = block_RO;
          // NOTE: We are about to modify a readonly allocator. This is technically inconsistent with readonly
          // but since we also hold the allocator lock we are guaranteed no other thread will modify the allocator
          // and also can't enter it's own readonly section (since this requires the lock briefly).
          _icxmalloc_block.Delete_ACS( allocator_RO_CS, block_CS );
        } RELEASE_CXMALLOC_ALLOCATOR;
RESUME_WARNINGS
        removed_holes++;
      }
      block_RO = next_RO;
    }
  } END_CXMALLOC_ALLOCATOR_READONLY;

  // NOTE: Some holes may remain if they exist before the last populated bidx

  // (Lock is released, Intellisense is confused)
  SUPPRESS_WARNING_UNBALANCED_LOCK_RELEASE
  return removed_holes;
}
