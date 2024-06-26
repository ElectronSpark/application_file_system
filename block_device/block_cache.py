from typing import Optional


class Block(object):
    """
    Represents a single cache block within a BlockCache, tracking its state and data.

    Each block is initialized with a reference count of 0 and both 'uptodate' and 'dirty' flags set to False.
    'uptodate' indicates if the block's content is current, while 'dirty' signifies if it has been modified.
    """

    def __init__(self, blk_cache: 'BlockCache', blk_id: int, blk_size: int):
        """
        Initializes a new cache block.

        Args:
            blk_cache: The BlockCache instance this block belongs to.
            blk_id: Unique identifier for the block within the block device.
            blk_size: Size of the block in bytes.
        """
        assert isinstance(blk_cache, BlockCache), "blk_cache must be an instance of BlockCache"
        assert blk_id >= 0, "blk_id must be a non-negative integer"
        assert blk_size > 0, "blk_size must be a positive integer"

        self.__blk_cache = blk_cache
        self.__blk_id = blk_id
        self.__blk_size = blk_size
        self.__blk_buf = bytearray(blk_size)
        self.__ref_cnt = 0
        self.__uptodate = False
        self.__dirty = False

    # def __del__(self):
    #     """
    #     Handles cleanup upon deletion of the cache block, issuing warnings if the block is dirty or still referenced.
    #     """
    #     if self.__ref_cnt > 0:
    #         print(f"Warning: Deleting a cache block with reference count {self.__ref_cnt} > 0.")
    #     if self.__dirty:
    #         print("Warning: Deleting a dirty cache block which needs to be written back.")

    @property
    def blk_cache(self) -> 'BlockCache':
        """
        The parent Block Cache of the cached block.
        """
        return self.__blk_cache
        
    @property
    def blk_size(self) -> 'BlockCache':
        """
        The size of the cached block in bytes.
        """
        return self.__blk_size

    @property
    def blk_id(self) -> int:
        """
        The block ID in the block device of the cached block.
        """
        return self.__blk_id

    @property
    def buffer(self) -> bytearray:
        """
        The buffer of this cache block as a bytearray.
        """
        return self.__blk_buf

    @property
    def is_dirty(self) -> bool:
        """
        Whether the block is modified and needs synchronization with the block device.
        """
        return self.__dirty

    def set_dirty(self):
        """
        Sets the dirty status of the cache block to True.
        """
        self.__dirty = True
    
    def clear_dirty(self):
        """
        Sets the dirty status of the cache block to False.
        """
        self.__dirty = False

    @property
    def is_uptodate(self) -> bool:
        """
        Whether the block has loaded the latest content from the block device.
        """
        return self.__uptodate

    def set_uptodate(self):
        """
        Sets the up to date status of the cache block to True.
        """
        self.__uptodate = True
    
    def clear_uptodate(self):
        """
        Sets the up to date status of the cache block to False.
        """
        self.__uptodate = False
        
    def move_blk_cache(self, new_cache, new_blk_id):
        """
        change block id and the cache it belongs to. This method will also 
        reset the block's dirty bit and uptodate bit.
        
        This should not be called by users.
        """
        assert new_blk_id >= 0
        if self.__ref_cnt != 0:
            print("warning: changing a block id of a cach block whos ref_count {} != 0".format(self.__ref_cnt))
        if self.is_dirty:
            print("warning: changing a block id of a dirty block")
        self.__blk_cache = new_cache
        self.__blk_id = new_blk_id
        self.__ref_cnt = 0
        self.clear_dirty()
        self.clear_uptodate()

    @property
    def ref_count(self) -> int:
        """
        The current reference count of the block.
        """
        return self.__ref_cnt

    def ref_inc(self):
        """
        Increments the reference count of the block by 1.
        """
        self.__ref_cnt += 1

    def ref_dec(self):
        """
        Decrements the reference count of the block by 1, ensuring it never goes below zero.
        """
        assert self.__ref_cnt > 0, "Reference count cannot be decremented below 0."
        if self.is_dirty:
            print("warning: dereferencing a dirty block!")
        self.__ref_cnt -= 1


class BlockCache(object):
    """
    A cache for temporarily storing blocks in RAM to reduce IO operations to a block device.
    
    The filesystem loads blocks into this cache before reading or writing, and writes them back after modifications,
    utilizing a least-recently-used (LRU) policy for managing cache space.
    """
    def __init__(self, blk_size: int, blk_limit: int):
        """
        Initializes a BlockCache instance.

        Args:
            blk_size: Size of each block in bytes.
            blk_limit: Maximum number of blocks allowed in the cache.
        """
        
        assert blk_size > 0, "block size must be greater than 0"
        assert blk_limit > 0, "block limit must be greater than 0"
        
        self.__cache = {}  # Block ID mapped to Block instance.
        self.__dirty_queue = [] # Block ID mapped to dirty Block.
        self.__lru_queue = []  # List of Block IDs to manage LRU.
        self.__blk_size = blk_size
        self.__blk_limit = blk_limit
        
    @property
    def blk_limit(self):
        """The maximum number of blocks the cache can hold.
        
        Returns:
            The maximum limit of blocks that can be stored in the cache.
        """
        return self.__blk_limit
    
    @property
    def blk_size(self):
        """The size of each block in bytes.
        
        Returns:
            The size of a single block in bytes, as defined during the initialization of the cache.
        """
        return self.__blk_size
    
    @property
    def count(self):
        """The current number of blocks stored in the cache.
        
        Returns:
            The total number of blocks currently in the cache. This is not the total size of the cache, but rather
            the count of individual blocks it contains.
        """
        return self.__cache_count()
    
    @property
    def lru_count(self):
        """The current number of blocks in the LRU (Least Recently Used) list.
    
        This list contains the block IDs of blocks that are not currently in
        active use(those with a reference count of 0 and an unset
        dirty bit). When a block is accessed from the block device and it exists
        in the LRU list, its block ID is removed from this list. Additionally,
        when memory is constrained, blocks from the LRU list may be released or
        evicted based on their order in the list, starting with the least
        recently used ones.
        
        Returns:
            int: The total count of blocks within the LRU list.
        """
        return self.__lru_count()

    @property
    def dirty_count(self):
        """The current number of dirty blocks.
        
        Returns:
            The number of dirty blocks currently in the cache.
        """
        return self.__dirty_count()
    
    @property
    def is_full(self) -> bool:
        """Determines if the cache has reached its maximum capacity.
        
        Compares the current count of blocks in the cache against its maximum
        limit to determine if the cache can no longer accommodate additional
        blocks.
        
        Returns:
            A boolean value: True if the cache is full and cannot store more
            blocks, False otherwise.
        """
        return self.__blk_limit <= self.count and self.__is_lru_empty()
    
    def get_dirty_block(self) -> Optional['Block']:
        """Get an inactive dirty block.
        
        Attempts to retrieve a dirty cache block with a reference count of 0
        from the dirty queue. The cache block returned will be removed from the
        dirty queue.
        
        Returns:
            Optional[Block]: A cache block instance if available in the dirty
            queue; otherwise, None.
        """
        if self.__is_dirty_empty():
            return None
        
        blk_id = self.__get_dirty()
        block = self.find_get_block(blk_id)
        assert isinstance(block, Block), "Failed to get a block in dirty list from cache"
        return block

    def get_lru_block(self) -> Optional['Block']:
        """Get an inactive block that has been written back.
        
        Retrieves a cache block from the tail of the LRU list, indicating it
        has been synchronized with the storage medium and holds no active
        references. The cache block returned will be removed from the LRU list.
        
        Returns:
            Optional[Block]: A cache block instance if the LRU list is not
            empty; otherwise, None.
        """
        if self.__is_lru_empty():
            return None
        
        blk_id = self.__get_lru()
        block = self.find_get_block(blk_id)
        assert isinstance(block, Block), "Failed to get a block in lru list from cache"
        return block

    def drop_block(self, block: 'Block') -> bool:
        """Drop a block from the cache.
        
        Attempts to remove a block from the cache. This method does not delete
        the block instance itself. If the block is currently referenced
        (reference count > 1), the method will not proceed and will return
        False.
        
        Args:
            block: The block to be removed from the cache.
            
        Returns:
            bool: True if the block was successfully removed; False otherwise.
        """
        if not self.__contains_block(block):
            # Can not drop a block from a block cache where it doesn't beong to
            return False
        
        if block.ref_count > 1:
            return False
        
        popped_block = self.__pop_block(block.blk_id)
        assert popped_block is block
        
        return True
            
    def find_get_block(self, blk_id: int) -> Optional['Block']:
        """Attempts to find and return a block by its ID from the cache or the
        LRU list.
        
        If found, the block's reference count is incremented. Blocks not found
        in the cache or LRU list return None. Users should decrement the
        block's reference count by calling "put_block" after usage.
        
        Args:
            blk_id: The ID of the block to retrieve.
        
        Returns:
            Optional[Block]: An instance of the Block if found; otherwise, None.
        """
        block = self.__get_block(blk_id)
        if block is not None:
            assert block.blk_id == blk_id
            if block.ref_count == 0:
                if block.is_dirty:
                    assert self.__is_in_dirty(blk_id)
                    self.__remove_dirty(blk_id)
                else:
                    assert self.__is_in_lru(blk_id)
                    self.__remove_lru(blk_id)
            block.ref_inc()
        return block

    def put_block(self, blk: 'Block'):
        """
        Decrements the reference count of a given block.
        
        If the block's reference count reaches 0, its subsequent handling
        depends on its dirty bit status. Dirty blocks without references are
        added to the dirty list, whereas synchronized blocks without
        references are added to the LRU list.
        
        Args:
            blk: The block to modify.
        """
        assert isinstance(blk, Block)
        assert self.__contains_block(blk)
        
        blk.ref_dec()
        if blk.ref_count == 0:
            if blk.is_uptodate:
                # When the block is up to date, it is added to the LRU list for 
                # potential future use.
                if blk.is_dirty:
                    # Dirty blocks without reference is added to dirty list.
                    # Blocks in dirty list need to be written-back.
                    self.__push_dirty(blk.blk_id)
                else:
                    self.__push_lru(blk.blk_id)
            else:
                # Release blocks that are not up to date.
                popped_block = self.__pop_block(blk.blk_id)
                assert popped_block is blk
    
    def alloc_block(self, blk_id: int) -> Optional['Block']:
        """
        Allocates and inserts a new block into the cache if there is space 
        available or if an old block can be evicted.

        Args:
            blk_id: The ID for the new block.

        Returns:
            The newly allocated Block if successful; otherwise, None.
        """
        assert isinstance(blk_id, int)
        assert blk_id >= 0
        
        if self.__cache_count() >= self.__blk_limit:
            if self.__is_lru_empty():
                return None
            
            popped_blk_id = self.__get_lru()
            self.__remove_lru(popped_blk_id)
            if popped_blk_id == blk_id:
                block = self.__get_block(popped_blk_id)
                assert isinstance(block, Block)
            else:
                # need to reinsert the block with a new block id.
                block = self.__pop_block(popped_blk_id)
                assert isinstance(block, Block)
                block.move_blk_cache(self, blk_id)
                assert self.__add_block(block)
            block.ref_inc()
            return block
        
        if not self.__get_block(blk_id) is None:
            # cannot create a block with a used block id.
            return None
        
        block = Block(self, blk_id, self.__blk_size)
        block.ref_inc()
        if not self.__add_block(block):
            # Return None if failed to add the 
            return None
        return block
    
    
    """
    Private methods for LRU list
    """
    def __remove_lru(self, blk_id: int):
        """
        Remove a block id from LRU list. No any validation check will be performed.
        """
        self.__lru_queue.remove(blk_id)
        
    def __is_in_lru(self, blk_id: int) -> bool:
        """
        Check if a block id is in LRU list.
        """
        return blk_id in self.__lru_queue
    
    def __push_lru(self, blk_id: int):
        """
        Add a block id to LRU list. No any validation check will be performed.
        """
        self.__lru_queue.append(blk_id)
    
    def __get_lru(self) -> Optional['int']:
        """
        Get a block id at the end of LRU list. No any validation check will be
        performed.
        """
        return self.__lru_queue[0]
    
    def __lru_count(self):
        """
        The current number of blocks stored in the LRU list.
        """
        return len(self.__lru_queue)
    
    def __is_lru_empty(self) -> bool:
        """
        Check if the LRU list is empty.
        """
        return self.__lru_count() == 0
    
    
    """
    Private methods for dirty list
    """
    def __push_dirty(self, blk_id: int):
        """
        push a Block ID into dirty list.
        """
        self.__dirty_queue.append(blk_id)
    
    def __get_dirty(self) -> Optional['int']:
        """
        get the first Block ID stored in dirty list.
        """
        return self.__dirty_queue[0]
    
    def __remove_dirty(self, blk_id: int):
        """
        remove a Block ID from dirty list.
        """
        self.__dirty_queue.remove(blk_id)
        
    def __is_in_dirty(self, blk_id: int) -> bool:
        """
        check if a blk_id is stored in dirty list.
        """
        return blk_id in self.__dirty_queue
    
    def __dirty_count(self):
        """
        The current number of dirty blocks.
        """
        return len(self.__dirty_queue)

    def __is_dirty_empty(self):
        """
        Check if the dirty list is empty.
        """
        return self.__dirty_count() == 0
        
    
    """
    Private methods for cache
    """
    def __add_block(self, block: 'Block') -> bool:
        """
        Add a block to the block cache. Return True if succeed.
        """
        if not block.blk_cache is self:
            return False
        if block.blk_id in self.__cache:
            return False
        self.__cache[block.blk_id] = block
        return True
    
    def __cache_count(self):
        """
        The current number of blocks stored in the cache.
        """
        return len(self.__cache)
        
    def __contains_block(self, block: Block) -> bool:
        """
        To check if the given block belongs to this block cache.
        """
        if not block.blk_cache is self:
            return False
        # The following are thorough checks, may be descarded in the future.
        return block.blk_id in self.__cache
    
    def __get_block(self, blk_id: int) -> Optional['Block']:
        """
        Find a block with given block id from the cache. Return None if not found.
        """
        block = self.__cache.get(blk_id, None)
        if not block is None:
            assert block.blk_id == blk_id, "Find a block with a inconsistent blk_id"
        return block
    
    def __pop_block(self, blk_id: int) -> Optional['Block']:
        """
        Find and drop a block with given block id from the cache.
        Return None if not found.
        """
        block = self.__cache.pop(blk_id, None)
        if not block is None:
            assert block.blk_id == blk_id, "Find a block with a inconsistent blk_id"
            block.move_blk_cache(None, blk_id)
        return block
    