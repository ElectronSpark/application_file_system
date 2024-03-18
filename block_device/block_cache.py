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

    def __del__(self):
        """
        Handles cleanup upon deletion of the cache block, issuing warnings if the block is dirty or still referenced.
        """
        if self.__ref_cnt > 0:
            print(f"Warning: Deleting a cache block with reference count {self.__ref_cnt} > 0.")
        if self.__dirty:
            print("Warning: Deleting a dirty cache block which needs to be written back.")

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
        
    def set_blk_id(self, new_blk_id):
        """
        change block id
        """
        assert new_blk_id >= 0
        self.__blk_id = new_blk_id

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
        
        self.__cache = {}  # Block ID mapped to Block instance for O(1) access.
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
        return len(self.__cache)
    
    @property
    def is_full(self) -> bool:
        """Determines if the cache has reached its maximum capacity.
        
        Compares the current count of blocks in the cache against its maximum limit to determine if
        the cache can no longer accommodate additional blocks.
        
        Returns:
            A boolean value: True if the cache is full and cannot store more blocks, False otherwise.
        """
        return self.__blk_limit <= self.count
    
    def find_get_block(self, blk_id: int) -> Optional['Block']:
        """
        Attempts to find and return a block by its ID from the cache or the LRU list.

        If found, the block's reference count is incremented. Blocks not found in the cache return None.
        Users should decrement the block's reference count by calling "put_block" after usage.

        Args:
            blk_id: The ID of the block to retrieve.

        Returns:
            An instance of Block if found; otherwise, None.
        """
        block = self.__cache.get(blk_id, None)
        if block is not None:
            if block.ref_count == 0:
                self.__lru_queue.remove(block.blk_id)
            block.ref_inc()
        return block

    def put_block(self, blk: 'Block'):
        """
        Decrements the reference count of a given block.

        If the block's reference count reaches 0, it is moved to the LRU list.

        Args:
            blk: The block to modify.
        """
        assert isinstance(blk, Block)
        assert blk.blk_cache is self, "The block belongs to another block cache."
        assert self.__cache.get(blk.blk_id, None) is blk
        
        blk.ref_dec()
        if blk.ref_count == 0:
            self.__lru_queue.append(blk.blk_id)
    
    def least_used_block(self) -> Optional['Block']:
        """
        Retrieves the least used block from the LRU list.

        Returns the block at the tail of the LRU list, indicating it is the least recently used.

        Returns:
            The least recently used block if available; otherwise, None.
        """
        if len(self.__lru_queue) == 0:
            return None
        blk_id = self.__lru_queue.pop(0)
        block = self.__cache.get(blk_id, None)
        assert block is not None    # A block in lru list must also be in cache
        assert block.ref_count == 0, "find a block in lru who's reference count is not 0."
        block.ref_inc()
        return block
    
    def alloc_block(self, blk_id: int) -> Optional['Block']:
        """
        Allocates and inserts a new block into the cache if there is space available or if an old block can be evicted.

        Args:
            blk_id: The ID for the new block.

        Returns:
            The newly allocated Block if successful; otherwise, None.
        """
        assert isinstance(blk_id, int)
        assert blk_id >= 0
        
        if self.is_full:
            block = self.least_used_block()
            if block is None or block.blk_id == blk_id:
                return block
            # need to reinsert the block with a new block id.
            block1 = self.__cache.pop(blk_id, None)
            assert block1 is block
            self.__cache[blk_id] = block1
            return block1
        
        if blk_id in self.__cache:
            # cannot create a block with a used block id.
            return None
        
        block = Block(self, blk_id, self.__blk_size)
        block.ref_inc()
        self.__cache[blk_id] = block
        return block
    
    def drop_block(self, blk: 'Block') -> bool:
        """
        Removes a specified block from the cache.
        
        @TODO: this method should be a non-user method

        Args:
            blk: The block to remove.

        Returns:
            True if the block was successfully removed; False otherwise.
        """
        assert isinstance(blk, Block)
        assert blk.blk_cache is self, "The block belongs to another block cache."
        
        blk_id = blk.blk_id
        block = self.__cache.pop(blk_id, None)
        if block is None:
            return False

        return True
        