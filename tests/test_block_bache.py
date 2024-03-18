import pytest
from block_device.block_cache import Block, BlockCache  # Adjust according to your project structure

class TestBlock:
    @pytest.fixture
    def block(self):
        """Fixture to create a Block instance for each test method."""
        blk_cache = BlockCache(4096, 8)
        blk_id = 1
        blk_size = 1024
        return Block(blk_cache, blk_id, blk_size)

    def test_initialization(self, block):
        """Test that a Block is correctly initialized."""
        assert block.blk_id == 1
        assert block.ref_count == 0
        assert not block.is_dirty
        assert not block.is_uptodate
        assert isinstance(block.buffer, bytearray)
        
        assert isinstance(block.blk_cache, BlockCache)
        actual_buffer_len = len(block.buffer)
        assert actual_buffer_len == 1024
        assert actual_buffer_len == block.blk_size

    def test_dirty_flag(self, block):
        """Test setting and clearing the dirty flag."""
        assert not block.is_dirty
        block.set_dirty()
        assert block.is_dirty
        block.clear_dirty()
        assert not block.is_dirty

    def test_uptodate_flag(self, block):
        """Test setting and clearing the uptodate flag."""
        assert not block.is_uptodate
        block.set_uptodate()
        assert block.is_uptodate
        block.clear_uptodate()
        assert not block.is_uptodate

    def test_reference_count(self, block):
        """Test incrementing and decrementing the reference count."""
        assert block.ref_count == 0
        block.ref_inc()
        assert block.ref_count == 1
        block.ref_dec()
        assert block.ref_count == 0

        # Ensure decrementing below zero raises an assertion error
        with pytest.raises(AssertionError):
            block.ref_dec()
            
    def test_reset_block_id(self, block):
        def __check_block_valid(block):
            assert block.ref_count == 0
            assert not block.is_dirty
            assert not block.is_uptodate
        block.set_blk_id(2)
        __check_block_valid(block)
        block.ref_inc()
        assert block.ref_count == 1
        block.set_blk_id(2)
        __check_block_valid(block)
        block.set_blk_id(3)
        __check_block_valid(block)

@pytest.fixture
def block_cache() -> BlockCache:
    """Fixture that provides a BlockCache instance with predefined settings."""
    blk_size = 1024  # Example block size
    blk_limit = 5  # Example limit of blocks in cache
    return BlockCache(blk_size, blk_limit)

class TestBlockCache:
    def test_alloc_block_success(self, block_cache: BlockCache):
        """Tests successful allocation of a new block in the cache."""
        blk_id = 1
        block = block_cache.alloc_block(blk_id)
        assert block is not None
        assert block.blk_id == blk_id
        # Assume alloc_block actually adds the block to the cache
        assert blk_id in block_cache._BlockCache__cache
        assert block.blk_size == block_cache.blk_size
        # Allocate a block with a used block id. It should be failed
        block2 = block_cache.alloc_block(blk_id)
        assert block2 is None

    def test_alloc_block_failure_due_to_limit(self, block_cache: BlockCache):
        """Tests failure to allocate a new block due to reaching the cache limit."""
        for blk_id in range(1, block_cache._BlockCache__blk_limit + 1):
            block = block_cache.alloc_block(blk_id)
            assert block is not None
        
        # Attempt to exceed limit
        block = block_cache.alloc_block(block_cache._BlockCache__blk_limit + 2)
        
        # Assuming the cache limit prevents the last block from being added
        assert len(block_cache._BlockCache__cache) == block_cache._BlockCache__blk_limit
        assert block is None  # The last attempt to allocate a block should fail

    def test_put_block_decrements_ref_count(self, block_cache: BlockCache):
        """Tests that putting a block back into the cache decrements its reference count."""
        blk_id = 1
        block = block_cache.alloc_block(blk_id)
        assert block.ref_count == 1
        block_ref = block_cache.find_get_block(blk_id)
        assert block_ref is block
        assert block_ref.ref_count == 2
        block_cache.put_block(block_ref)
        assert block_ref.ref_count == 1
        block_cache.put_block(block)
        assert block.ref_count == 0

    @pytest.mark.skip
    def test_least_used_block(self, block_cache: BlockCache):
        """Tests retrieving the least used block from the cache."""
        # Populate cache with some blocks and simulate usage
        allocated_blocks = []
        for blk_id in range(1, 4):
            block = block_cache.alloc_block(blk_id)
            assert isinstance(block, Block)
            allocated_blocks.append(block)
            block_cache.put_block(block)
        
        least_used_block = block_cache.least_used_block()
        # Assuming the first block is the least used
        assert least_used_block is not None
        assert least_used_block.blk_id == 1  

    @pytest.mark.skip
    def test_drop_block(self, block_cache: BlockCache):
        """Tests dropping a block from the cache."""
        blk_id = 1
        block = block_cache.alloc_block(blk_id)  # Add a block to the cache
        block_cache.put_block(block)
        block = block_cache.find_get_block(blk_id)  # Retrieve the added block
        result = block_cache.drop_block(block)
        assert result is True
        assert blk_id not in block_cache._BlockCache__cache

