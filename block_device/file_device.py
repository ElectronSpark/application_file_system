from ..prototypes.block_dev import BlockDevice
from block_cache import BlockCache
from threading import Lock
from typing import Optional
import os


class FileDevice(BlockDevice):
    def __init__(self, dev_path: str, block_size: int=4096, cache_size: int=16):
        """
    Initializes a new FileDevice instance, representing a file-based block device.
    
    This constructor sets up the file device with a specific block size and cache size, ensuring
    that the block size is at least 512 bytes and the cache can hold at least 5 blocks. It prepares
    the device for operation but does not open the device file immediately; the `open` method must
    be called to begin IO operations.

    Args:
        dev_path (str): The file system path to the device file. If the file does not exist, it will
                        be created when the device is opened.
        block_size (int): The size of each block in bytes. Defaults to 4096. Must be at least 512 bytes.
        cache_size (int): The number of blocks that the cache can hold. Defaults to 16. Must be greater than 4.

    Raises:
        AssertionError: If the provided block size is less than 512 bytes or if the cache size is not
                        greater than 4 blocks.

    The constructor asserts the validity of the provided block and cache sizes to ensure they meet
    minimum requirements. It initializes internal attributes including the device path, block size,
    cache size, and a lock for thread-safe operations. The block cache and file descriptor are initialized
    as None and will be set when the device is opened.
    """
        assert block_size >= 512, "The minimun size for a file block device is 512 Bytes."
        assert cache_size > 4, "The minumum cache size is 16 blocks."
        
        super().__init__()
        
        self.__block_size = block_size
        self.__cache_size = cache_size
        self.__dev_path = dev_path
        self.__block_cache = None
        self.__fd = None
        self.__dev_lock = Lock()
        
    def open(self) -> bool:
        """Opens the device.
        
        Opens the corresponding file on the host OS and initializes the device. 
        If the file block device is already open, this method will not perform
        any action.
        
        Returns:
            bool: True if successfully opened.
        """
        if self.__is_open():
            return True
        
        return self.__do_open()
    
    def close(self) -> bool:
        """Closes the device.
        
        Closes the block device, releasing its block cache, and then closes the
        file on the host OS.
        If the file block device is already closed, this method will not
        perform any action.
        
        @TODO: Flush the device before closing.
        
        Returns:
            bool: True if successfully closed.
        """
        if not self.__is_open():
            return True
        
        return self.__do_close()
    
    def block_read(self, blk_id: int, blk_cnt: int) -> Optional['bytearray']:
        """Reads blocks from the device.
        
        Attempts to retrieve blocks from the Block Cache first. If the blocks 
        are found in the Block Cache and are up to date, it reads the data 
        from the cache. If not found in the Block Cache, it reads from the 
        device file and stores the blocks in the Block Cache.
        
        Args:
            blk_id (int): The block ID of the first block to read.
            blk_cnt (int): The number of blocks to read.
        
        Returns:
            Optional[bytearray]: The content of the blocks read if successful;
            otherwise, None.
        """
        pass
    
    def block_write(self, blk_id: int, blk_cnt: int, buf: bytes) -> int:
        """Writes blocks to the device.
        
        Writes blocks from the given buffer to the device's Block Cache first.
        To persist changes, the device must be flushed to ensure all changes
        are written back.
        
        Args:
            blk_id (int): The block ID of the first block to write.
            blk_cnt (int): The number of blocks to write.
            buf (bytes): The buffer storing data to write.
        
        Returns:
            int: The actual number of blocks written.
        """
        pass
    
    def flush(self) -> bool:
        """Flushes the device's Block Cache.
        
        Writes all dirty blocks in the Block Cache back to the device.
        
        Returns:
            bool: True if successful.
        """
        pass
    
    def lock(self) -> bool:
        """Acquires the device lock.
        
        Returns:
            bool: True if the lock is successfully acquired.
        """
        return self.__dev_lock.acquire()
    
    def unlock(self) -> None:
        """Releases the device lock."""
        return self.__dev_lock.release()
        
    def __is_open(self) -> bool:
        """Checks if the device is open.
        
        For a file block device, it is considered open if its file descriptor
        is not None.
        
        Returns:
            bool: True if the device is open.
        """
        return self.__fd is not None
    
    def __do_open(self) -> bool:
        """Performs the actual device opening process.
        
        Creates the Block Cache to reduce IO traffic and attempts to open or
        create the device file.
        
        Returns:
            bool: True if successful.
        """
        
        # Create Block Cache to reduce IO traffic
        block_cache  = BlockCache(blk_size=self.__block_size, blk_limit=self.__cache_size)
        
        # Open or create the device's file.
        try:
            if not os.path.exists(self.__dev_path):
                open_text_mode = "xb+"
            else:
                open_text_mode = "rb+"
            fd = open(self.__dev_path, open_text_mode)
            
        except OSError:
            del block_cache
            return False
            
        self.__fd = fd
        self.__block_cache = block_cache
        return True
        
    def __do_close(self) -> bool:
        """Performs the actual device closing process.
        
        Closes the file descriptor and deletes the Block Cache.
        
        Returns:
            bool: True if successful.
        """
        fd = self.__fd
        self.__fd = None
        fd.close()
        
        del self.__block_cache
        self.__block_cache = None
        return True
        