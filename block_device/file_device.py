from ..prototypes.block_dev import BlockDevice
from threading import Lock
from typing import Optional, IO
import os


class FileDevice(BlockDevice):
    def __init__(self, dev_path: str, block_size: int=4096):
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

    Raises:
        AssertionError: If the provided block size is less than 512 bytes or if the cache size is not
                        greater than 4 blocks.

    The constructor asserts the validity of the provided block and cache sizes to ensure they meet
    minimum requirements. It initializes internal attributes including the device path, block size,
    and a lock for thread-safe operations. The block cache and file descriptor are initialized
    as None and will be set when the device is opened.
    """
        assert block_size >= 512, "The minimun size for a file block device is 512 Bytes."
        
        super().__init__()
        
        self.__block_size = block_size  # Size of each block in bytes
        self.__blk_cnt = None   # Total number of blocks in the device
        self.__dev_path = dev_path
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
        
        Returns:
            bool: True if successfully closed.
        """
        if not self.__is_open():
            return True
        
        flush_ret = self.flush()
        close_ret = self.__do_close()
        
        return flush_ret and close_ret
    
    def block_read(self, blk_id: int, blk_cnt: int) -> Optional['bytearray']:
        """Reads blocks from the device file.
        
        Args:
            blk_id (int): The block ID of the first block to read.
            blk_cnt (int): The number of blocks to read.
        
        Returns:
            Optional[bytearray]: The content of the blocks read if successful;
            otherwise, None.
        """
        if not self.__is_open():
            return None
        
        offset_bytes = self.__calc_block_bytes(blk_id)
        read_size = self.__calc_block_bytes(blk_cnt)
        if offset_bytes < 0 or read_size < 0:
            return None
        
        if blk_cnt + blk_cnt > self.__blk_cnt:
            # Out of range
            return None
        
        # seek to the place to write
        self.__fd.seek(offset_bytes)
        
        return self.__fd.read(read_size)
    
    def block_write(self, blk_id: int, blk_cnt: int, buf: bytes) -> int:
        """Writes blocks to the device file.
        
        Args:
            blk_id (int): The block ID of the first block to write.
            blk_cnt (int): The number of blocks to write.
            buf (bytes): The buffer storing data to write.
        
        Returns:
            int: The actual number of blocks written. -1 if failed.
        """
        if not self.__is_open():
            return -1
        
        offset_bytes = self.__calc_block_bytes(blk_id)
        write_size = self.__calc_block_bytes(blk_cnt)
        if offset_bytes < 0 or write_size < 0:
            return -1
        
        if blk_cnt + blk_cnt > self.__blk_cnt:
            # Out of range
            return -1
        
        # seek to the place to write
        self.__fd.seek(offset_bytes)
        
        # do write operation
        if write_size <= len(buf):
            wt_size = self.__fd.write(buf[:write_size])
            return wt_size // self.__block_size
        else:
            z_padding_size = write_size - len(buf)
            wt_size = self.__fd.write(buf)
            pd_size = self.__fd.write(bytearray([0])*z_padding_size)
            return (wt_size + pd_size) // self.__block_size
    
    def flush(self) -> bool:
        """Flushes the device's Block Cache.
        
        Writes all dirty blocks in the Block Cache back to the device.
        
        Returns:
            bool: True if successful.
        """
        if self.__is_open():
            self.__fd.flush()
            return True
        else:
            return False
    
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
        
        Attempts to open the device file.
        
        Returns:
            bool: True if successful.
        """
        
        # Open or create the device's file.
        try:
            fd = open(self.__dev_path, "rb+")
            
        except OSError:
            return False
        
        blk_cnt = self.__get_blk_cnt(fd)
        if blk_cnt is None:
            fd.close()
            return False    
        
        self.__fd = fd
        self.__blk_cnt = blk_cnt
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
        
        return True
        
    def __calc_block_bytes(self, blk_id: int) -> int:
        """Calculate the offset in bytes for the file device

        Args:
            blk_id (int): The Block ID

        Returns:
            int: offset in bytes if block id is within the range of the device,
                -1 if failed.
        """
        if not isinstance(self.__blk_cnt, int):
            # Cannot get the number of blocks in the device, because the device
            # file is not properly opened.
            return -1
        
        if blk_id < 0 or self.__blk_cnt <= blk_id:
            # The given block id is out of range.
            return -1
        
        return self.__block_size * blk_id
    
    def __get_blk_cnt(self, fd: IO['any']) -> Optional['int']:
        """Get the total number of blocks in the given device file
        
        Args:
            fd: The opened device file
            
        Returns:
            Optional['int']: The total number of blocks if success.
                None if failed.
        """
        if not fd.seekable():
            return None
        
        fd.seek(0, os.SEEK_END)
        file_size = fd.tell()
        if file_size < self.__block_size:
            return None
        
        return file_size // self.__block_size
