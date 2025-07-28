pub struct Inode(u64);

/**
 * there are 3 different numbers "packed" into u64:
 *
 *     +---------+---------+---------+---------+---------+---------+----------+----------+
 *     | group#  |       entry_number (24 bits)         |         alias_index (32 bits)  |
 *     |  u8     |              u32 (partial)           |              u32               |
 *     +---------+---------+---------+---------+---------+---------+----------+----------+
 *
 * */

//const ENTRY_ROOT: u32 = 0xFFFFFF; // 24-bit max value
const ENTRY_ROOT: u32 = 0x000000; // 24-bit min value
const ALIAS_FOR_DG: u32 = 0xffff_ffff;

impl Inode {

    pub fn from_ino(value: u64) -> Inode {
        Inode(value)
    }

    pub fn from_alias(reference_index: u32, alias_index: u32) -> Inode {
        Inode(((reference_index as u64) << 32) | (alias_index as u64))
    }

    pub fn from_group_number(group_number: u8) -> Inode {
        let entry_number: u32 = ENTRY_ROOT;
        let reference_index: u32 = ((group_number as u32) << 24) | entry_number;
        let alias_index: u32 = ALIAS_FOR_DG; // special marker for diskgroup

        Inode::from_alias(reference_index, alias_index)
    }

    /*pub fn from_root() -> Inode {
        Inode(1)
    }*/

    pub fn get_reference_index(&self) -> u32 {
        (self.0 >> 32) as u32
    }

    pub fn get_alias_index(&self) -> u32 {
        self.0 as u32
    }

    pub fn _get_group_number(&self) -> u8 {
        (self.0 >> 56) as u8
    }

    pub fn _get_entry_number(&self) -> u32 {
        (self.0 >> 32) as u32 & 0x00FF_FFFF
    }

    pub fn get_ino(&self) -> u64 {
        self.0
    }

    pub fn _is_disk_group(&self) -> bool {
        self.get_alias_index() == ALIAS_FOR_DG && self._get_entry_number() == ENTRY_ROOT
    }


    pub fn _debug_dump(&self) {
        println!("Inode {} -> group_number: {}, reference_index={}/{:X}, alias_index={:X}, entry_number={:X}, is_diskgroup={}", self.0, self._get_group_number(),
                 self.get_reference_index(), self.get_reference_index(),
                 self.get_alias_index(), self._get_entry_number(), self._is_disk_group());
    }

}