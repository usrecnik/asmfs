/*
block structure for trailer archivelog block - as inferred by comparing various archivelog files and running:
ALTER SYSTEM DUMP LOGFILE 'path to archivelog';

Anyway, these findings certainly might not be accurate. This is a work in progress.

Offset 0x00-0x01: 01 22          - Block type identifier (always 0x2201)
Offset 0x0A-0x0B: 10 80          - Constant flags
Offset 0x0E-0x0F: 8c 00 00 00    - Constant value
Offset 0x18-0x19: 01 00 00       - Constant
Offset 0x22-0x27: 01 00 01 00 -
                  01 00 00 00 - Constant pattern
Offset 0x28-0x2B: 01 00 00 00 0a - Constant
Offset 0x52-0x57: 18 04 00 00 00 00 00 00 - Constant (likely Oracle version?)
Offset 0x68-0x6F: 00 06 00 00 01 00 00 00 - Constant
Offset 0x70-0x7F: 00 00 00 00 06 00 10 00 10 00 00 00 28 23 00 00 - Constant
Offset 0x88-0x8F: 72 00 0c 00    - Constant
Offset 0x94-0x97: 00 80 00 00    - Constant (appears twice)
Offset 0xA0+:     All zeros       - Padding to end of block

Offset 0x04-0x05 (little-endian word):
    Seq 128: 0x0e64 = 3684 blocks
    Seq 129: 0x0e4b = 3659 blocks
    Seq 130: 0x0ed4 = 3796 blocks
    This is the BLOCK NUMBER where this trailer resides!

Offset 0x08 (byte):
    Seq 128: 0x80 = 128
    Seq 129: 0x81 = 129
    Seq 130: 0x82 = 130
    This is the SEQUENCE NUMBER! (Thread 1, so just the low byte)

Offset 0x0C-0x0D (word):
    Appears to be a checksum or hash

Offset 0x10-0x13 (RBA - Redo Block Address):
    These are RBA pointers that change per file
    Offset 0x38, 0x40, 0x48, 0x90, 0x98 all contain similar RBA values

    Offset 0x1C-0x1F:

    Variable data, possibly flags or SCN-related
Offset 0x50-0x56 (timestamp):
    Changes per file - likely the archive log completion timestamp
*/

pub fn generate_archivelog_trail() -> Result<Vec<u8>, String> {
    let mut block :Vec<u8> = vec![0x00; 512];

    write_u16_le(&mut block, 0x00, 0x2201);         // block type identifier
    write_u16_le(&mut block, 0x0A, 0x8010);         // constant flags (?)
    write_u32_le(&mut block, 0x0E, 0x0000008c);     // constant value (?)
    write_u32_le(&mut block, 0x18, 0x00000001);     // constant (?)
    write_u32_le(&mut block, 0x22, 0x00010001);     // constant pattern (?)
    write_u32_le(&mut block, 0x25, 0x00000001);     // constant pattern (cont) (?)
    write_u32_le(&mut block, 0x28, 0x0a000001);     // constant (?)
    //
    // 00 00 00 00 - Constant (likely Oracle version?)
    write_u32_le(&mut block, 0x52, 0x00000418);     // maybe Oracle version?
    Ok(block)
}

fn write_u8(buf: &mut [u8], offset: usize, value: u8) {
    buf[offset] = value;
}

fn write_u16_le(buf: &mut [u8], offset: usize, value: u16) {
    buf[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn write_u32_le(buf: &mut [u8], offset: usize, value: u32) {
    buf[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}