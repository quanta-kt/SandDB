const POLY: u32 = 0x82F63B78;

const fn make_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut j = 0;
        while j < 8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ POLY;
            } else {
                crc >>= 1;
            }
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

static CRC32C_TABLE: [u32; 256] = make_table();

pub fn crc32c(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;

    for &byte in data {
        let index = (crc ^ (byte as u32)) & 0xFF;
        crc = CRC32C_TABLE[index as usize] ^ (crc >> 8);
    }

    crc ^ 0xFFFF_FFFF
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc32c() {
        assert_eq!(crc32c(b"123456789"), 0xe3069283);
    }

    #[test]
    fn test_empty() {
        assert_eq!(crc32c(b""), 0x00000000);
    }

    #[test]
    fn test_hello_world() {
        assert_eq!(crc32c(b"hello world"), 0xc99465aa);
    }
}
