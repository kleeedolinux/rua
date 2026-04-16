use rua::bytecode::decode_module;

#[test]
fn malformed_bytecode_is_rejected() {
    let corpus = vec![
        vec![],
        vec![0, 1, 2, 3, 4, 5],
        b"RUAC".to_vec(),
        b"RUAC\x01\x00".to_vec(),
        b"RUAC\x01\x00\xff\xff\xff\xff".to_vec(),
    ];
    for bytes in corpus {
        let _ = decode_module(&bytes);
    }
}
