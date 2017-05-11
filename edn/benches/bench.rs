#![feature(test)]

extern crate test;
extern crate edn;
use std::fs::File;
use std::io::Read;

#[bench]
fn edn_parser_bench(b: &mut test::Bencher) {
    let mut data_file = File::open("../tests/music-data-partial.dtm").expect("Unable to open the file");
    let mut data_contents = String::new();
    data_file.read_to_string(&mut data_contents).expect("Unable to read the file.");
    b.bytes = data_contents.len() as u64;
    b.iter(|| {
       edn::parse::value(&data_contents[..]).expect("to parse test input");
    });
}

