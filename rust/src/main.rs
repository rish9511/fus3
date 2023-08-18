use std::error::Error;
use std::fs::File; 
use std::fs::Metadata;
use std::io::Read;
// use std::io::Read;
use std::io::SeekFrom;
use std::io::Seek;
use std::convert::TryFrom;

use std::io;


fn main() -> Result<(), Box<dyn std::error::Error>> { // Don't fully understand why Box is required
    println!("rusting away!!");
    // let args = Vec::from_iter(std::env::args()); // does not work

    // let args: Vec<_> = std::env::args().collect();
    // let args = Vec::from_iter(std::env::args());
    // println!("{:?}", args)
    
    // println!("{:?}", &std::env::args()[1..4]);

    // println!("{}", std::any::type_name::<_>(std::env::args()));

    let _args: Vec<String> = std::env::args().collect();

    // let file: Result<File, std::io::Error> = File::open("Cargo.toml");

    let _result: Result<String, std::io::Error> = std::fs::read_to_string("../Cargo.toml");

    // let contents: String = result.unwrap();

    let result = File::open("../Cargo.lock");

    match result {

        Ok(mut file) => {
            let metadata= file.metadata()?;
            println!("{} bytes", metadata.len());
            let file_size = metadata.len(); 
            
            // Lets try to read one byte at a time
            // starting with 0, we want to go till file_size - 1

            let PART_SIZE = 50;
            let total_parts = file_size/PART_SIZE;

            println!("total parts {}, file size {}", total_parts, file_size);
            // Read 50 bytes
            // move current cursor to number of bytes read + 1
            // Read next 50 bytes. Repeat this till the last part
            // when reading the last part, read all the left bytes. i.e read till end
            let mut bytes_left = file_size;
            for n in 1..total_parts+1 {
                let mut bytes_to_read: usize = usize::try_from(PART_SIZE)?;
                if n == total_parts {
                    bytes_to_read = usize::try_from(bytes_left)?;
                }
                // let mut buffer: [u8; bytes_to_read] = [0; bytes_to_read];
                let mut buffer: Vec<u8> = vec![0; bytes_to_read];

                let bytes_read: usize = file.read(&mut buffer)?;
                // let new_pos = file.seek(SeekFrom::Current(i64::try_from(bytes_read)? + 1))?;
                println!("bytes read : {}", bytes_read);
                let chunk = String::from_utf8(buffer)?;
                bytes_left = file_size - u64::try_from(bytes_read)?;
                println!("{}", chunk);
            }

            // println!("{:?}", chunk);
            // for n in 0..file_size-1 {
            // }
        }
        Err(err) => {
            // println!("Error: {}", err);
            return Err(Box::new(err));
        }
        
    };

    // file.seek(SeekFrom::Start(15));

    // let mut buf = String::new();
    // file.read_to_string(&mut buf);


    // println!("{:?}", buf);

    return Ok(())
    //     println!("{:?}", result.err());
    // }
    // else {
    //     let contents: String = result.unwrap();
    //     println!("{}", contents)
    // }

    // for arg in args {
    //     println!("{}", arg);
    // }
}
