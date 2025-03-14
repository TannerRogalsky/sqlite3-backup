use clap::Parser;
use futures::TryFutureExt;

/// Backup an SQLite database to a file, compress it, and upload it to S3 or an S3-compatible storage.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    ///The path to the SQLite database to back up.
    #[arg(short, long)]
    source_path: std::path::PathBuf,

    /// The access key for the S3-compatible storage.
    #[arg(long)]
    access_key: String,

    /// The secret key for the S3-compatible storage.
    #[arg(long)]
    secret_key: String,

    /// The name of the S3-compatible storage.
    #[arg(long)]
    space_name: String,

    /// Print progress about the backup (default = false).
    #[arg(long, default_value_t = false)]
    progress: bool,
}

fn main() {
    let Args {
        source_path,
        access_key,
        secret_key,
        space_name,
        progress,
    } = Args::parse();
    let time = jiff::Zoned::now()
        .with_time_zone(jiff::tz::TimeZone::UTC)
        .strftime("%Y-%m-%d_%H:%M:%S");
    let dest_file = tempfile::NamedTempFile::new().unwrap();
    let dest_compressed_file = tempfile::tempfile().unwrap();

    let s3_client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::new()
            .region(aws_sdk_s3::config::Region::new("ny3".to_string()))
            .endpoint_url("https://nyc3.digitaloceanspaces.com") // Set the endpoint for DigitalOcean Spaces
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                access_key, secret_key, None, None, "do",
            ))
            .build(),
    );

    unsafe {
        use sqlite3_sys::*;

        let source_filename =
            std::ffi::CString::new(source_path.to_string_lossy().to_string()).unwrap();

        let mut source_db: *mut sqlite3 = std::ptr::null_mut();
        let rc = sqlite3_open(source_filename.as_ptr(), &mut source_db);
        assert_eq!(rc, SQLITE_OK);

        let mut previous = std::time::Instant::now();
        let callback = |remaining_pages: i32, total_pages: i32| {
            if progress {
                let now = std::time::Instant::now();
                println!(
                    "Backup progress: {} of {} pages remaining in {}ms",
                    remaining_pages,
                    total_pages,
                    now.duration_since(previous).as_millis()
                );
                previous = now;
            }
            std::thread::sleep(std::time::Duration::from_millis(25));
        };

        let dest_filename = dest_file.path().to_str().unwrap();
        let rc = backup_db(source_db, dest_filename, callback);
        assert_eq!(rc, SQLITE_OK);

        let rc = sqlite3_close(source_db);
        assert_eq!(rc, SQLITE_OK);
    }

    let dest_compressed_file = {
        let mut reader = std::io::BufReader::new(dest_file);
        let mut encoder = liblzma::write::XzEncoder::new_parallel(
            std::io::BufWriter::new(dest_compressed_file),
            9,
        );

        std::io::copy(&mut reader, &mut encoder).unwrap();

        let mut writer = encoder.finish().unwrap();
        std::io::Write::flush(&mut writer).unwrap();
        writer.into_inner().unwrap()
    };

    let upload_fut = aws_sdk_s3::primitives::ByteStream::read_from()
        .file(tokio::fs::File::from_std(dest_compressed_file))
        .build()
        .err_into::<anyhow::Error>()
        .and_then(|object| {
            s3_client
                .put_object()
                .key(format!("{}.db.xz", time))
                .bucket(space_name)
                .body(object)
                .send()
                .err_into()
        });

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    match rt.block_on(upload_fut) {
        Ok(_) => {
            println!("Upload successful @ {}", time);
        }
        Err(err) => {
            println!("Upload failed: {}", err);
        }
    }
}

/// Perform an online backup of a database to a file.
///
/// * `source_db` - The database connection to back up.
/// * `dest_filename` - The file to back up to.
/// * `progress_callback` - A function that receives (remaining_pages, total_pages).
///
/// Returns an SQLite result code.
fn backup_db<F>(
    source_db: *mut sqlite3_sys::sqlite3,
    dest_filename: &str,
    mut progress_callback: F,
) -> i32
where
    F: FnMut(i32, i32),
{
    use sqlite3_sys::*;
    use std::ffi::CString;
    use std::ptr;

    let destination_name = CString::new("main").unwrap();
    let source_name = CString::new("main").unwrap();

    unsafe {
        let dest_filename_c = match CString::new(dest_filename) {
            Ok(cstr) => cstr,
            Err(_) => return SQLITE_ERROR,
        };

        let mut dest_db: *mut sqlite3 = ptr::null_mut();
        let mut rc = sqlite3_open(dest_filename_c.as_ptr(), &mut dest_db);

        if rc == SQLITE_OK {
            let backup = sqlite3_backup_init(
                dest_db,
                destination_name.as_ptr(),
                source_db,
                source_name.as_ptr(),
            );
            sqlite3_backup_step(backup, 0);
            let page_count = sqlite3_backup_pagecount(backup);
            let step = page_count / 100;
            if !backup.is_null() {
                loop {
                    rc = sqlite3_backup_step(backup, step);
                    progress_callback(sqlite3_backup_remaining(backup), page_count);

                    if rc != SQLITE_OK && rc != SQLITE_BUSY && rc != SQLITE_LOCKED {
                        break;
                    }
                }
                sqlite3_backup_finish(backup);
            }
            rc = sqlite3_errcode(dest_db);
        }
        sqlite3_close(dest_db);
        rc
    }
}
