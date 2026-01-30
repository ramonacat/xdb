pub mod allocation;
pub mod futex;

use std::ffi::CStr;

use libc::{__errno_location, strerror};

fn panic_on_errno() -> ! {
    let errno = errno();

    panic!("platform error: {} ({errno})", unsafe {
        CStr::from_ptr(strerror(errno)).to_string_lossy()
    });
}

fn errno() -> i32 {
    unsafe { *__errno_location() }
}
