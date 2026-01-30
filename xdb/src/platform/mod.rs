pub mod futex;

use std::ffi::CStr;

use libc::{__errno_location, strerror};

// TODO create abstractions here for all the raw calls to libc

// TODO this should be private once there's no platform code outside this module
pub fn panic_on_errno() -> ! {
    let errno = errno();

    panic!("platform error: {} ({errno})", unsafe {
        CStr::from_ptr(strerror(errno)).to_string_lossy()
    });
}

// TODO this should be private once there's no platform code outside this module
pub fn errno() -> i32 {
    unsafe { *__errno_location() }
}
