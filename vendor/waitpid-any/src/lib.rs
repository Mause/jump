use libc::{pid_t, waitpid};

pub struct WaitHandle {
    pid: pid_t,
}

#[derive(Debug)]
pub struct WaitError {}
impl std::fmt::Display for WaitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WaitError occurred")
    }
}
impl std::error::Error for WaitError {}
impl WaitError {
    pub fn raw_os_error(&self) -> Option<i32> {
        None
    }
}

impl WaitHandle {
    pub fn open(pid: i32) -> Result<WaitHandle, WaitError> {
        Ok(WaitHandle { pid })
    }
    pub fn wait(&self) -> Result<(), WaitError> {
        let status = std::ptr::from_mut(&mut 0);
        unsafe {
            waitpid(self.pid, status, 0);
        }
        if status.is_null() {
            return Err(WaitError {});
        }
        Ok(())
    }
}
