// Each gearman server defines its own error codes. So these are specific to rustygear
pub enum RustygearServerError {
    InvalidPacket = 1,
    InvalidClientId = 2,
}
