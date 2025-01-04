use jni::errors::{Error, JniError};

#[derive(Debug, Clone)]
pub struct CustomError {
    pub message: String,
}

impl From<jni::errors::Error> for CustomError {
    fn from(value: Error) -> Self {
        CustomError {
            message: value.to_string(),
        }
    }
}

impl From<CustomError> for JniError {
    fn from(value: CustomError) -> Self {
        eprintln!("Error occurred: {:?}", value.message);
        JniError::Other(-1)
    }
}
