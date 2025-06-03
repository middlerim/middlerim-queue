use std::ptr;
// use std::slice; // Unused

use jni::JNIEnv;
use jni::objects::{JByteBuffer, JClass, JMethodID, JString, JValue}; // Removed JObject
use jni::signature::Primitive; // Removed JavaType
use jni::sys::{jint, jlong}; // Removed jbyteArray, jobject, jsize

use shmem::{reader, writer, ShmemLibError};
// use std::ptr::null_mut; // Unused

// Helper function to throw a Java exception
// JNIEnv methods like throw_new require &mut JNIEnv implicitly if env is passed by value.
// If env is &JNIEnv, then throw_new would need &mut *env, which is not possible.
// So, JNIEnv itself must be mutable if we are to call &mut self methods on it.
// The JNI functions receive `env: JNIEnv`. We can pass `&mut env` to `throw_exception`.
fn throw_exception(env: &mut JNIEnv, class_name: &str, msg: &str) { // Takes &mut JNIEnv
    if let Ok(class) = env.find_class(class_name) { // find_class takes &JNIEnv (or env itself)
        // throw_new takes &mut JNIEnv implicitly when env is JNIEnv.
        // If env is &mut JNIEnv, then env.throw_new also works.
        let _ = env.throw_new(class, msg);
    } else {
        // This is a critical failure: the exception class itself couldn't be found.
        // We can't throw the intended exception, so we panic.
        // This will likely crash the JVM, but it indicates a severe setup issue.
        panic!("Failed to find exception class to throw: {}", class_name);
    }
}


#[no_mangle]
pub unsafe extern "system" fn Java_io_middlerim_queue_Writer_init(
    mut env: JNIEnv, _class: JClass, j_config_path: JString, // Made env mutable, class unused
) -> jlong {
    let config_path_str = match env.get_string(&j_config_path) {
        Ok(s) => s,
        Err(_) => {
            throw_exception(&mut env, "java/lang/RuntimeException", "Failed to get config path string from JString");
            return 0;
        }
    };
    let config_path: String = config_path_str.into();

    let cfg: writer::WriterConfig = match confy::load_path(&config_path) {
        Ok(c) => c,
        Err(e) => {
            let err_msg = format!("Failed to load config from path '{}': {}", config_path, e);
            throw_exception(&mut env, "java/io/IOException", &err_msg);
            return 0;
        }
    };

    match writer::MessageWriter::new(&cfg) {
        Ok(writer) => Box::into_raw(Box::new(writer)) as jlong,
        Err(e) => {
            let err_msg = format!("Failed to create MessageWriter: {}", e);
            throw_exception(&mut env, "java/lang/RuntimeException", &err_msg);
            return 0;
        }
    }
}

#[no_mangle]
pub unsafe extern "system" fn Java_io_middlerim_queue_Writer_close(
    _env: JNIEnv, _class: JClass, writer_ptr: jlong, // env, class unused
) -> () {
    let writer = &mut *(writer_ptr as *mut writer::MessageWriter);
    writer.close();
    // Correctly drop the MessageWriter
    unsafe {
        Box::from_raw(writer_ptr as *mut writer::MessageWriter); // free
    };
}

#[no_mangle]
pub unsafe extern "system" fn Java_io_middlerim_queue_Writer_add(
    mut env: JNIEnv, _class: JClass, writer_ptr: jlong, j_message: JByteBuffer, j_length: jlong, // class unused
) -> jlong {
    let message_buf = match env.get_direct_buffer_address(&j_message) {
        Ok(buf) => buf,
        Err(_) => {
            throw_exception(&mut env, "java/lang/RuntimeException", "Failed to get direct buffer address from JByteBuffer");
            return -1;
        }
    };

    if writer_ptr == 0 {
        throw_exception(&mut env, "java/lang/NullPointerException", "Writer pointer is null");
        return -1;
    }
    let writer = &mut *(writer_ptr as *mut writer::MessageWriter);

    match writer.add(message_buf, j_length as usize) {
        Ok(row_index) => row_index as jlong,
        Err(e) => {
            let err_msg = format!("Failed to add message to queue: {}", e);
            throw_exception(&mut env, "java/io/IOException", &err_msg);
            return -1;
        }
    }
}

// Changed type to Option<JMethodID> - no lifetime param needed for the type itself
static mut METHOD_SET_POSITION: Option<JMethodID> = None;

#[no_mangle]
pub unsafe extern "system" fn Java_io_middlerim_queue_Reader_init(
    mut env: JNIEnv, _class: JClass, j_config_path: JString, // class unused
) -> jlong {
    let byte_buffer_class = match env.find_class("java/nio/ByteBuffer") {
        Ok(cls) => cls,
        Err(_) => {
            throw_exception(&mut env, "java/lang/RuntimeException", "Failed to find java/nio/ByteBuffer class");
            return 0;
        }
    };
    // JClass is Copy, so using it for get_method_id and then for new_global_ref (if needed) is fine.
    // The E0382 error was likely a red herring or fixed by other changes.
    // For caching JMethodID, best practice is to get it from a JClass that is a GlobalRef if caching globally.
    // However, for system classes, the JClass obtained via find_class is often stable enough.
    let method_id = match env.get_method_id(byte_buffer_class, "position", "(I)Ljava/nio/ByteBuffer;") {
        Ok(mid) => mid,
        Err(_) => {
            throw_exception(&mut env, "java/lang/RuntimeException", "Failed to get method ID for ByteBuffer.position");
            return 0;
        }
    };
    METHOD_SET_POSITION = Some(method_id); // JMethodID is Copy, direct store is fine.

    let config_path_str = match env.get_string(&j_config_path) {
        Ok(s) => s,
        Err(_) => {
            throw_exception(&mut env, "java/lang/RuntimeException", "Failed to get config path string from JString");
            return 0;
        }
    };
    let config_path: String = config_path_str.into();

    let cfg: reader::ReaderConfig = match confy::load_path(&config_path) {
        Ok(c) => c,
        Err(e) => {
            let err_msg = format!("Failed to load config from path '{}': {}", config_path, e);
            throw_exception(&mut env, "java/io/IOException", &err_msg);
            return 0;
        }
    };

    match reader::MessageReader::new(&cfg) {
        Ok(reader) => Box::into_raw(Box::new(reader)) as jlong,
        Err(e) => {
            let err_msg = format!("Failed to create MessageReader: {}", e);
            throw_exception(&mut env, "java/lang/RuntimeException", &err_msg);
            return 0;
        }
    }
}

#[no_mangle]
pub unsafe extern "system" fn Java_io_middlerim_queue_Reader_close(
    _env: JNIEnv, _class: JClass, reader_ptr: jlong, // env, class unused
) -> () {
    let reader = &mut *(reader_ptr as *mut reader::MessageReader);
    reader.close();
    unsafe {
        Box::from_raw(reader_ptr as *mut reader::MessageReader); // free
    };
}


struct NullContext {}

#[no_mangle]
pub unsafe extern "system" fn Java_io_middlerim_queue_Reader_read(
    mut env: JNIEnv, _class: JClass, reader_ptr: jlong, j_row_index: jlong, j_buff: JByteBuffer, // class unused
) -> () {
    if reader_ptr == 0 {
        throw_exception(&mut env, "java/lang/NullPointerException", "Reader pointer is null");
        return;
    }
    let reader = &mut *(reader_ptr as *mut reader::MessageReader);
    let row_index = j_row_index as usize;
    let ctx = &mut NullContext {};

    // It's important that the closure captures `env` and `j_buff` correctly.
    // The closure `f` needs to be `Fn` or `FnMut` if called multiple times, or `FnOnce` if once.
    // `reader.read` expects `&F` where `F: Fn(...)`.
    // The closure here is simple enough that direct use should be okay.

    // Pass a mutable reference to env into the closure if needed for throw_exception
    // However, the closure is Fn, so it can only capture by shared reference (&) or by move.
    // To call throw_exception(&mut env, ...), the closure would need to capture &mut env,
    // making it FnMut. reader.read expects &Fn.
    // This means errors inside the closure that need to throw Java exceptions are problematic.
    // The current pattern of returning Result from closure and then throwing outside is better.

    let read_result = reader.read(row_index, &|buff: *mut u8, length: usize, _ctx: &mut NullContext| {
        let buff_ptr = match env.get_direct_buffer_address(&j_buff) {
            Ok(bp) => bp,
            Err(_) => {
                // Cannot throw from here directly if `reader.read` doesn't support it.
                // This error needs to be propagated out of `reader.read`.
                // For now, if this fails, we can't proceed with this specific read lambda.
                // This indicates a problem that should ideally be caught before calling reader.read
                // or reader.read should return a Result that allows us to throw.
                // A panic here is undesirable. Let's make the closure return a Result.
                return Err(ShmemLibError::Logic("Failed to get direct buffer address in read callback".to_string()));
            }
        };

        // Check if METHOD_SET_POSITION is initialized
        let current_method_id = match METHOD_SET_POSITION {
            Some(mid) => mid,
            None => {
                // This is a critical internal error, means Reader_init wasn't called or failed.
                return Err(ShmemLibError::Logic("METHOD_SET_POSITION not initialized".to_string()));
            }
        };

        unsafe {
            ptr::copy(buff, buff_ptr, length); // Removed .as_mut_ptr()
        }

        // Pass JMethodID directly
        let ret_type = jni::signature::ReturnType::Primitive(Primitive::Void);
        // call_method_unchecked expects &[sys::jvalue]
        let j_value_wrapper = JValue::Int(length as jint);
        let arg_jvalue: jni::sys::jvalue = match j_value_wrapper {
            JValue::Int(i) => jni::sys::jvalue { i },
            _ => {
                return Err(ShmemLibError::Logic("Internal JNI error: Unexpected JValue type, expected Int".to_string()));
            }
        };
        let args = [arg_jvalue];
        // If j_buff in closure is &JByteBuffer (captured by ref), direct pass should use From<&JByteBuffer>
        let position_result = env.call_method_unchecked(j_buff, current_method_id, ret_type, &args);
        if position_result.is_err() || env.exception_check().unwrap_or(false) {
            // Exception occurred in Java (e.g. ByteBuffer.position() failed or another pending exception)
            // We cannot throw a new one here, just return an error from closure.
            return Err(ShmemLibError::Logic("Exception after calling ByteBuffer.position()".to_string()));
        }
        Ok(()) // Callback successful
    }, ctx);

    // Handle result from reader.read (which now includes results from the closure)
    if let Err(e) = read_result {
        let err_msg = format!("Failed during read operation: {}", e);
        // If an exception is already pending from the callback (e.g. from call_method_unchecked),
        // don't throw a new one. Otherwise, throw one for the Rust error.
        if !env.exception_check().unwrap_or(false) { // find_class in throw_exception needs &JNIEnv
                                                     // throw_new needs &mut JNIEnv.
                                                     // This is an issue if env in closure is not &mut.
                                                     // For now, assume this outer throw_exception will work if env is not &mut.
                                                     // The JNIEnv passed to Reader_read should be made mutable for this.
            throw_exception(&mut env, "java/io/IOException", &err_msg);
        }
    }
}
