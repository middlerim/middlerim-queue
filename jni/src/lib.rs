use std::ptr;
// use std::slice; // Unused

use jni::JNIEnv;
use jni::objects::{JByteBuffer, JClass, JMethodID, JString, JValue}; // Removed JObject
use jni::signature::Primitive; // Removed JavaType
use jni::sys::{jint, jlong};

use shmem::{reader, writer, ShmemLibError, core::ShmemConfig as CoreShmemConfig}; // Renamed for clarity
use serde::Deserialize; // For JniTomlConfig

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

    eprintln!("[JNI Writer_init] Config path: {}", config_path);
    let config_content_str = match std::fs::read_to_string(&config_path) {
        Ok(s) => s,
        Err(e) => {
            let err_msg = format!("Failed to read config file from path '{}': {}", config_path, e);
            throw_exception(&mut env, "java/io/IOException", &err_msg);
            return 0;
        }
    };
    eprintln!("[JNI Writer_init] Config content:\n{}", config_content_str);

    eprintln!("[JNI Writer_init] Config content:\n{}", config_content_str);

    eprintln!("[JNI Writer_init] Config content:\n{}", config_content_str);

    // Define a struct that matches the TOML structure written by Java (flink-focused)
    #[derive(Deserialize, Debug)]
    struct JniTomlConfig {
        shmem_flink_path: String, // Expecting this to be present for flink
        #[serde(default)] // Should be true from Java test TOML
        use_flink_backing: bool,
        max_rows: usize,
        max_row_size: usize,
        max_slots: usize,
        max_slot_size: usize,
    }

    let parsed_toml_config: JniTomlConfig = match toml::from_str(&config_content_str) {
        Ok(c) => c,
        Err(e) => {
            let err_msg = format!("Failed to parse JniTomlConfig from path '{}': {}", config_path, e);
            throw_exception(&mut env, "java/io/IOException", &err_msg);
            return 0;
        }
    };
    eprintln!("[JNI Writer_init] Parsed JniTomlConfig: {:?}", parsed_toml_config);

    // Construct the final shmem::core::ShmemConfig
    let final_core_shmem_config: CoreShmemConfig;
    if parsed_toml_config.use_flink_backing {
        // Enhanced logging for flink path
        let flink_path_str = &parsed_toml_config.shmem_flink_path;
        eprintln!("[JNI Writer_init] Received shmem_flink_path: {}", flink_path_str);
        let p = std::path::Path::new(flink_path_str);
        match p.parent() {
            Some(parent_dir) => {
                eprintln!("[JNI Writer_init] Parent directory deduced: {}", parent_dir.display());
                eprintln!("[JNI Writer_init] Checking existence of parent directory from JNI layer...");
                if parent_dir.exists() {
                    eprintln!("[JNI Writer_init] Parent directory EXISTS (from JNI).");
                    match std::fs::metadata(parent_dir) {
                        Ok(meta) => {
                            let perms = meta.permissions();
                            eprintln!("[JNI Writer_init] Parent directory permissions (from JNI): readonly={}", perms.readonly());
                        }
                        Err(e) => eprintln!("[JNI Writer_init] Could not get metadata for parent directory (from JNI): {}", e),
                    }
                } else {
                    eprintln!("[JNI Writer_init] Parent directory DOES NOT EXIST (from JNI) before core call.");
                }
            }
            None => {
                eprintln!("[JNI Writer_init] Could not get parent directory for path: {}", flink_path_str);
            }
        }

        final_core_shmem_config = CoreShmemConfig {
            shmem_file_name: parsed_toml_config.shmem_flink_path, // Use the direct flink path
            use_flink_backing: true,
            data_dir: None, // Not used for flink when path is absolute
            max_rows: parsed_toml_config.max_rows,
            max_row_size: parsed_toml_config.max_row_size,
            max_slots: parsed_toml_config.max_slots,
            max_slot_size: parsed_toml_config.max_slot_size,
        };
    } else {
        // This path should not be taken by the current Java test
        let default_os_id_name = "jni_os_id_default".to_string(); // Fallback ID
        eprintln!("[JNI Writer_init] Warning: use_flink_backing is false. Using OS_ID default: {}", default_os_id_name);
        final_core_shmem_config = CoreShmemConfig {
            shmem_file_name: default_os_id_name,
            use_flink_backing: false,
            data_dir: None, // Use system default for os_id
            max_rows: parsed_toml_config.max_rows,
            max_row_size: parsed_toml_config.max_row_size,
            max_slots: parsed_toml_config.max_slots,
            max_slot_size: parsed_toml_config.max_slot_size,
        };
    }

    let writer_shmem_wrapper = writer::WriterConfig { shmem: final_core_shmem_config };
    eprintln!("[JNI Writer_init] Final ShmemConfig for MessageWriter::new: {:?}", writer_shmem_wrapper.shmem);

    match writer::MessageWriter::new(&writer_shmem_wrapper) {
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
        let _ = Box::from_raw(writer_ptr as *mut writer::MessageWriter); // free
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

    match writer.add(unsafe { std::slice::from_raw_parts(message_buf, j_length as usize) }) {
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
    eprintln!("[JNI Reader_init] Config path: {}", config_path);
    let config_content_str = match std::fs::read_to_string(&config_path) {
        Ok(s) => s,
        Err(e) => {
            let err_msg = format!("Failed to read config file from path '{}': {}", config_path, e);
            throw_exception(&mut env, "java/io/IOException", &err_msg);
            return 0;
        }
    };
    eprintln!("[JNI Reader_init] Config content:\n{}", config_content_str);

    // Define a struct that matches the TOML structure written by Java
    // This should be the same as in Writer_init or a shared definition
    #[derive(Deserialize, Debug)]
    struct JniTomlConfig {
        shmem_flink_path: String,
        #[serde(default)]
        use_flink_backing: bool,
        max_rows: usize,
        max_row_size: usize,
        max_slots: usize,
        max_slot_size: usize,
    }

    let parsed_toml_config: JniTomlConfig = match toml::from_str(&config_content_str) {
        Ok(c) => c,
        Err(e) => {
            let err_msg = format!("Failed to parse JniTomlConfig from path '{}': {}", config_path, e);
            throw_exception(&mut env, "java/io/IOException", &err_msg);
            return 0;
        }
    };
    eprintln!("[JNI Reader_init] Parsed JniTomlConfig: {:?}", parsed_toml_config);

    // Construct the final shmem::core::ShmemConfig
    let final_core_shmem_config: CoreShmemConfig;
    if parsed_toml_config.use_flink_backing {
        // Enhanced logging for flink path (similar to Writer_init)
        let flink_path_str = &parsed_toml_config.shmem_flink_path;
        eprintln!("[JNI Reader_init] Received shmem_flink_path: {}", flink_path_str);
        let p = std::path::Path::new(flink_path_str);
        match p.parent() {
            Some(parent_dir) => {
                eprintln!("[JNI Reader_init] Parent directory deduced: {}", parent_dir.display());
                eprintln!("[JNI Reader_init] Checking existence of parent directory from JNI layer...");
                if parent_dir.exists() {
                    eprintln!("[JNI Reader_init] Parent directory EXISTS (from JNI).");
                     match std::fs::metadata(parent_dir) {
                        Ok(meta) => {
                            let perms = meta.permissions();
                            eprintln!("[JNI Reader_init] Parent directory permissions (from JNI): readonly={}", perms.readonly());
                        }
                        Err(e) => eprintln!("[JNI Reader_init] Could not get metadata for parent directory (from JNI): {}", e),
                    }
                } else {
                    eprintln!("[JNI Reader_init] Parent directory DOES NOT EXIST (from JNI) before core call.");
                }
            }
            None => {
                eprintln!("[JNI Reader_init] Could not get parent directory for path: {}", flink_path_str);
            }
        }

        final_core_shmem_config = CoreShmemConfig {
            shmem_file_name: parsed_toml_config.shmem_flink_path,
            use_flink_backing: true,
            data_dir: None,
            max_rows: parsed_toml_config.max_rows,
            max_row_size: parsed_toml_config.max_row_size,
            max_slots: parsed_toml_config.max_slots,
            max_slot_size: parsed_toml_config.max_slot_size,
        };
    } else {
        let default_os_id_name = "jni_os_id_default".to_string(); // Fallback ID
        eprintln!("[JNI Reader_init] Warning: use_flink_backing is false. Using OS_ID default: {}", default_os_id_name);
        final_core_shmem_config = CoreShmemConfig {
            shmem_file_name: default_os_id_name,
            use_flink_backing: false,
            data_dir: None,
            max_rows: parsed_toml_config.max_rows,
            max_row_size: parsed_toml_config.max_row_size,
            max_slots: parsed_toml_config.max_slots,
            max_slot_size: parsed_toml_config.max_slot_size,
        };
    }

    let reader_shmem_wrapper = reader::ReaderConfig { shmem: final_core_shmem_config };
    eprintln!("[JNI Reader_init] Final ShmemConfig for MessageReader::new: {:?}", reader_shmem_wrapper.shmem);

    match reader::MessageReader::new(&reader_shmem_wrapper) {
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
        let _ = Box::from_raw(reader_ptr as *mut reader::MessageReader); // free
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

    let read_result = reader.read(row_index, |buff: *mut u8, length: usize, _ctx: &mut NullContext| {
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
            ptr::copy(buff, buff_ptr, length);
        }

        // Pass JMethodID directly
        let ret_type = jni::signature::ReturnType::Primitive(Primitive::Void);
        // call_method_unchecked expects &[sys::jvalue]
        let j_value_obj = JValue::Int(length as jint); // This is jni::objects::JValue
        let arg_sys_jvalue = j_value_obj.as_jni();   // This is jni::sys::jvalue
        let args_sys_slice = [arg_sys_jvalue];          // This is [jni::sys::jvalue; 1]

        // j_buff is JByteBuffer (not Copy).
        // get_direct_buffer_address takes &j_buff (borrows).
        // call_method_unchecked takes an Into<JObject>.
        // JByteBuffer derefs to JObject. So *j_buff gives a JObject.
        // &*j_buff gives an &JObject, which is Into<JObject> (by copy). This avoids moving j_buff.
        let position_result = env.call_method_unchecked(&*j_buff, current_method_id, ret_type, &args_sys_slice[..]);
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
