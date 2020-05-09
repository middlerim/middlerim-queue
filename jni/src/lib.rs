use std::ptr;
use std::slice;

use jni::JNIEnv;
use jni::objects::{JByteBuffer, JClass, JObject, JString};
use jni::sys::{jbyteArray, jlong, jobject, jsize};

use shmem::{reader, writer};

#[no_mangle]
pub unsafe extern "system" fn Java_io_middlerim_queue_Writer_init(
    env: JNIEnv, class: JClass, j_config_path: JString,
) -> jlong {
    let config_path: String = env.get_string(j_config_path).unwrap().into();
    let cfg: writer::WriterConfig = confy::load_path(config_path).unwrap();
    let writer = writer::MessageWriter::new(&cfg).unwrap();
    Box::into_raw(Box::new(writer)) as jlong
}

#[no_mangle]
pub unsafe extern "system" fn Java_io_middlerim_queue_Writer_add(
    env: JNIEnv, class: JClass, writer_ptr: jlong, j_message: JByteBuffer,
) -> jlong {
    let message = env.get_direct_buffer_address(j_message).unwrap();
    let mut writer = &mut *(writer_ptr as *mut writer::MessageWriter);
    let row_index = writer.add(message.as_ptr(), message.len()).unwrap();
    row_index as jlong
}

#[no_mangle]
pub unsafe extern "system" fn Java_io_middlerim_queue_Reader_init(
    env: JNIEnv, class: JClass, j_config_path: JString,
) -> jlong {
    let config_path: String = env.get_string(j_config_path).unwrap().into();
    let cfg: reader::ReaderConfig = confy::load_path(config_path).unwrap();
    let reader = reader::MessageReader::new(&cfg).unwrap();
    Box::into_raw(Box::new(reader)) as jlong
}


struct NullContext {}

#[no_mangle]
pub unsafe extern "system" fn Java_io_middlerim_queue_Reader_read(
    env: JNIEnv, class: JClass, reader_ptr: jlong, j_row_index: jlong,
) -> jbyteArray {
    let f = &|buff: *const u8, length: usize, ctx: &mut NullContext| {
        env.byte_array_from_slice(slice::from_raw_parts(buff, length)).unwrap()
    };
    let reader = &mut *(reader_ptr as *mut reader::MessageReader);
    let row_index = j_row_index as usize;
    let ctx = &mut NullContext {};
    reader.read(row_index, f, ctx).unwrap()
}
