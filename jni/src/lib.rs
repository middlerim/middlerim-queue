use std::borrow::{Borrow, BorrowMut};
use std::ptr;
use std::slice;

use jni::JNIEnv;
use jni::objects::{JByteBuffer, JClass, JMethodID, JObject, JString, JValue};
use jni::signature::{JavaType, Primitive};
use jni::sys::{jbyteArray, jint, jlong, jobject, jsize};

use shmem::{reader, writer};
use std::ptr::null_mut;

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
pub unsafe extern "system" fn Java_io_middlerim_queue_Writer_close(
    env: JNIEnv, class: JClass, writer_ptr: jlong,
) -> () {
    let writer = &mut *(writer_ptr as *mut writer::MessageWriter);
    writer.close();
    unsafe {
        Box::from_raw(writer_ptr as *mut reader::MessageReader); // free
    };
}

#[no_mangle]
pub unsafe extern "system" fn Java_io_middlerim_queue_Writer_add(
    env: JNIEnv, class: JClass, writer_ptr: jlong, j_message: JByteBuffer, j_length: jlong,
) -> jlong {
    let message = env.get_direct_buffer_address(j_message).unwrap();
    let mut writer = &mut *(writer_ptr as *mut writer::MessageWriter);
    let row_index = writer.add(message.as_ptr(), j_length as usize).unwrap();
    row_index as jlong
}

static mut method_set_position: *mut JMethodID = std::ptr::null_mut();

#[no_mangle]
pub unsafe extern "system" fn Java_io_middlerim_queue_Reader_init(
    env: JNIEnv, class: JClass, j_config_path: JString,
) -> jlong {
    method_set_position = unsafe {
        std::mem::transmute::<*mut JMethodID, *mut JMethodID<'static>>(Box::into_raw(Box::new(env.get_method_id(
                env.find_class("java/nio/ByteBuffer").unwrap(),
                "position",
                "(I)Ljava/nio/ByteBuffer;").unwrap())))
    };

    let config_path: String = env.get_string(j_config_path).unwrap().into();
    let cfg: reader::ReaderConfig = confy::load_path(config_path).unwrap();
    let reader = reader::MessageReader::new(&cfg).unwrap();
    Box::into_raw(Box::new(reader)) as jlong
}

#[no_mangle]
pub unsafe extern "system" fn Java_io_middlerim_queue_Reader_close(
    env: JNIEnv, class: JClass, reader_ptr: jlong,
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
    env: JNIEnv, class: JClass, reader_ptr: jlong, j_row_index: jlong, j_buff: JByteBuffer,
) -> () {
    let reader = &mut *(reader_ptr as *mut reader::MessageReader);
    let row_index = j_row_index as usize;
    let ctx = &mut NullContext {};
    let f = &|buff: *mut u8, length: usize, ctx: &mut NullContext| {
        let buff_p = env.get_direct_buffer_address(j_buff).unwrap();
        unsafe {
            ptr::copy(buff, buff_p.as_mut_ptr(), length);
        }
        env.call_method_unchecked(j_buff, *method_set_position, JavaType::Primitive(Primitive::Void), &([JValue::Int(length as jint)]));
    };
    reader.read(row_index, f, ctx);
}
