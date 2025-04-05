import ctypes

lzo = ctypes.cdll.LoadLibrary("./minilzo.so")


def decompress_block(block, dst_len=0):
    in_buf = ctypes.create_string_buffer(block)
    cb = ctypes.c_int(len(in_buf) - 1)
    cbOut = dst_len if dst_len else len(in_buf) * 10
    out_buf = ctypes.create_string_buffer(cbOut)
    cbOut = ctypes.c_int(cbOut)
    lzo.lzo1x_decompress(
        ctypes.byref(in_buf), cb, ctypes.byref(out_buf), ctypes.byref(cbOut)
    )
    return out_buf.raw[: cbOut.value]
