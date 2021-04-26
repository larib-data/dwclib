import numpy as np
from numba import njit


@njit
def wave_unfold(
    indata: bytes, doscale: bool, cau: float, cal: float, csu: float, csl: float
):
    npindata = np.frombuffer(indata, dtype=np.int16)
    if doscale:
        m = (cau - cal) / (csu - csl)
        b = cau - m * csu
    else:
        m = 1
        b = 0
    outdata = m * npindata.astype(np.float32) + b
    return outdata


if __name__ == '__main__':
    indata = b'\xff\x03\xff\x03\xff\x03\xff\x03\xff\x03\xff\x03\xff\x03'
    cau = 1.0
    cal = 0.0
    csu = 2282
    csl = 1815
    expect_out = np.array(
        [
            -1.6959312,
            -1.6959312,
            -1.6959312,
            -1.6959312,
            -1.6959312,
            -1.6959312,
            -1.6959312,
        ],
        'float32',
    )

    outlen = int(len(indata) / 2)
    # outdata = np.empty(outlen, dtype='float32')
    outdata = wave_unfold(indata, cau, cal, csu, csl)
    print(f'Input data: {indata}')
    print(f'Output data: {outdata}')
    print(f'Expected output: {expect_out}')
    assert np.array_equal(outdata, expect_out)
