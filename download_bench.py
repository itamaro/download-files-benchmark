#!/usr/bin/env python3
# Copyright 2018 Itamar Ostricher


"""Benchmark and compare different ways to download files in Python"""


import base64
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
import hashlib
import itertools
import os
import shutil
import subprocess
import tempfile
from time import time

import humanize
import requests


FILES = [
    {
        'name': 'LC80440342016259LGN00_BQA.TIF',
        'url': 'https://storage.googleapis.com/gcp-public-data-landsat/LC08/PRE/044/034/LC80440342016259LGN00/LC80440342016259LGN00_BQA.TIF',
        'md5': b'zqigvl5Envmi/GLc8yH51A==',  # base64
        'size': '3.2MB',
    },
    {
        'name': 'LC80440342016259LGN00_B1.TIF',
        'url': 'https://storage.googleapis.com/gcp-public-data-landsat/LC08/PRE/044/034/LC80440342016259LGN00/LC80440342016259LGN00_B1.TIF',
        'md5': b'835L6B5frB0zCB6s22r2Sw==',  # base64
        'size': '71.26MB',
    },
    {
        'name': 'LC80440342016259LGN00_B8.TIF',
        'url': 'https://storage.googleapis.com/gcp-public-data-landsat/LC08/PRE/044/034/LC80440342016259LGN00/LC80440342016259LGN00_B8.TIF',
        'md5': b'y795LrUzBwk2tL6PM01cEA==',  # base64
        'size': '304.12MB',
    },
]


DOWNLOAD_FUNCTIONS = []


def bench(download_func):
    """A decorator for benchmarked download functions.

    Registers the function in the global list of function to benchmark,
    and computes total download time that is returned as the result.
    """

    @wraps(download_func)
    def wrapper(*args, **kwargs):
        start = time()
        download_func(*args, **kwargs)
        end = time()
        return end - start

    DOWNLOAD_FUNCTIONS.append(wrapper)
    return wrapper


def calc_md5(fname):
    """Return the base64 encoding of the MD5 hash of the file fname."""
    with open(fname, 'rb') as fp:
        md5hash = hashlib.md5()
        # read in chunks of 1MB
        for chunk in iter(lambda: fp.read(1024 * 1024), b''):
            md5hash.update(chunk)
        return base64.b64encode(md5hash.digest())


def file_size(fname):
    """Return file size (in bytes) of file fname"""
    with open(fname, 'rb') as fp:
        return fp.seek(0, 2)


def bench_download(file_desc, download_func):
    """Benchmark a download function"""
    fp = tempfile.NamedTemporaryFile(delete=False)
    fp.close()
    try:
        # time a download of file_desc using download_func
        total_time = download_func(file_desc['url'], fp.name)
        # compute MD5 to verify correctness
        md5 = calc_md5(fp.name)
        if md5 != file_desc['md5']:
            print(f'Error in benchmark of {download_func.__name__} using '
                  f'file desc {file_desc["name"]} ({file_desc["size"]}): '
                  f'MD5 mismatch ({md5} != {file_desc["md5"]})')
        else:
            total_bytes = file_size(fp.name)
            speed = total_bytes / total_time
            print(f'{download_func.__name__: <22}'
                  f'{total_time: >6.2f} sec'
                  f'{humanize.naturalsize(total_bytes): >10}'
                  f'{humanize.naturalsize(speed): >10}/s')
    finally:
        os.unlink(fp.name)


@bench
def requests_raw_shutil(url, fname):
    """Download url into fname using requests raw with shutil copyfileobj"""
    with requests.get(url, stream=True) as response:
        with open(fname, 'wb') as fp:
            shutil.copyfileobj(response.raw, fp)


@bench
def requests_chunks(url, fname):
    """Download url into fname using requests chunked content iterator"""
    with requests.get(url, stream=True) as response:
        with open(fname, 'wb') as fp:
            for chunk in response.iter_content(chunk_size=128 * 1024):
                fp.write(chunk)


@bench
def wget_subprocess(url, fname):
    """Download url into fname using wget in a subprocess"""
    subprocess.check_call(['wget', '-q', '-O', fname, url])


@bench
def curl_subprocess(url, fname):
    """Download url into fname using curl in a subprocess"""
    subprocess.check_call(['curl', '-s', '-o', fname, url])


def run_download_bench():
    """Run the download benchmarking over all registered functions and files"""
    print('=== Benchmark download in main thread ===')
    list(map(lambda args: bench_download(*args),
             itertools.product(FILES, DOWNLOAD_FUNCTIONS)))
    print('=== Benchmark download in a single worker thread pool ===')
    with ThreadPoolExecutor(max_workers=1) as executor:
        list(executor.map(lambda args: bench_download(*args),
                          itertools.product(FILES, DOWNLOAD_FUNCTIONS)))


if __name__ == '__main__':
    run_download_bench()
