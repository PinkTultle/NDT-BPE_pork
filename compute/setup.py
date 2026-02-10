#!/usr/bin/env python3
from pathlib import Path
from setuptools import setup

try:
    from pybind11.setup_helpers import Pybind11Extension, build_ext
except ImportError as exc:
    raise SystemExit(
        "pybind11 is required to build this module. "
        "Install it with: pip install pybind11"
    ) from exc

ROOT = Path(__file__).resolve().parent

liburing_a = ROOT / "third_party" / "liburing" / "src" / "liburing.a"
if not liburing_a.exists():
    raise SystemExit(
        f"Missing {liburing_a}. Run ./build_compute.sh in {ROOT} first."
    )

ext_modules = [
    Pybind11Extension(
        "ndt_compute",
        sources=[
            str(ROOT / "src" / "bindings.cpp"),
            str(ROOT / "src" / "io-uring.cpp"),
            str(ROOT / "src" / "fiemap_schedule.cpp"),
        ],
        include_dirs=[
            str(ROOT / "include"),
            str(ROOT / "third_party" / "liburing" / "src" / "include"),
        ],
        extra_objects=[str(liburing_a)],
        extra_compile_args=["-std=c++17"],
        extra_link_args=["-lpthread", "-ldl", "-lrt"],
    )
]

setup(
    name="ndt_compute",
    version="0.1.0",
    description="NDT-BPE compute bindings (FIEMAP + NVMe io_uring).",
    ext_modules=ext_modules,
    cmdclass={"build_ext": build_ext},
    zip_safe=False,
)
