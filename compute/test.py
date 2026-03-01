import ndt_compute as nc
from pathlib import Path

arrow_path = "/mnt/nvme/data-00000-of-00080.arrow"
out_bin = "/mnt/nvme/wiki_corpus.bin"

res = nc.tokenize_to_nvme(
    dev_path="/dev/ng0n1",
    input_path=str(arrow_path),
    output_path=out_bin,
    slots=4,
)
print(res)
