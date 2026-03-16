import ndt_compute

stats = ndt_compute.arrow_text_dump(
    input_path="data/dataset.arrow",
    output_path="data/extracted_text.txt",
    column="text",
    delimiter="\n",
    max_rows=10000
)
print(f"Extracted {stats['rows']} rows.")
