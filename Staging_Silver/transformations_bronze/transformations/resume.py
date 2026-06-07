from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql import types as T
import io
from utilities import utils as u

# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.
catalog_name = spark.conf.get("catalog_name")
volume_path = f"/Volumes/{catalog_name}/staging/source_files/Resume_Data_PDF/"

primary_key = "path"

@dp.temporary_view(name="resume_data_vw")
def resume_data():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .load(volume_path)
        .withColumn("parsed_content", u.parse_pdf_content(col("content"))).withColumn("modificationTime", current_timestamp())
    )
    return df


dp.create_streaming_table(
    name=f"{catalog_name}.bronze.resume_data",
    comment="SCD1 Bronze target table",
)
dp.create_auto_cdc_flow(
    target=f"{catalog_name}.bronze.resume_data",
    source="resume_data_vw",
    keys=[primary_key],
    sequence_by=col("modificationTime"),
    stored_as_scd_type="1",
)