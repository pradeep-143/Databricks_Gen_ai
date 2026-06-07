from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql import types as T
import io
from utilities import utils as u

# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.
catalog_name = "resume_use_case"
volume_path = f"/Volumes/{catalog_name}/staging/source_files/Company_JD_s/"

primary_key = "path"

@dp.temporary_view(name="company_jd_vw")
def resume_data():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .load(volume_path)
        .withColumn("parsed_content", u.parse_docx_content(col("content"))).withColumn("modificationTime", current_timestamp())
    )
    return df


dp.create_streaming_table(
    name=f"{catalog_name}.bronze.company_jd",
    comment="SCD1 Bronze target table",
)
dp.create_auto_cdc_flow(
    target=f"{catalog_name}.bronze.company_jd",
    source="company_jd_vw",
    keys=[primary_key],
    sequence_by=col("modificationTime"),
    stored_as_scd_type="1",
)
