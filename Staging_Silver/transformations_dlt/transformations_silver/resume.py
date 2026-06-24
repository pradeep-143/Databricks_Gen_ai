import dlt
from pyspark.sql import functions as F
from pyspark import pipelines as dp
from pyspark.sql import types as T
from pyspark.sql import DataFrame

catalog_name = spark.conf.get("catalog_name")
primary_key = "path"
schema = """
struct<
  name:string,
  title:string,
  experience:string,
  email:string,
  country:string,
  phone_number:string,
  skills:string,
  tools:string,
  past_companies:string
>
"""


@dp.view(name="silver_resume_data")
@dp.expect_or_drop("valid_skills", "skills IS NOT NULL")
def silver_company_jd():
    df = spark.readStream.table(f"{catalog_name}.bronze.resume_data")
    df = df.selectExpr(
        """ai_query('databricks-meta-llama-3-3-70b-instruct',CONCAT('You are an information extraction engine.Extract the following fields from the resume text below:- name- title - experience (total years as a number or range like "12+" or "12") - country - email - phone_number - skills - tools - past_companies Rules:
1. Return ONLY a valid JSON object.
2. Do NOT include markdown, explanations, or the word "json".
3. If a field is missing, return null.
4. experience must be a number or range of years only.
5. skills and tools must be strings.
6. past_companies must be string of company names only.if there is no company name return null
7. Do not infer data that is not explicitly present.
Resume Text:', parsed_content)) as data""",
        "path",
        "modificationTime",
    )
    df = df.withColumn("data", F.from_json(F.col("data"), schema))
    df = df.selectExpr("data.*", "path", "modificationTime")
    return df


dp.create_streaming_table(
    name=f"{catalog_name}.silver.resume_data", comment="SCD1 Silver target table"
)

dp.apply_changes(
    target=f"{catalog_name}.silver.resume_data",
    source="silver_resume_data",
    keys=[primary_key],
    sequence_by="modificationTime",
    stored_as_scd_type="1",
)
