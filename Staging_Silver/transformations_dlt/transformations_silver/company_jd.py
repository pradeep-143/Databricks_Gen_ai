import dlt
from pyspark.sql import functions as F
from pyspark import pipelines as dp

catalog_name = spark.conf.get("catalog_name")
primary_key = "path"
schema = "struct<Title:string, Experience:int, Skills:string>"

@dlt.view(
    name='silver_company_jd'
)
def silver_company_jd():
    df = spark.readStream.table(
        f"{catalog_name}.bronze.company_jd"
    )
    df = df.selectExpr(
    """ai_query(
        'databricks-meta-llama-3-3-70b-instruct',
        CONCAT(
            'You are a data extraction engine. Extract the following fields from the input text:\n',
            '- Title (string)\n',
            '- Experience (int, year should always be maximun)\n',
            '- Skills (strings)\n\n',
            'Rules:\n',
            '1. Return ONLY a valid JSON object, nothing else.\n',
            '2. No markdown, no code blocks, no explanations.\n',
            '3. Experience MUST be a int year  and choose max from range (e.g. "5-8"). If only one number, use 5.\n',
            '4. Skills must be of only strings without brackets.\n',
            '5. If a field is missing, return null for that field.\n\n',
            'Output format example:\n',
            '{"Title": "Software Engineer", "Experience": 3, "Skills": "Python,SQL"}\n\n',
            'Input text:\n---\n',
            parsed_content,
            '\n---\nJSON output:'
        )
    ) as data""",
    "modificationTime","path")
    df = df.select(F.from_json(F.col("data"),schema).alias("data"),F.col("path"),F.col("modificationTime"))
    df=df.select(F.col("data.*"),F.col("path"),F.col("modificationTime"))
    return df

dp.create_streaming_table(
    name=f"{catalog_name}.silver.company_jd",
    comment="SCD1 Silver target table"
)

dp.apply_changes(
    target=f"{catalog_name}.silver.company_jd",
    source="silver_company_jd",
    keys=[primary_key],
    sequence_by="modificationTime",
    stored_as_scd_type="1"
)
