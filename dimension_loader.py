"""
Reusable Configuration-Driven Dimension Loader - SCD Type 1
Loads N dimension tables in parallel from source PSXLATITEM
Uses Auto CDC for efficient insert/update operations (SCD Type 1)
Add new dimensions by adding entries to DIMENSION_CONFIG array
"""
import dlt
from pyspark.sql.functions import col

# Define source as streaming view for Auto CDC
# skipChangeCommits allows the source table to receive both inserts and updates
@dlt.view(
    name="psxlatitem_source",
    comment="Source table for all dimension loads"
)
def psxlatitem_source():
    return (
        spark.readStream
        .option("skipChangeCommits", "true")  # Allow updates in source table
        .table("ADW_DEV.ENR.PSXLATITEM")
    )

# Configuration Array - Add new dimensions here
DIMENSION_CONFIG = [
    {
        "source_filter": "SSR_COMPONENT",
        "target_table": "dim_course_type",
        "code_column": "COURSE_TYPE_CD",
        "desc_column": "COURSE_TYPE_DS"
    },
    {
        "source_filter": "ENRL_STATUS_REASON",
        "target_table": "dim_enrl_status_reason",
        "code_column": "ENRL_STATUS_REASON_CD",
        "desc_column": "ENRL_STATUS_REASON_DS"
    },
    {
        "source_filter": "INSTR_ROLE",
        "target_table": "dim_instr_role",
        "code_column": "INSTR_ROLE_CD",
        "desc_column": "INSTR_ROLE_DS"
    }
]

# Function factory to create SCD Type 1 CDC flows
def create_scd_type1_loader(config):
    # Create the target streaming table
    dlt.create_streaming_table(
        name=config["target_table"],
        comment=f"SCD Type 1 dimension from PSXLATITEM where FIELDNAME={config['source_filter']}"
    )
    
    # Apply CDC transformation (SCD Type 1)
    dlt.apply_changes(
        target=config["target_table"],
        source=f"{config['target_table']}_source",
        keys=[config["code_column"]],  # Primary key for SCD Type 1
        sequence_by="LASTUPDTTM",       # Order changes by this timestamp
        stored_as_scd_type=1            # SCD Type 1: INSERT new, UPDATE existing
    )
    
    # Define the filtered source for this dimension
    @dlt.view(name=f"{config['target_table']}_source")
    def dimension_source():
        return (
            dlt.read_stream("psxlatitem_source")
            .filter(col("FIELDNAME") == config["source_filter"])
            .select(
                col("FIELDVALUE").alias(config["code_column"]),
                col("XLATLONGNAME").alias(config["desc_column"]),
                col("LASTUPDTTM"),
                col("ETL_INSERT_DATE"),
                col("ETL_INSERT_USER"),
                col("ETL_UPDATE_DATE"),
                col("ETL_UPDATE_USER")
            )
        )
    
    return dimension_source

# Loop through configuration and create parallel SCD Type 1 flows
for config in DIMENSION_CONFIG:
    create_scd_type1_loader(config)
