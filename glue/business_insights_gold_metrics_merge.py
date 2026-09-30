import boto3
import time

athena = boto3.client("athena", region_name="us-east-2")

WORKGROUP = "user_posts_wg"

SAVED_QUERY_IDS = [
    "d2127bb8-2800-423e-96b6-c674e02b4a21",
    "688a5871-9f18-4649-abff-e45b6cd436e6",
    "bb7d25b0-2c3f-4555-9be7-acc20ccc02d6",
    "b9b4dbf9-69b1-40ea-9d5d-02f820880675",
    "0122ee32-d15f-4721-92c6-a1e04513f761",
    "16c11304-922f-4a06-8dc7-a10841193dcf"
]


def run_saved_query(query_id):

    # Get SQL from the Athena saved query
    response = athena.get_named_query(
        NamedQueryId=query_id
    )

    query = response["NamedQuery"]
    query_name = query["Name"]
    sql = query["QueryString"]

    print(f"Starting: {query_name}")

    # Execute the SQL
    execution = athena.start_query_execution(
        QueryString=sql,
        WorkGroup=WORKGROUP
    )

    execution_id = execution["QueryExecutionId"]

    # Wait for Athena query to finish
    while True:

        result = athena.get_query_execution(
            QueryExecutionId=execution_id
        )

        state = result["QueryExecution"]["Status"]["State"]

        if state == "SUCCEEDED":
            print(f"SUCCESS: {query_name}")
            return

        if state in ["FAILED", "CANCELLED"]:
            reason = result["QueryExecution"]["Status"].get(
                "StateChangeReason",
                "Unknown error"
            )

            raise Exception(
                f"{query_name} {state}: {reason}"
            )

        time.sleep(5)


for query_id in SAVED_QUERY_IDS:
    run_saved_query(query_id)

print("All 6 Gold metric MERGE queries completed successfully.")