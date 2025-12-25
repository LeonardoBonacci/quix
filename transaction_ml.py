import os
import random
from quixstreams import Application
import onnxruntime as ort
import numpy as np


# Load ONNX model
session = ort.InferenceSession("validator_model.onnx")
input_name = session.get_inputs()[0].name

def is_valid_transaction(tx: dict) -> bool:
    """
    tx: {'amount': float, 'country': 0/1, 'merchant': 0/1}
    returns: True if valid, False otherwise
    """
    # Prepare input array (1 sample, 3 features)
    x = np.array([[tx['amount'], tx['country'], tx['merchant']]], dtype=np.float32)
    # Run ONNX inference
    prob = session.run(None, {input_name: x})[0][0][0]
    return prob > 0.5  # threshold 0.5

def main():
    app = Application(
        broker_address=os.getenv("BROKER_ADDRESS", "localhost:9092"),
        consumer_group="transaction-validator-v0",
        auto_offset_reset="earliest",
    )

    # Topics
    input_topic = app.topic(name="transaction_requests2")
    valid_topic = app.topic(name="transactions")
    invalid_topic = app.topic(name="invalid_transactions")

    # ---- READ FROM KAFKA (not a Source) ----
    sdf = app.dataframe(topic=input_topic)

    # Enrich with validation result
    sdf["valid"] = sdf.apply(is_valid_transaction)

    # ---- BRANCH STREAM ----
    valid_sdf = sdf[sdf["valid"] == True]
    invalid_sdf = sdf[sdf["valid"] == False]

    # Drop the column before sending
    valid_sdf.drop(columns=["valid"])
    invalid_sdf.drop(columns=["valid"])

    # Optional debugging
    valid_sdf.print()
    invalid_sdf.print()

    # ---- WRITE TO TOPICS ----
    valid_sdf.to_topic(valid_topic)
    invalid_sdf.to_topic(invalid_topic)

    app.run()


if __name__ == "__main__":
    main()
