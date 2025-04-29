"""
The `Model` class is an interface between the ML model that you're packaging and the model
server that you're running it on.

The main methods to implement here are:
* `load`: runs exactly once when the model server is spun up or patched and loads the
   model onto the model server. Include any logic for initializing your model, such
   as downloading model weights and loading the model into memory.
* `predict`: runs every time the model server is called. Include any logic for model
  inference and return the model output.

See https://truss.baseten.co/quickstart for more.
"""
import boto3
from pathlib import Path

import pandas as pd
from datasets import Dataset
import torch
from transformers import (
    AutoTokenizer,
    AutoModelForSequenceClassification,
)


class Model:
    def __init__(self, **kwargs):
        self._config = kwargs.get("config")
        secrets = kwargs.get("secrets")
        self.s3_config = (
            {
                "aws_access_key_id": secrets["aws_access_key_id"],
                "aws_secret_access_key": secrets["aws_secret_access_key"],
                "aws_region": secrets["aws_region"],
            }
        )
        self.s3_bucket = (secrets["aws_bucket"])

        self._data_dir = "./data/"
        self._model = None
        self._tokenizer = None
        self.model_prefix = "tw_adaptation_2024"

    def load(self):
        s3 = boto3.resource(
            's3',
            aws_access_key_id=self.s3_config["aws_access_key_id"],
            aws_secret_access_key=self.s3_config["aws_secret_access_key"]
        )

        bucket = s3.Bucket(self.s3_bucket)
        # model_path = os.path.join(self._data_dir, self.model_prefix)
        out_model_path = Path(self._data_dir) / "model"
        out_model_path.mkdir(exist_ok=True, parents=True)

        for obj in bucket.objects.filter(Prefix=self.model_prefix):
            obj_key_parts = obj.key.split("/")
            target = out_model_path / obj_key_parts[-1]
            if not target.exists():
                bucket.download_file(obj.key, str(target))

        self._model = AutoModelForSequenceClassification.from_pretrained(
            out_model_path,
            num_labels=2,
        )
        self._tokenizer = AutoTokenizer.from_pretrained(out_model_path)
        return

    def predict(self, model_input):
        # Run model inference here
        df = pd.DataFrame(model_input)
        chunk_dataset = Dataset.from_pandas(df[["id", "text"]])
        model_inputs = self._tokenizer(chunk_dataset["text"], padding=True, return_tensors="pt", truncation=True)
        output = self._model(**model_inputs)
        logits = output.logits
        sigmoid_logits = torch.sigmoid(logits)

        binary_predictions = (sigmoid_logits > 0.5).int()
        binary_predictions = binary_predictions.tolist()


        label_mapping = {
            'adaptation': [1.0, 0.0],
            'mitigation': [0.0, 1.0],
            'neither': [0.0, 0.0],
            'both': [1.0, 1.0]
        }

        reverse_mapping = {tuple(value): key for key, value in label_mapping.items()}
        chunk_predictions = [reverse_mapping[tuple(pred)] for pred in binary_predictions]

        return chunk_predictions
