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

import os
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
        # Uncomment the following to get access
        # to various parts of the Truss config.
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
        self.model_prefix = "cwf_adaptation"


    def load(self):
        # Load model here and assign to self._model.

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
            num_labels=3,
        )
        self._tokenizer = AutoTokenizer.from_pretrained(out_model_path)
        return

    def predict(self, model_input):
        # Run model inference here
        df = pd.DataFrame(model_input)
        chunk_dataset = Dataset.from_pandas(df[["id", "text"]])
        model_inputs = self._tokenizer(chunk_dataset["text"], padding=True, return_tensors="pt", truncation=True)
        output = self._model(**model_inputs)
        representation = torch.nn.functional.softmax(output.logits, dim=-1)
        labels = [x.argmax().item() for x in representation]
        label2id = {0: 'Adaptation', 1: 'Mitigation', 2: 'Dual'}
        chunk_predictions = [label2id[x] for x in labels]

        return chunk_predictions
