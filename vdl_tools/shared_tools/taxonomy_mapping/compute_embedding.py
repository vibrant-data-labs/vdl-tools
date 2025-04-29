"""
# run from taxonomy_mapping folder

https://huggingface.co/intfloat/e5-mistral-7b-instruct
"""
import os
import openai
import numpy as np
# from sentence_transformers import SentenceTransformer

from dotenv import load_dotenv
# import voyageai


load_dotenv(override=True, dotenv_path=os.getcwd() + '/.env')
openai.api_key = os.getenv("OPENAI_API_KEY")

# voyageai.api_key = 'pa-8k4FE6NcTMNO1oViS0Lz9PBuNAjAE7VSv4YlnCQZJr4'

client = openai.OpenAI()


# def get_voyage_embedding(txt):
#    return np.array(voyageai.get_embeddings([txt],
#                                            model="voyage-2",
#                                            input_type="document")[0])


def get_openai_embedding(txt, v3=False):
    response = client.embeddings.create(
        input=txt,
        model="text-embedding-3-large" if v3 else "text-embedding-ada-002"
    )
    emb = response.data[0].embedding
    return np.array(emb)


# class STEmbedding:
#    def __init__(self, model_name):
#        self.model = SentenceTransformer(model_name)

#    def get_embeddings(self, texts):
#        return self.model.encode(texts)


sgpt_e = None


# def get_sgpt_embedding(txt):
#    mname = "Muennighoff/SGPT-1.3B-weightedmean-nli-bitfit"
#    # mname = "Muennighoff/SGPT-125M-weightedmean-nli-bitfit"
#    global sgpt_e
#    if sgpt_e is None:
#        sgpt_e = STEmbedding(mname)
#    v = sgpt_e.get_embeddings([txt])[0]
#    v = v/np.linalg.norm(v)
#    return v


# roberta_e = None


# def get_roberta_embedding(txt):
#    mname = "all-roberta-large-v1"
#    global roberta_e
#    if roberta_e is None:
#        roberta_e = STEmbedding(mname)
#    v = roberta_e.get_embeddings([txt])[0]
#    v = v/np.linalg.norm(v)
#    return v
