from dataclasses import dataclass
import pandas as pd
import numpy as np
import umap
from hdbscan import HDBSCAN, membership_vector


# clustering params
@dataclass
class ClusParams:
    n_components: int = 3
    min_clus_size: int = 6
    min_samples: int = 3
    cluster_selection_epsilon: float = 1
    distance: str = 'euclidean'


# clustering params
cl_params = ClusParams(
    n_components=3,
    min_clus_size=40,
    min_samples=5,
    cluster_selection_epsilon=0,
    distance="cosine"
)


def _results_to_dataframe(df, hdb_model, umap_layout):
    soft_clusters = membership_vector(hdb_model, umap_layout)
    labels = [np.argmax(x) for x in soft_clusters]
    strengths = [np.max(x) for x in soft_clusters]

    # put umap results into a dataframe
    udf = pd.DataFrame({'x': umap_layout[:, 0],
                        'y': umap_layout[:, 1],
                        'z': umap_layout[:, 2],
                        'label': labels,
                        'label_prob': strengths
                        })
    final_df = df.join(udf)
    return final_df


def clusters_from_similarities(df, sims, cl_params):
    # embed to ndim dimensions
    model = umap.UMAP(metric='precomputed')
    umap_layout = model.fit_transform(1 - sims)
    # train hdbscan model to cluster the umap embedding
    hdb_model = HDBSCAN(min_cluster_size=cl_params.min_clus_size, min_samples=cl_params.min_samples,
                        cluster_selection_epsilon=cl_params.cluster_selection_epsilon,
                        cluster_selection_method='leaf',
                        prediction_data=True)
    hdb_model.fit(umap_layout)
    return _results_to_dataframe(df, hdb_model, umap_layout)


def clusters_from_embeddings(df, emb_vects, cl_params):
    ndata = len(df)
    n_neighbors = np.clip(ndata // 200, min(ndata // 4, 8), 50)
    umap_model = umap.UMAP(n_components=cl_params.n_components, min_dist=0,
                           n_neighbors=n_neighbors, metric=cl_params.distance)
    umap_model.fit(emb_vects)
    umap_layout = umap_model.embedding_
    # train hdbscan model to cluster the umap embedding
    hdb_model = HDBSCAN(min_cluster_size=cl_params.min_clus_size, min_samples=cl_params.min_samples,
                        cluster_selection_epsilon=cl_params.cluster_selection_epsilon,
                        cluster_selection_method='leaf',
                        prediction_data=True)
    hdb_model.fit(umap_layout)
    return _results_to_dataframe(df, hdb_model, umap_layout)
