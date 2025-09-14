import json
from typing import Sequence

import numpy as np
import pandas as pd
import torch
from pykeen.evaluation import MetricResults
from pykeen.models import Model
from pykeen.pipeline import pipeline, PipelineResult
from pykeen.predict import predict_triples
from pykeen.triples import TriplesFactory, leakage


def generate_training_set(fact_triples: list[tuple[str, str, str]]) -> tuple[TriplesFactory, TriplesFactory, TriplesFactory]:
    triples_array = np.array(fact_triples)

    tf = TriplesFactory.from_labeled_triples(triples_array)
    training, validation, testing = tf.split([0.8, 0.1, 0.1], random_state=42)

    # Reduce data leakage between training and testing triples
    core_training, core_validation, core_testing = leakage.unleak(training, validation, testing, n=0.4)

    training.mapped_triples = core_training.mapped_triples
    validation.mapped_triples = core_validation.mapped_triples
    testing.mapped_triples = core_testing.mapped_triples

    return training, validation, testing


def train_model(training: TriplesFactory, validation: TriplesFactory, testing: TriplesFactory, model_configuration: dict, seed = 42) -> PipelineResult:
    # Use all available CPU cores
    torch.set_num_threads(torch.get_num_threads())

    model_name = model_configuration['model']
    print(f"Training model '{model_name}'...")

    results = pipeline(
        training=training,
        validation=validation,
        testing=testing,
        random_seed=seed,
        **model_configuration
    )

    print(f"Completed training for model {model_name}")
    return results


def save_training_results(results: PipelineResult, model_dir_path: str) -> None:
    """
    Save the final model as well as training metrics and training triples
    :param results:
    :param model_dir_path:
    """

    results.save_to_directory(model_dir_path)

    results_dataframe = results.metric_results.to_df()
    results_dataframe.to_csv(f'{model_dir_path}/metrics.csv', index=False)


def load_model(model_dir_path: str) -> tuple[Model, TriplesFactory]:
    model = torch.load(
        model_dir_path + "/trained_model.pkl",
        map_location=torch.device('cuda' if torch.cuda.is_available() else 'cpu'),
        weights_only=False
    )

    # Triples factory (for ID→label mapping)
    tf = TriplesFactory.from_path(
        path=f"{model_dir_path}/training_triples/numeric_triples.tsv.gz",
        entity_to_id_path=f"{model_dir_path}/training_triples/entity_to_id.tsv.gz",
        relation_to_id_path=f"{model_dir_path}/training_triples/relation_to_id.tsv.gz",
    )

    return model, tf


def load_training_results(model_dir_path: str) -> pd.DataFrame:
    return pd.read_csv(f"{model_dir_path}/metrics.csv")


def summarize_training_metrics(metrics: MetricResults) -> pd.DataFrame:
    return pd.DataFrame({
        "MRR": [metrics.get_metric("mrr")],
        "Hits@10": [metrics.get_metric("hits_at_10")],
        "Hits@5": [metrics.get_metric("hits_at_5")],
        "Hits@3": [metrics.get_metric("hits_at_3")],
        "Hits@1": [metrics.get_metric("hits_at_1")],
        "Mean Rank": [metrics.get_metric("mr")],
    })


def score_triples(embedding_model: Model, triples_factory: TriplesFactory, triples: Sequence[tuple[str, str, str]]) -> pd.DataFrame:
    score_pack = predict_triples(model=embedding_model, triples=triples)
    scores_df = score_pack.process(factory=triples_factory).df
    return scores_df.nlargest(n=5, columns="score")