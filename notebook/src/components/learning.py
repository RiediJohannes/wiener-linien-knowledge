import numpy as np
import pandas as pd
import torch
import os
from pykeen.evaluation import MetricResults, RankBasedEvaluator
from pykeen.evaluation.rank_based_evaluator import RankBasedMetricKey
from pykeen.models import Model
from pykeen.pipeline import pipeline, PipelineResult
from pykeen.triples import TriplesFactory, leakage


MODELS_SOURCE = os.path.join("notebook", "trained_models")

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


def save_training_results(results: PipelineResult, model_dir_path: str,
                          validation_triples: TriplesFactory = None, testing_triples: TriplesFactory = None) -> None:
    """
    Save the final model as well as training metrics and training triples to the local file system.
    :param results: The PipelineResults object obtained from the training pipeline.
    :param model_dir_path: The path to a directory where all output files will be saved to.
    :param validation_triples: The triples used for validation during training.
    :param testing_triples: The triples used for testing after training.
    """

    # Export the trained model and its training triples
    results.save_to_directory(model_dir_path)

    # Export validation and testing triples, if there were any
    if validation_triples:
        validation_triples.to_path_binary(os.path.join(model_dir_path, "validation_triples"))
    if testing_triples:
        testing_triples.to_path_binary(os.path.join(model_dir_path, "testing_triples"))

    # Export training metrics to a CSV file
    results_dataframe = results.metric_results.to_df()
    results_dataframe.to_csv(os.path.join(model_dir_path, 'metrics.csv'), index=False)


def available_models() -> list[str]:
    return os.listdir(MODELS_SOURCE)

def load_model(model_name: str) -> tuple[Model, TriplesFactory]:
    model_dir = _get_model_path(model_name)
    return load_model_from_path(model_dir)

def load_training_results(model_name: str) -> pd.DataFrame:
    model_dir = _get_model_path(model_name)
    return load_training_results_from_path(model_dir)

def load_triples(model_name: str) -> tuple[TriplesFactory, TriplesFactory, TriplesFactory]:
    model_dir = _get_model_path(model_name)
    return load_triples_from_path(model_dir)


def load_model_from_path(model_dir_path: str) -> tuple[Model, TriplesFactory]:
    source_dir = _get_model_source_dir(model_dir_path)
    model = torch.load(
        os.path.join(source_dir, "trained_model.pkl"),
        map_location=torch.device('cuda' if torch.cuda.is_available() else 'cpu'),
        weights_only=False
    )

    # Training triples factory (for ID→label mapping)
    training_triples = TriplesFactory.from_path_binary(path=os.path.join(source_dir, "training_triples"))
    return model, training_triples

def load_training_results_from_path(model_dir_path: str) -> pd.DataFrame:
    csv_path = os.path.join(_get_model_source_dir(model_dir_path), 'metrics.csv')
    return pd.read_csv(csv_path)

def load_triples_from_path(model_dir_path: str) -> tuple[TriplesFactory, TriplesFactory, TriplesFactory]:
    triples_source = _get_model_source_dir(model_dir_path)
    training = TriplesFactory.from_path_binary(path=os.path.join(triples_source, "training_triples"))
    validation = TriplesFactory.from_path_binary(path=os.path.join(triples_source, "validation_triples"))
    testing = TriplesFactory.from_path_binary(path=os.path.join(triples_source, "testing_triples"))
    return training, validation, testing


def summarize_training_metrics(metrics: MetricResults) -> pd.DataFrame:
    return pd.DataFrame({
        "MRR": [metrics.get_metric("mrr")],
        "Hits@10": [metrics.get_metric("hits_at_10")],
        "Hits@5": [metrics.get_metric("hits_at_5")],
        "Hits@3": [metrics.get_metric("hits_at_3")],
        "Hits@1": [metrics.get_metric("hits_at_1")],
        "Mean Rank": [metrics.get_metric("mr")],
    })

def evaluate_model(model: Model, testing_triples: TriplesFactory, other_known_triples: list[TriplesFactory]) -> MetricResults[RankBasedMetricKey]:
    evaluator = RankBasedEvaluator(
        filtered=True,  # Note: this is True by default; we're just being explicit
    )

    other_triples = [factory.mapped_triples for factory in other_known_triples]

    # Evaluate your model with not only testing triples,
    # but also filter on validation triples
    return evaluator.evaluate(
        model=model,
        mapped_triples=testing_triples.mapped_triples,
        additional_filter_triples=other_triples
    )

def _get_model_path(model_name: str) -> str:
    models_dict = {d.name.lower(): d.path for d in os.scandir(MODELS_SOURCE) if d.is_dir()}

    if model_name.lower() not in models_dict.keys():
        raise ValueError(f"Model with name '{model_name}' could not be found.")

    return models_dict[model_name.lower()]

def _get_model_source_dir(model_dir_path: str) -> str:
    return model_dir_path\
        if os.path.splitroot(model_dir_path)[2].startswith("notebook")\
        else os.path.join("notebook", model_dir_path)
