import numpy as np
import torch
from pykeen.models import Model
from pykeen.pipeline import pipeline, PipelineResult
from pykeen.triples import TriplesFactory


def generate_training_set(fact_triples: list[tuple[str, str, str]]) -> tuple[TriplesFactory, TriplesFactory, TriplesFactory]:
    triples_array = np.array(fact_triples)

    tf = TriplesFactory.from_labeled_triples(triples_array)
    training, validation, testing = tf.split([0.8, 0.1, 0.1], random_state=42)

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

def load_model(model_dir_path: str) -> tuple[Model, TriplesFactory]:
    model = torch.load(model_dir_path + "/trained_model.pkl", weights_only=False)

    # Triples factory (for ID→label mapping)
    tf = TriplesFactory.from_path(
        path=f"{model_dir_path}/training_triples/numeric_triples.tsv.gz",
        entity_to_id_path=f"{model_dir_path}/training_triples/entity_to_id.tsv.gz",
        relation_to_id_path=f"{model_dir_path}/training_triples/relation_to_id.tsv.gz",
    )

    return model, tf