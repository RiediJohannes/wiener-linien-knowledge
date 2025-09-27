from typing import Sequence

import pandas as pd
from pyarrow import Tensor
from pykeen.models import Model
from pykeen.predict import predict_triples, predict_target, Predictions
from pykeen.triples import TriplesFactory

from src.components.types import Stop, ModeOfTransport, Frequency, parse_mode_of_transport, parse_frequency, Connection


class PredictionMachine:
    def __init__(self, embedding_model: Model, training_triples: TriplesFactory, *other_known_triples: TriplesFactory):
        self.model: Model = embedding_model
        self.training_triples: TriplesFactory = training_triples
        self.other_known_triples: list[Tensor] = [factory.mapped_triples for factory in other_known_triples]

    def score_potential_connections(self, stops_with_targets: list[tuple[str, list[str]]], *, connection_types: list[str] = None, order_ascending = True) -> pd.DataFrame:
        relations = connection_types if connection_types else ["BUS_CONNECTS_TO", "TRAM_CONNECTS_TO"]
        triples = [
            (start, relation, target)
            for start, targets in stops_with_targets
            for target in targets
            for relation in relations
        ]

        return self.score_triples(triples, order_ascending=order_ascending)

    def predict_connection_frequency(self, stops_with_targets: list[tuple[str, list[str]]], order_ascending = True):
        frequency_relations = ["NONSTOP_TO", "VERY_FREQUENTLY_TO", "FREQUENTLY_TO", "REGULARLY_TO", "OCCASIONALLY_TO", "RARELY_TO"]
        triples = [
            (start, relation, target)
            for start, targets in stops_with_targets
            for target in targets
            for relation in frequency_relations
        ]

        scored_df = self.score_triples(triples, order_ascending=order_ascending, apply_filter=False)

        # Keep only the highest-scoring relation for each (start, target) pair
        filtered_df = (
            scored_df
            .sort_values("score", ascending=order_ascending)
            .groupby(["head_label", "tail_label"], as_index=False)
            .first()  # Keep the first row (highest score) for each group
        )

        return filtered_df


    def score_triples(self, triples: Sequence[tuple[str, str, str]], order_ascending = True, apply_filter = True) -> pd.DataFrame:
        score_pack = predict_triples(
            model=self.model,
            triples_factory=self.training_triples,
            triples=triples
        )

        score_predictions = score_pack.process(factory=self.training_triples) # Convert ScorePack to Prediction
        score_dataframe = self.filter_predictions(score_predictions) if apply_filter else score_predictions.df
        return score_dataframe.sort_values(by=['score'], ascending=order_ascending)

    def predict_component(self, *, head: str = None, rel: str = None, tail: str = None, targets: Sequence[str] = None, apply_filter = True) -> pd.DataFrame:
        # Count how many of head, relation, and tail are not None
        not_none_count = sum(1 for param in [head, rel, tail] if param is not None)

        if not_none_count != 2:
            raise ValueError(f"Exactly two of 'head', 'relation', or 'tail' must be set. Got {not_none_count}")

        prediction = predict_target(
            model=self.model,
            triples_factory=self.training_triples,
            head=head,
            relation=rel,
            tail=tail,
            targets=targets
        )

        return self.filter_predictions(prediction) if apply_filter else prediction.df

    # noinspection PyTypeChecker
    def filter_predictions(self, triple_predictions: Predictions) -> pd.DataFrame:
        """
        Removes predicted triples that are already known to be true since they were part of either the
        training, validation or testing set.
        """
        filter_triples = [self.training_triples.mapped_triples] + self.other_known_triples
        return triple_predictions.filter_triples(*filter_triples).df

def create_connections(connection_triples: list[tuple[str,str,str]], stops_by_id: dict[str, Stop]) -> list[Connection]:
    connections = []
    for head, rel, tail in connection_triples:
        from_stop: Stop = stops_by_id[head]
        to_stop: Stop = stops_by_id[tail]
        mode_of_transport: ModeOfTransport = parse_mode_of_transport(rel)
        frequency: Frequency = parse_frequency(rel)

        connections.append(Connection(from_stop, to_stop, mode_of_transport, frequency))

    return connections