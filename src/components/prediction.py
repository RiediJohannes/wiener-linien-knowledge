from typing import Sequence

import pandas as pd
from pyarrow import Tensor
from pykeen.models import Model
from pykeen.predict import predict_triples, predict_target, Predictions
from pykeen.triples import TriplesFactory


class PredictionMachine:
    def __init__(self, embedding_model: Model, training_triples: TriplesFactory, *other_known_triples: TriplesFactory):
        self.model: Model = embedding_model
        self.training_triples: TriplesFactory = training_triples
        self.other_known_triples: list[Tensor] = [factory.mapped_triples for factory in other_known_triples]

    def score_potential_connections(self, stops_with_targets: list[tuple[str, list[str]]], connection_types: list[str] = None, order_ascending = True) -> pd.DataFrame:
        relations = connection_types if connection_types else ["BUS_CONNECTS_TO", "TRAM_CONNECTS_TO"]
        triples = [
            (start, relation, target)
            for start, targets in stops_with_targets
            for target in targets
            for relation in relations
        ]

        return self.score_triples(triples, order_ascending=order_ascending)

    def score_triples(self, triples: Sequence[tuple[str, str, str]], order_ascending = True) -> pd.DataFrame:
        score_pack = predict_triples(
            model=self.model,
            triples_factory=self.training_triples,
            triples=triples
        )

        score_dataframe = score_pack.process(factory=self.training_triples).df
        return score_dataframe.sort_values(by=['score'], ascending=order_ascending)

    def predict_tail(self, head: str, relation: str, targets: Sequence[str] = None) -> pd.DataFrame:
        prediction = predict_target(
            model=self.model,
            triples_factory=self.training_triples,
            head=head,
            relation=relation
        )

        filter_triples = [self.training_triples.mapped_triples] + self.other_known_triples
        pred_filtered = prediction.filter_triples(*filter_triples)
        return pred_filtered