from recommenders.evaluation.python_evaluation import (
    map_at_k,
    ndcg_at_k,
    precision_at_k,
    recall_at_k,
    rmse,
    mae,
    logloss,
    rsquared,
    exp_var
)
from recommenders.models.sar import SAR


class RecommenderSystem:
    def __init__(
        self,
        col_user="userID",
        col_item="itemID",
        col_rating="rating",
        col_timestamp="timestamp",
        similarity_type="jaccard",
        time_decay_coefficient=30,
        timedecay_formula=True,
        normalize=True
    ):
        
        """Starting SAR model with the needed parameters"""
        self.model = SAR(
            col_user=col_user,
            col_item=col_item,
            col_rating=col_rating,
            col_timestamp=col_timestamp,
            similarity_type=similarity_type,
            time_decay_coefficient=time_decay_coefficient,
            timedecay_formula=timedecay_formula,
            normalize=normalize
        )
        self.fitted = False

    def fit(self, train):
        """training of SAR model."""
        self.model.fit(train)
        self.fitted = True
        return self.model
    

    def prediction(self, test, top_k=10):
        if not self.fitted:
            raise RuntimeError("No trained model.call fit() first.")
        
        # prediction
        preds = self.model.recommend_k_items(test.drop_duplicates(), top_k=10, remove_seen=True)
        return preds

    def evaluate(self, test, top_k=10):
        """Evaluation on test set."""
        if not self.fitted:
            raise RuntimeError("No trained model.call fit() first.")
        
        # prediction
        preds = self.model.recommend_k_items(test, top_k=top_k, remove_seen=True)
        
        # metrics
        metrics = {
            "map": map_at_k(test, preds, col_user='userID', col_item='itemID', col_rating='rating', k=top_k),
            "ndcg": ndcg_at_k(test, preds, col_user='userID', col_item='itemID', col_rating='rating', k=top_k),
            "precision": precision_at_k(test, preds, col_user='userID', col_item='itemID', col_rating='rating', k=top_k),
            "recall": recall_at_k(test, preds, col_user='userID', col_item='itemID', col_rating='rating', k=top_k)
        }
        return metrics

    def get_model(self):
        """returns SAR object."""
        return self.model
