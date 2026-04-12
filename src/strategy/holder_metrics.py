"""Holder Churn Rate metrics for identifying sticky vs flipping cohorts."""

from typing import Dict, Any, Optional

def compute_holder_churn_metrics(token_address: str, fetched_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Computes Holder Churn Rate: new_buyers_24h / total_unique_buyers_24h
    Returns score based on returning ratio.
    """
    if fetched_data is None:
        fetched_data = {}
        
    new_buyers_24h = fetched_data.get("new_buyers_24h")
    total_buyers_24h = fetched_data.get("total_buyers_24h")
    
    if new_buyers_24h is None or total_buyers_24h is None or total_buyers_24h == 0:
        return {
            "holder_churn_rate_24h": None,
            "new_buyers_ratio_24h": None,
            "returning_buyers_ratio_24h": None,
            "holder_churn_score": None,
            "holder_churn_provenance": {
                "source": "simulated",
                "error": "missing_data"
            }
        }
        
    churn_rate = new_buyers_24h / total_buyers_24h
    returning_ratio = 1.0 - churn_rate
    
    if returning_ratio >= 0.65:
        score = 1.0
    elif 0.40 <= returning_ratio < 0.65:
        score = 0.5
    else:
        score = 0.0
        
    return {
        "holder_churn_rate_24h": round(churn_rate, 4),
        "new_buyers_ratio_24h": round(churn_rate, 4),
        "returning_buyers_ratio_24h": round(returning_ratio, 4),
        "holder_churn_score": score,
        "holder_churn_provenance": {
            "source": fetched_data.get("source", "dune")
        }
    }
