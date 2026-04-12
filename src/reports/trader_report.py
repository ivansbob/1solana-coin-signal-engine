"""
Turns arbitrary execution mappings into Live Trader JSON & Markdown explicitly displaying trust matrices explicitly.
"""

from typing import Dict, Any, List
from src.reports.metric_catalog import get_metric

class TraderReportGenerator:
    """Consumes candidate evaluations producing heavily annotated MD bounds."""
    
    @staticmethod
    def generate_candidate_report(candidate_metrics: Dict[str, float], regime: str) -> str:
        md = []
        md.append(f"# Candidate Evaluation: Regime `{regime}`")
        if regime == "IGNORE":
            md.append("> **BLOCKER DETECTED**: Strategy enforces NO EXECUTION bounds.")
            
        md.append("## Metric Trace")
        md.append("| Metric | Value | Trust Level | Directionality | Provenance |")
        md.append("|---|---|---|---|---|")
        
        # Sort explicitly so execution_grade is at the absolute top validating logic bounds intuitively natively.
        trust_priority = {"execution_grade": 0, "research_grade": 1, "heuristic": 2, "context_only": 3, "unknown": 4, "degraded": 4}
        
        metric_items = []
        for key, val in candidate_metrics.items():
            ref = get_metric(key)
            # Degrade immediately if data is None natively
            if val is None:
                ref["trust_level"] = "degraded"
                val = "MISSING"
                
            metric_items.append((ref, val))
            
        metric_items.sort(key=lambda x: trust_priority.get(x[0]["trust_level"], 9))
        
        for ref, val in metric_items:
            trust_label = f"**{ref['trust_level'].upper()}**"
            if ref['trust_level'] == "heuristic": trust_label = f"*{ref['trust_level'].upper()}*"
            if ref['trust_level'] in ["unknown", "degraded"]: trust_label = f"~~{ref['trust_level'].upper()}~~"
            
            val_str = f"`{val:.2f}`" if isinstance(val, (int, float)) else f"`{val}`"
            
            md.append(f"| {ref['display_name']} | {val_str} | {trust_label} | {ref['directionality']} | {ref['source']} |")
            
        md.append("")
        md.append("## Executive Summary")
        md.append("Heuristic models heavily dictate bonus values but natively fail when **EXECUTION_GRADE** inputs drop below limits.")
        return "\n".join(md)
