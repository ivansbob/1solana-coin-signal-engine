# Smart Money Distance & Bundle Shield

Evaluating smart wallets solely on their inclusion ignores fundamental physics: jumping blindly into a smart wallet signal *late* transforms you into their exit liquidity. The Smart Money Distance metrics directly measure how far the current price has strayed from the origin.

## Core Rules

The engine calculates `distance_from_smart_entry_pct`.
- **Optimal Safety:** (Distance 0–32%). Original accumulators aren't far enough in profit to comfortably dump the supply aggressively. Yields a max 1.0 modifier.
- **Moderate Warning:** (Distance 32–55%). Yields 0.70 modifier.
- **Danger Zone:** (Distance 55–80%). They are already up substantially. Yields 0.35 score + warning.
- **Overextended Chase:** (> 80%). The train left. Force Score 0.0 and issues extreme penalties if bundle structures are also chaotic.

## Missing Data Handling

A core feature of the bootstrap is handling APIs that fail to sync historical wallets fast enough.
If `smart_cohort_weighted_avg_entry_price` is missing, the platform defensively infers distance = 100%. We intentionally sabotage unknown situations because treating a missing entry point as an "Optimistic Origin" will murder equity when bots chase late peaks. This forces the token to drop if no other algorithmic variables save it.

## The Bundle Shield

Calculates localized saturation scaling down modifiers based on `.recent_bundle_ratio` and `.bundle_sell_pressure`. High bundle concentrations indicate inorganic pushing, scaling safety scores sequentially to zero and triggering soft blockers against automated executions.
