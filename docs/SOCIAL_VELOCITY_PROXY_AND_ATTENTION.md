# Social Velocity and Attention Distortion

Crypto assets moving exclusively via paid Twitter manipulation create extreme "Rug Risk".
The Social Velocity algorithmic pipeline distinctly tracks velocity over short spans (10-minutes vs 60-minutes) capturing organic accelerating shifts. 

## Secondary Status
Because social analytics easily trigger false positives, `SocialVelocityScore` maxes at 8.0 parameter points explicitly operating purely as a *secondary bonus modifier*, completely unable to override garbage underlying parameters natively. 

## Attention Distortion Rules
Hype is neutralized entirely `(Social Score = 0.0)` if metrics track significant:
- Automated Bot copying.
- Paid influencer deployments.
- Social momentum violently decoupled from organic liquidity movements. 
  
Missing proxy metrics naturally degenerate the social modifier to 0.0, adding perfectly neutral components instead of penalizing heavily.

## Narrative Velocity Proxy
The Narrative Velocity Proxy extends social velocity analysis by focusing specifically on X (Twitter) and Telegram mentions, using a shorter 5-minute window compared to the 60-minute baseline. This allows detection of genuine narrative explosions before they fully manifest in price.

### Acceleration Ratio Formula
```
Velocity_5m = mentions_X+TG_5m
Velocity_60m = mentions_X+TG_60m
AccelerationRatio = Velocity_5m / (Velocity_60m + 1)
```

### Scoring Thresholds
- **1.0**: AccelerationRatio ≥ 3.0 (explosive narrative growth)
- **0.65**: 1.8 ≤ AccelerationRatio < 3.0 (strong growth)
- **0.3**: 1.2 ≤ AccelerationRatio < 1.8 (steady growth)
- **0.0**: Otherwise (fading or no growth)

Scores below 0.3 trigger warnings for low narrative support, indicating potential weak community engagement or artificial hype.
