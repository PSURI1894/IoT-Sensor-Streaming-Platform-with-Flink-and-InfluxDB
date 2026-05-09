from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="Predictive Maintenance Real-time Inference API")

class TelemetryPayload(BaseModel):
    asset_id: str
    vibration: float
    temperature: float
    pressure: float

@app.post("/predict")
def predict_ttf(data: TelemetryPayload):
    # Mock real-time ML XGBoost model scoring
    ttf_hours = 240.0 # Standard operating hours remaining
    risk_prob = 0.02
    
    if data.vibration > 4.0 or data.temperature > 85.0:
        ttf_hours = 8.5
        risk_prob = 0.94
        
    return {
        "asset_id": data.asset_id,
        "time_to_failure_hours": ttf_hours,
        "risk_probability": risk_prob,
        "action": "SCHEDULE_MAINTENANCE" if risk_prob > 0.50 else "OPERATE_NORMAL"
    }
