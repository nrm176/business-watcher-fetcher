from typing import Optional, List, Tuple, Dict, Any
from datetime import date, datetime
from pydantic import BaseModel, Field
from pydantic import field_validator
import pandas as pd


class WatcherRecord(BaseModel):
    id: str = Field(..., description="Deterministic md5 hex digest")
    dtype: str = Field(..., description="現状 or 先行き")
    category: str
    reason: Optional[str] = None
    region: str
    dt: date
    comments: Optional[str] = None
    industry: Optional[str] = None
    industry_detail: Optional[str] = None
    job_title: Optional[str] = None
    pref: Optional[str] = None
    score: float

    @field_validator('id')
    def id_is_hex(cls, v: str) -> str:
        if not isinstance(v, str) or len(v) != 32:
            raise ValueError('id must be a 32-char md5 hex string')
        try:
            int(v, 16)
        except Exception:
            raise ValueError('id must be hex')
        return v

    @field_validator('dtype')
    def dtype_allowed(cls, v: str) -> str:
        allowed = {'現状', '先行き'}
        if v not in allowed:
            raise ValueError(f'dtype must be one of {allowed}')
        return v

    @field_validator('dt', mode='before')
    def parse_dt(cls, v):
        if isinstance(v, date):
            return v
        if isinstance(v, datetime):
            return v.date()
        if isinstance(v, str):
            # supports both YYYY-MM-DD and pandas Timestamp str
            try:
                return datetime.strptime(v, '%Y-%m-%d').date()
            except Exception:
                try:
                    return pd.to_datetime(v).date()
                except Exception:
                    pass
        raise ValueError('dt must be a date-like value (YYYY-MM-DD)')

    @field_validator('score', mode='before')
    def float_score(cls, v):
        try:
            return float(v)
        except Exception:
            raise ValueError('score must be a float')


def validate_dataframe(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, List[Dict[str, Any]]]:
    """
    Validate dataframe rows using WatcherRecord. Returns (valid_df, invalid_df, errors)
    Errors contain {'index': idx, 'errors': str, 'row': dict}
    """
    required_cols = ['id', 'dtype', 'category', 'region', 'dt', 'score']
    for c in required_cols:
        if c not in df.columns and c != 'id':
            # 'id' might be index; below we handle that
            raise ValueError(f'Missing required column: {c}')

    # Ensure 'id' is a column (not only index)
    work_df = df.reset_index()

    val_rows = []
    bad_rows = []
    errors: List[Dict[str, Any]] = []

    for idx, row in work_df.iterrows():
        payload = row.to_dict()
        try:
            rec = WatcherRecord(**payload)
            val_rows.append(rec.model_dump())
        except Exception as e:
            bad_rows.append(payload)
            errors.append({'index': payload.get('id', idx), 'errors': str(e), 'row': payload})

    valid_df = pd.DataFrame(val_rows)
    invalid_df = pd.DataFrame(bad_rows)

    if not valid_df.empty:
        valid_df = valid_df.set_index('id')

    if not invalid_df.empty and 'id' in invalid_df.columns:
        invalid_df = invalid_df.set_index('id', drop=False)

    return valid_df, invalid_df, errors
