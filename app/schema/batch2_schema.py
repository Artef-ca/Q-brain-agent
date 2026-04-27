from pydantic import BaseModel, Field
from typing import List, Optional, Literal, Dict, Any, Union


class Trend(BaseModel):
    value: str
    direction: Literal["up", "down"]
    color: Literal["green", "red"]


class Change(BaseModel):
    value: str
    percentage: Optional[str] = None
    direction: Literal["up", "down"]


class KpiCardBlock(BaseModel):
    type: Literal["kpi-card"]
    title: str
    value: str
    subtitle: str
    trend: Optional[Trend] = None


class ComparisonItem(BaseModel):
    label: str
    currentValue: str
    change: Change


class ComparisonBlock(BaseModel):
    type: Literal["comparison"]
    title: str
    subtitle: str
    items: List[ComparisonItem]


class ParagraphDividerBlock(BaseModel):
    type: Literal["paragraph-divider"]
    paragraphs: List[str]


class ChartBlock(BaseModel):
    type: Literal["chart"]
    chartType: Literal["area", "bar", "pie", "line", "donut", "radar", "radial"]
    title: str = Field(description="Clear, concise title for the chart. Must summarize the main metric or insight being visualized (e.g., 'Revenue by Quarter', 'Top 10 Customers by Sales').")
    subtitle: str = Field(description="Brief explanation of what the chart shows. This is *MANDATORY* and must provide context.")
    data: List[Dict[str, Any]]
    height: int = Field(default=300, description="Height of the chart in pixels. This is *MANDATORY* and must always be 300.")


class TableBlock(BaseModel):
    type: Literal["table"]
    headers: List[str]
    rows: List[List[str]]


Batch2Block = Union[
    KpiCardBlock,
    ComparisonBlock,
    ParagraphDividerBlock,
    ChartBlock,
    TableBlock,
]


    
class Batch2Response(BaseModel):
    type: Literal["batch2"]
    blocks: List[Batch2Block]
