"""FastAPI service exposing candidate funding lookups."""

from __future__ import annotations

from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query

from .search import candidate_summary, schedule_a_history, search_candidates, top_contributors

app = FastAPI(title="FEC Candidate Funding Lookup")


@app.get("/candidate")
def candidate_search(q: str = Query(..., description="Candidate name"), limit: int = Query(10, ge=1, le=50)):
    matches = search_candidates(q, limit=limit)
    return {"results": [match.__dict__ for match in matches]}


@app.get("/candidate/{cand_id}/summary")
def candidate_summary_endpoint(cand_id: str):
    data = candidate_summary(cand_id)
    if not data:
        raise HTTPException(status_code=404, detail="Candidate not found")
    return {"summary": data}


@app.get("/candidate/{cand_id}/schedule-a")
def candidate_schedule_a(
    cand_id: str,
    cycle: Optional[List[int]] = Query(None),
    limit: int = Query(200, ge=1, le=1000),
    page: int = Query(1, ge=1),
):
    rows = schedule_a_history(cand_id, cycles=cycle, limit=limit, page=page)
    return {"rows": rows, "page": page, "limit": limit}


@app.get("/candidate/{cand_id}/top-contributors")
def candidate_top_contributors(cand_id: str, limit: int = Query(20, ge=1, le=100)):
    rows = top_contributors(cand_id, limit=limit)
    return {"rows": rows}
