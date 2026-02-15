from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from src.app.settings import settings

#due to cloud migration, using blob
from src.app.services.data_loader import load_scores, load_report
from src.app.services.markdown import load_md_as_html

# from src.app.services.blob_artifacts import load_scores_df, load_report_dict, load_model_card_md
# from src.app.services.markdown import md_to_html

app = FastAPI(title="Equity Bayesian Dashboard", version="0.1")

templates = Jinja2Templates(directory=str(settings.repo_root / "src" / "app" / "templates"))
app.mount("/static", StaticFiles(directory=str(settings.repo_root / "src" / "app" / "static")), name="static")


@app.get("/health")
def health():
    return {"ok": True}

#due to cloud migration, using blob
@app.get("/api/scores")
def api_scores():
    df = load_scores(settings.today_scores_csv)
    return JSONResponse(df.to_dict(orient="records"))


@app.get("/api/report")
def api_report():
    rep = load_report(settings.model_report_json)
    return JSONResponse(rep)


@app.get("/api/model")
def api_model_card():
    html = load_md_as_html(settings.model_card_md)
    return JSONResponse({"html": html})

# @app.get("/api/scores")
# def api_scores():
#     df = load_scores_df()
#     df = df.sort_values("z_score", ascending=False).reset_index(drop=True)
#     return JSONResponse(df.to_dict(orient="records"))

# @app.get("/api/report")
# def api_report():
#     return JSONResponse(load_report_dict())

# @app.get("/api/model")
# def api_model_card():
#     md = load_model_card_md()
#     html = md_to_html(md)
#     return JSONResponse({"html": html})

@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    # server-side render shell; JS populates table via /api endpoints
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/model", response_class=HTMLResponse)
def model_page(request: Request):
    return templates.TemplateResponse("model.html", {"request": request})
