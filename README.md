# parent-illness-absences

Reproducible Python project to test whether U.S. parents today miss work for their own illness more than in the 1990s/2000s, using CPS microdata (via the IPUMS Microdata Extract API) and a quick cross-check from BLS CPS tables A-46/A-47.

Data sources
- IPUMS CPS Basic Monthly microdata (1994–present). Requires free IPUMS account + API key.
- BLS CPS A-46/A-47 tables (own illness absences for full-time workers) for national context.

Key variable definitions (IPUMS-CPS names)
- Employment: `EMPSTAT` to restrict to employed civilians; hours: `UHRSWORKT`; class: `CLASSWKR`.
- Absence status: `ABSENT` (has job, not at work last week).
- Absence reason: `WHYABSNT` (own illness/injury/medical vs other). Own illness indicator defined as `ABSENT==1` and `WHYABSNT` in IPUMS codes for own illness/injury/medical.
- Parent flags: `NCHILD>0` OR co-resident parent via `MOMLOC>0 or POPLOC>0` with a co-resident child; stricter `has_child_u5` via `NCHLT5>0`.

What this project does
- Pulls CPS microdata (IPUMS API) for 1994–present with needed variables.
- Constructs monthly rates of own-illness absences among 25–49-year-old employed civilians, split by parents vs non-parents (optionally by sex, education).
- Scrapes BLS A-46/A-47 (non-parent-stratified) “own illness” absence series for context.
- Runs DiD models across periods: P1=1994–2007, P2=2008–2019, P3=2020–present with state and month fixed effects and covariates.
- Produces figures and a small HTML report.

Repro steps
1) Create a free IPUMS account and generate an API key:
   - https://usas.ipums.org/ (Account) → https://ipums.org/support/extract-api
   - Save your key in a `.env` file at the repo root:
     
     ```.env
     IPUMS_API_KEY=YOUR_IPUMS_KEY
     ```

2) Install dependencies (uv/poetry optional; pip works):
   - `python -m venv .venv && source .venv/bin/activate`
   - `pip install -r requirements.txt`

3) Pull data and build outputs:
   - API mode: `make ipums`  # submits IPUMS CPS extract and downloads when ready (parquet in data/raw)
     - For a quick run, set `IPUMS_YEARS=1994-1996,2018-2021` in `.env`.
   - Manual mode (no API): Create an extract on https://cps.ipums.org/ with the variables listed below,
     choose CSV output, download it locally, then add to `.env`:
     
     ```.env
     IPUMS_LOCAL_CSV=/absolute/path/to/your_ipums_cps_extract.csv
     ```
     
     Then run `make build` and `make figs`.
   - `make bls`    # scrapes BLS A-46/A-47 into data/processed/bls_absences.csv
   - `make build`  # processes CPS into monthly parent vs non-parent rates; runs DiD
   - `make figs`   # renders figures and figures/index.html

4) Open the HTML report:
   - `open figures/index.html` (macOS) or view the PNGs in `figures/`.

Caveats
- Parents are measured via co-residence flags and counts (`NCHILD`, `MOMLOC`, `POPLOC`, `NCHLT5`). Non-cohabiting parents may be missed.
- Seasonality and business cycle effects are present; we include fixed effects, but caution is warranted.
- 2020+ pandemic regime shifts alter absence behavior; we model with a separate period (P3).
- BLS A-46/A-47 are table aggregates (not parent-stratified) and serve as a sanity check only.

GitHub remote (manual snippet)
```
git init -b main
git add .
git commit -m "Initial commit: parent-illness-absences scaffold"
git remote add origin git@github.com:<YOUR_USERNAME>/parent-illness-absences.git
git push -u origin main
```

Optional helper script
- You can put a `GITHUB_TOKEN` in `.env` (fine-grained PAT with repo access) and run `python scripts/push.py` to add/commit/push (the script shells out to git and does not store tokens).

References
- IPUMS CPS Microdata Extract API workflow: https://developer.ipums.org/docs/microdata_extracts_api/
- IPUMS CPS variables: ABSENT, WHYABSNT, NCHILD, NCHLT5, MOMLOC, POPLOC, EMPSTAT: https://cps.ipums.org/cps-action/variables/
- BLS CPS A-46/A-47 tables: https://www.bls.gov/cps/cpsaat46.htm and https://www.bls.gov/cps/cpsaat47.htm
