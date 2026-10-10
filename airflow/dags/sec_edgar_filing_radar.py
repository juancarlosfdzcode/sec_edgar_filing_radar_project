import json
import re
import time
import os
from datetime import date, datetime, timedelta
from pathlib import Path

import requests
from airflow.sdk import dag, task
from bs4 import BeautifulSoup

HEADERS = {"User-Agent": "$YOUR_EMAIL"}
COMPANIES = {"AAPL": "320193", "NVDA": "1045810", "MSFT": "789019"}
LOOKBACK_DAYS = 120

DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "filings"
CHROMA_DIR = DATA_DIR.parent / "chroma"
ANSWERS_DIR = DATA_DIR.parent / "answers"
REPORTS_DIR = DATA_DIR.parents[1] / "reports"

LLM_URL = "https://api.groq.com/openai/v1/chat/completions"
LLM_MODEL = "openai/gpt-oss-20b"
TOP_K = 4

QUESTIONS = [
    "What were the most recent quarterly financial results?",
    "Were there any executive or board changes?",
    "What other material events were disclosed?",
]

SYSTEM_PROMPT = (
    "You answer questions about SEC 8-K filings. Use only the excerpts provided. "
    "If they do not contain the answer, reply exactly: "
    "'Not mentioned in recent filings.' "
    "Be concise (at most four sentences) and include figures when available."
)


def fetch(url: str) -> requests.Response:
    """GET a SEC URL with the required User-Agent, raising on HTTP errors."""
    time.sleep(0.2)
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r


def clean(html: str) -> str:
    """Strip tags, styles and XBRL metadata, keeping only the text."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "ix:header"]):
        tag.decompose()
    text = soup.get_text(separator="\n")
    text = re.sub(r"[ \t\xa0]+", " ", text)
    return re.sub(r"\n\s*\n+", "\n\n", text).strip()


def chunk(text: str, size: int = 1000, overlap: int = 150) -> list[str]:
    """Split text into overlapping windows of roughly `size` characters."""
    step = size - overlap
    return [text[start:start + size] for start in range(0, len(text), step)]


def ask_llm(question: str, context: str) -> str:
    """Answer a question using only the given excerpts. 
	Retries with a pause when the API returns a rate-limit error.
    """
    payload = {
        "model": LLM_MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": f"Excerpts:\n{context}\n\nQuestion: {question}",
            },
        ],
    }
    headers = {"Authorization": f"Bearer {os.environ['GROQ_API_KEY']}"}

    for _ in range(5):
        response = requests.post(LLM_URL, headers=headers, json=payload, timeout=60)
        if response.status_code == 429:
            wait = float(response.headers.get("retry-after", 20))
            print(f"Rate limited, waiting {wait:.0f}s")
            time.sleep(wait)
            continue
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"].strip()

    raise RuntimeError("LLM rate limit not cleared after 5 attempts")


def recent_8ks(cik: str, since: str) -> tuple[str, list[dict]]:
    """Return the company name and its 8-K filings since the given date."""
    data = fetch(f"https://data.sec.gov/submissions/CIK{int(cik):010d}.json").json()
    recent = data["filings"]["recent"]
    rows = zip(
        recent["form"],
        recent["filingDate"],
        recent["accessionNumber"],
        recent["items"],
    )
    filings = [
        {"date": filed, "accession": accession, "items": items}
        for form, filed, accession, items in rows
        if form == "8-K" and filed >= since
    ]
    return data["name"], filings


def download_text(cik: str, accession: str) -> tuple[str, str]:
    """Download the main document and exhibit 99.1 as clean text."""
    folder = (
        f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession.replace('-', '')}"
    )
    index_url = f"{folder}/{accession}-index.htm"
    soup = BeautifulSoup(fetch(index_url).text, "html.parser")

    docs = {}
    for row in soup.select("table.tableFile tr"):
        cells = row.find_all("td")
        if len(cells) >= 4 and cells[2].a:
            docs.setdefault(
                cells[3].get_text(strip=True), cells[2].a.get_text(strip=True)
            )

    parts = []
    for doc_type in ("8-K", "EX-99.1"):
        name = docs.get(doc_type, "")
        if name.endswith((".htm", ".html")):
            parts.append(clean(fetch(f"{folder}/{name}").text))
    return "\n\n".join(parts), index_url


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["sec-edgar-filing-radar"],
)

def sec_edgar_filing_radar():

    @task
    def ingest() -> list[str]:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        since = (date.today() - timedelta(days=LOOKBACK_DAYS)).isoformat()
        new_files = []

        for ticker, cik in COMPANIES.items():
            company, filings = recent_8ks(cik, since)
            for filing in filings:
                path = DATA_DIR / f"{ticker}_{filing['accession']}.json"
                if path.exists():
                    continue

                text, url = download_text(cik, filing["accession"])
                record = {
                    "ticker": ticker,
                    "company": company,
                    "url": url,
                    "text": text,
                    **filing,
                }
                path.write_text(json.dumps(record, ensure_ascii=False, indent=2))
                new_files.append(str(path))
                print(f"{ticker} {filing['date']} ({filing['items']}): {len(text)} chars")

        print(f"New filings: {len(new_files)}")
        return new_files

    @task
    def index(paths: list[str]) -> int:
        import chromadb

        client = chromadb.PersistentClient(path=str(CHROMA_DIR))
        collection = client.get_or_create_collection("filings")
        total = 0

        for path in paths:
            record = json.loads(Path(path).read_text())
            chunks = chunk(record["text"])
            if not chunks:
                continue

            metadata = {
                key: record[key]
                for key in ("ticker", "company", "date", "items", "url")
            }
            collection.upsert(
                ids=[f"{record['accession']}-{i}" for i in range(len(chunks))],
                documents=chunks,
                metadatas=[metadata] * len(chunks),
            )
            total += len(chunks)
            print(f"{record['ticker']} {record['date']}: {len(chunks)} chunks")

        print(f"Chunks indexed: {total}")
        return total

    @task
    def answer(indexed: int) -> str:
        import chromadb

        client = chromadb.PersistentClient(path=str(CHROMA_DIR))
        collection = client.get_or_create_collection("filings")
        results = []

        for ticker in COMPANIES:
            for question in QUESTIONS:
                hits = collection.query(
                    query_texts=[question],
                    n_results=TOP_K,
                    where={"ticker": ticker},
                )
                docs, metas = hits["documents"][0], hits["metadatas"][0]
                if not docs:
                    continue

                context = "\n\n".join(
                    f"[{meta['date']}] {doc}" for doc, meta in zip(docs, metas)
                )
                results.append(
                    {
                        "ticker": ticker,
                        "question": question,
                        "answer": ask_llm(f"{question} (company: {ticker})", context),
                        "sources": sorted({meta["url"] for meta in metas}),
                    }
                )
                print(f"{ticker}: {question}")

        ANSWERS_DIR.mkdir(parents=True, exist_ok=True)
        path = ANSWERS_DIR / f"{date.today().isoformat()}.json"
        path.write_text(json.dumps(results, ensure_ascii=False, indent=2))
        print(f"Answers saved: {len(results)}")
        return str(path)

    @task
    def report(answers_path: str) -> str:
        answers = json.loads(Path(answers_path).read_text())
        day = Path(answers_path).stem
        lines = [f"# Filing Radar report for {day}", ""]

        for ticker in COMPANIES:
            lines += [f"## {ticker}", ""]
            for item in answers:
                if item["ticker"] != ticker:
                    continue
                lines += [f"### {item['question']}", "", item["answer"], ""]
                if not item["answer"].startswith("Not mentioned"):
                    lines += ["Sources:", ""]
                    lines += [f"- {url}" for url in item["sources"]]
                    lines.append("")

        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        path = REPORTS_DIR / f"{day}.md"
        path.write_text("\n".join(lines))
        print(f"Report written to {path}")
        return str(path)

    report(answer(index(ingest())))

sec_edgar_filing_radar()
