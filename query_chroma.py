import chromadb

client = chromadb.PersistentClient(path="data/chroma")
collection = client.get_collection("filings")
print("Chunks:", collection.count())

results = collection.query(
    query_texts=["What was the quarterly revenue?"],
    n_results=3,
)

for doc, meta, distance in zip(
    results["documents"][0], results["metadatas"][0], results["distances"][0]
):
    print(f"\n--- {meta['ticker']} {meta['date']} (distance {distance:.2f})")
    print(doc[:300])
