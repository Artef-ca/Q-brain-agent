import os, requests, uuid

BASE = os.getenv("BASE_URL","http://localhost:8080")
payload = {
    "user_id": "antony",
    "session_id": str(uuid.uuid4()),
    "question": "How many rows are in the main ticketing table?",
    "uploaded_doc_refs": [],
    "message_context_ref": None
}

r = requests.post(f"{BASE}/chat", json=payload, timeout=120)
print(r.status_code)
print(r.json())
