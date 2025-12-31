import os
import json
from flask import Flask, request, abort

app = Flask(__name__)

VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "test_verify_token")

# =========================
# 1. VERIFY WEBHOOK (GET)
# =========================
@app.route("/webhook", methods=["GET"])
def verify_webhook():
    mode = request.args.get("hub.mode")
    token = request.args.get("hub.verify_token")
    challenge = request.args.get("hub.challenge")

    print("🔍 VERIFY REQUEST:")
    print("mode:", mode)
    print("token:", token)
    print("challenge:", challenge)

    if mode == "subscribe" and token == VERIFY_TOKEN:
        print("✅ WEBHOOK VERIFIED SUCCESSFULLY")
        return challenge, 200
    else:
        print("❌ WEBHOOK VERIFICATION FAILED")
        return "Forbidden", 403


# =========================
# 2. RECEIVE WEBHOOK (POST)
# =========================
@app.route("/webhook", methods=["POST"])
def receive_webhook():
    try:
        payload = request.get_json(force=True)
    except Exception as e:
        print("❌ FAILED TO PARSE JSON:", e)
        abort(400)

    print("\n==============================")
    print("📩 WEBHOOK RECEIVED")
    print("==============================")
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    print("==============================\n")

    # Ghi log chi tiết từng entry
    if payload.get("object") == "page":
        for entry in payload.get("entry", []):
            print("➡️ ENTRY ID:", entry.get("id"))
            print("➡️ TIME:", entry.get("time"))

            # COMMENT / FEED
            if "changes" in entry:
                for change in entry["changes"]:
                    print("📝 CHANGE FIELD:", change.get("field"))
                    print("📝 CHANGE VALUE:")
                    print(json.dumps(change.get("value"), indent=2, ensure_ascii=False))

            # MESSAGE (nếu có)
            if "messaging" in entry:
                for msg in entry["messaging"]:
                    print("💬 MESSAGING EVENT:")
                    print(json.dumps(msg, indent=2, ensure_ascii=False))

    else:
        print("⚠️ OBJECT IS NOT PAGE")

    return "EVENT_RECEIVED", 200


# =========================
# 3. HEALTH CHECK
# =========================
@app.route("/")
def index():
    return "Webhook test server is running", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
