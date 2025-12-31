from flask import Flask, request
import json
import sys

app = Flask(__name__)

VERIFY_TOKEN = "YOUR_VERIFY_TOKEN"

@app.route("/webhook", methods=["GET"])
def verify():
    mode = request.args.get("hub.mode")
    token = request.args.get("hub.verify_token")
    challenge = request.args.get("hub.challenge")

    print("🔔 VERIFY CALLED", flush=True)

    if mode == "subscribe" and token == VERIFY_TOKEN:
        print("✅ VERIFY SUCCESS", flush=True)
        return challenge, 200
    else:
        print("❌ VERIFY FAILED", flush=True)
        return "Forbidden", 403


@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.get_json()
        print("\n================ WEBHOOK RECEIVED ================", flush=True)
        print(json.dumps(data, indent=2, ensure_ascii=False), flush=True)
        print("=================================================\n", flush=True)
    except Exception as e:
        print("❌ ERROR:", str(e), flush=True)

    return "OK", 200


@app.route("/")
def home():
    return "Webhook test running", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
