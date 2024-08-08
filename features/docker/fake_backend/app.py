import logging

from flask import Flask, request

app = Flask(__name__)

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="BACKEND - %(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


@app.route("/request-ids/<request_id>")
def test_id(request_id):
    """
    The <request_id> will appear in the docker logs
    once this endpoint is called. That's all we need.
    """
    logging.info(f"Scenario: {request.get_data()}")
    return "", 200


@app.route("/", defaults={"path": ""})
@app.route("/<path:path>", methods=["GET", "POST"])
def main(path):
    logging.info(f"this-requests-path: {path}")
    logging.info(f"this-requests-url: {request.url}")
    logging.info(f"this-requests-headers: {dict(request.headers)}")
    logging.info(f"this-requests-body: {request.get_data()}")
    logging.info(f"this-requests-json: {request.get_json(silent=True)}")
    return "I am the backend", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
