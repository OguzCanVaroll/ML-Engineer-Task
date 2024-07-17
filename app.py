from flask import Flask, jsonify
import threading
import consumer

app = Flask(__name__)

threading.Thread(target=consumer.consume_messages, daemon=True).start()

@app.route('/products', methods=['GET'])
def get_products():
    return jsonify(consumer.products)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
