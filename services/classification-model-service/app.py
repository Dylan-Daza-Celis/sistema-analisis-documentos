from flask import Flask, request, jsonify
from classifier import DocumentClassifier
import logging

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Inicializar clasificador
logger.info("Inicializando clasificador...")
classifier = DocumentClassifier()
logger.info("Clasificador listo")


@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint"""
    return jsonify({"status": "ok"}), 200


@app.route("/classify", methods=["POST"])
def classify():
    """
    Endpoint para clasificar texto
    
    Body: {
        "text": "contenido del documento para clasificar"
    }
    
    Response: {
        "tema": "Computación",
        "confianza": 0.95,
        "metodo": "logistic_regression"
    }
    """
    try:
        data = request.get_json()
        
        if not data or "text" not in data:
            return jsonify({"error": "Campo 'text' requerido"}), 400
        
        text = data.get("text", "").strip()
        
        if not text:
            return jsonify({"error": "Texto vacío"}), 400
        
        result = classifier.classify(text)
        
        return jsonify(result), 200
        
    except Exception as e:
        logger.error(f"Error en clasificación: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/keywords", methods=["POST"])
def extract_keywords():
    """
    Endpoint para extraer palabras clave
    
    Body: {
        "text": "contenido del documento"
    }
    
    Response: {
        "keywords": ["palabra1", "palabra2", ...]
    }
    """
    try:
        data = request.get_json()
        
        if not data or "text" not in data:
            return jsonify({"error": "Campo 'text' requerido"}), 400
        
        text = data.get("text", "").strip()
        
        if not text:
            return jsonify({"error": "Texto vacío"}), 400
        
        keywords = classifier.extract_keywords(text)
        
        return jsonify({"keywords": keywords}), 200
        
    except Exception as e:
        logger.error(f"Error extrayendo keywords: {str(e)}")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
