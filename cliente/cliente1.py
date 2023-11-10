import rpyc
import json
from flask import Flask, request, jsonify

app = Flask(__name__)

porta_control1 = 18861
porta_control2 = 18862

proxy_control1 = rpyc.connect('localhost', porta_control1, config={'allow_public_attrs': True})
proxy_control2 = rpyc.connect('localhost', porta_control2, config={'allow_public_attrs': True})

@app.route('/', methods=['GET'])
def buscar_dados():
    dados = proxy_control1.root.retorna_todos_dados()

    json_data = json.dumps([{'data_hora': item[0], 'umidade': item[1], 'molhou': item[2]} for item in dados])
    return json.loads(json_data)


@app.route('/', methods=['PUT'])
def atualiza_limiar():
    novo_limiar = request.args.get('novo_limiar')
    proxy_control1.root.altera_limiar(novo_limiar)
    return jsonify({'novo_limiar': novo_limiar})


app.run(debug=True)