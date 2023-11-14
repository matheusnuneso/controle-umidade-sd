import rpyc
import random
import json
from flask import Flask, request, jsonify

app = Flask(__name__)

porta_controls = [18861, 18862]

porta_control1 = 18861
porta_control2 = 18862

@app.route('/', methods=['GET'])
def buscar_dados():
    try:
        #tenta conexão no controlador 1
        proxy_control = rpyc.connect('localhost', porta_control1, config={'allow_public_attrs': True, 'sync_request_timeout': 2})
    except ConnectionRefusedError as e:
    
        try:
            #tenta conexão no controlador 2
            proxy_control = rpyc.connect('localhost', porta_control2, config={'allow_public_attrs': True, 'sync_request_timeout': 2})

        except ConnectionRefusedError as e:
            return jsonify({"sucesso": False, "erro": "Os dois controladores estão fora"})
        
    dados = proxy_control.root.retorna_todos_dados()
    json_data = json.dumps([{'data_hora': item[0], 'umidade': item[1], 'molhou': item[2]} for item in dados])

    return json.loads(json_data)



@app.route('/', methods=['PUT'])
def atualiza_limiar():
    sucesso_control = False
    novo_limiar = request.args.get('novo_limiar')

    random_gerado = random.randint(0, 1)

    controlador = porta_controls[random_gerado]
    controlador_reserva = porta_control2 if controlador == porta_control1 else porta_control1

    try:
        proxy_control = rpyc.connect('localhost', controlador, config={'allow_public_attrs': True})
        proxy_control.root.altera_limiar(novo_limiar)
        sucesso_control = True

    except ConnectionRefusedError as e:
        print(f'CONTROLADOR {random_gerado+1} FORA DO AR')

        try:
            proxy_control_reserva = rpyc.connect('localhost', controlador_reserva, config={'allow_public_attrs': True})
            proxy_control_reserva.root.altera_limiar(novo_limiar)
            sucesso_control = True

        except ConnectionRefusedError as e:
            random_gerado_reserva = 0 if random_gerado == 1 else 1
            print(f'CONTROLADOR {random_gerado_reserva+1} FORA DO AR')
    
    if(sucesso_control):
        return jsonify({"sucesso": True, "novo-limiar": novo_limiar})
    else:
        return jsonify({"sucesso": False, "erro": "Os dois controladores fora do ar"})

app.run(debug=True)