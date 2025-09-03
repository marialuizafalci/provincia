import tensorflow as tf
import numpy as np
import matplotlib.pyplot as plt
import os
import getpass
import socket
import hashlib
import platform
from datetime import datetime
from tensorflow.keras.models import load_model
from tensorflow.keras.preprocessing import image
from sklearn.metrics import accuracy_score

# ====== DFANALYZER IMPORTS ======
from dfa_lib_python.dataflow import Dataflow
from dfa_lib_python.transformation import Transformation
from dfa_lib_python.set import Set
from dfa_lib_python.set_type import SetType
from dfa_lib_python.attribute import Attribute
from dfa_lib_python.attribute_type import AttributeType
from dfa_lib_python.task import Task
from dfa_lib_python.dataset import DataSet
from dfa_lib_python.element import Element

# ====== FUNÇÕES DE METADADOS ======
def coletar_metadados_script(script_path):
    with open(script_path, "rb") as f:
        conteudo = f.read()
    script_hash = hashlib.md5(conteudo).hexdigest()
    modificado = datetime.fromtimestamp(os.path.getmtime(script_path))
    return script_path, modificado.isoformat(), script_hash

def coletar_metadados_sistema():
    return getpass.getuser(), os.getcwd(), socket.gethostname(), platform.python_version()

# ====== CONFIGURAÇÃO DO FLUXO DE DADOS ======
dataflow_tag = "teste_modelo_cnn_chuvas"
dflow = Dataflow(dataflow_tag)

transf = Transformation("TesteModeloCNNChuvas")

# Conjuntos de dados
input_set = Set("ParametrosEntrada", SetType.INPUT, [
    Attribute("diretorio_modelo", AttributeType.TEXT),
    Attribute("diretorio_teste", AttributeType.TEXT),
    Attribute("img_height", AttributeType.TEXT),
    Attribute("img_width", AttributeType.TEXT)
])

frames_teste_set = Set("FramesTeste", SetType.INPUT, [
    Attribute("frame_path", AttributeType.FILE)
])

output_set = Set("ResultadosPrevisao", SetType.OUTPUT, [
    Attribute("classe_prevista", AttributeType.TEXT),
    Attribute("classe_verdadeira", AttributeType.TEXT)
])

script_metadata_set = Set("script_metadata6", SetType.INPUT, [
    Attribute("script_path", AttributeType.TEXT),
    Attribute("script_last_modified", AttributeType.TEXT),
    Attribute("script_hash", AttributeType.TEXT)
])

sistema_metadata_set = Set("sistema_metadata6", SetType.INPUT, [
    Attribute("executed_by", AttributeType.TEXT),
    Attribute("cwd", AttributeType.TEXT),
    Attribute("hostname", AttributeType.TEXT),
    Attribute("python_version", AttributeType.TEXT)
])

transf.set_sets([input_set, frames_teste_set, output_set, script_metadata_set, sistema_metadata_set])
dflow.add_transformation(transf)
dflow.save()

# ====== DEFINIÇÃO DOS PARÂMETROS ======
diretorio_modelo = "modelo_salvo/modelo_cnn.h5"
diretorio_teste = "separados/test"
img_height = 64
img_width = 64

# ====== INICIAR TASK ======
task = Task(1, dataflow_tag, "TesteModeloCNNChuvas")
task.begin()

task.add_dataset(DataSet("ParametrosEntrada", [Element([
    diretorio_modelo,
    diretorio_teste,
    str(img_height),
    str(img_width)
])]))

# ====== METADADOS ======
script_path = __file__ if "__file__" in globals() else "5-teste.py"
script_metadata6 = coletar_metadados_script(script_path)
sistema_metadata6 = coletar_metadados_sistema()

task.add_dataset(DataSet("script_metadata6", [Element(list(script_metadata6))]))
task.add_dataset(DataSet("sistema_metadata6", [Element(list(sistema_metadata6))]))

# ====== CARREGAR MODELO ======
model = load_model(diretorio_modelo)

class_names = ['sem_chuva', 'chuva_leve', 'chuva_forte', 'chuva_com_alagamento']

# ====== PROCESSAR FRAMES ======
X_test = []
y_true = []
y_pred = []
frames_usados = []
resultado_final = []

for root, _, files in os.walk(diretorio_teste):
    for file in files:
        caminho_img = os.path.join(root, file)
        if os.path.isfile(caminho_img):
            try:
                img = image.load_img(caminho_img, target_size=(img_height, img_width))
                img_array = image.img_to_array(img)
                img_array = tf.expand_dims(img_array, 0)
                predictions = model.predict(img_array)
                score = tf.nn.softmax(predictions[0])
                predicted_class = class_names[np.argmax(score)]

                # Inferir a classe verdadeira do caminho
                true_class = None
                if 'sem_chuva' in caminho_img:
                    true_class = 'sem_chuva'
                elif 'chuva_leve' in caminho_img:
                    true_class = 'chuva_leve'
                elif 'chuva_forte' in caminho_img:
                    true_class = 'chuva_forte'
                elif 'chuva_com_alagamento' in caminho_img:
                    true_class = 'chuva_com_alagamento'

                if true_class:
                    y_true.append(true_class)
                    y_pred.append(predicted_class)
                    resultado_final.append(Element([predicted_class, true_class]))
                    frames_usados.append(Element([caminho_img]))
                    print(f"✅ Imagem: {file} | Previsto: {predicted_class} | Verdadeiro: {true_class}")
                else:
                    print(f"⚠️ Caminho não identificou classe verdadeira: {caminho_img}")

            except Exception as e:
                print(f"[ERRO] Falha ao processar {file}: {e}")

# ====== REGISTRAR NO DFANALYZER ======
task.add_dataset(DataSet("FramesTeste", frames_usados))
task.add_dataset(DataSet("ResultadosPrevisao", resultado_final))
task.end()

# ====== MÉTRICA OPCIONAL ======
if y_true and y_pred:
    acuracia = accuracy_score(y_true, y_pred)
    print(f"\n🎯 Acurácia no conjunto de teste: {acuracia:.2%}")
