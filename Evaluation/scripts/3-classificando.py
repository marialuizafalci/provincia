import os
import shutil
import cv2
import numpy as np
import getpass
import socket
import platform
import hashlib
from datetime import datetime
from dfa_lib_python.dataflow import Dataflow
from dfa_lib_python.transformation import Transformation
from dfa_lib_python.set import Set
from dfa_lib_python.set_type import SetType
from dfa_lib_python.attribute import Attribute
from dfa_lib_python.attribute_type import AttributeType
from dfa_lib_python.task import Task
from dfa_lib_python.dataset import DataSet
from dfa_lib_python.element import Element

# ==== Configurações Gerais ====
frames_folder = 'frames'  # Pasta com as imagens de entrada
base_output_folder = '/mnt/d/Doutorado/ProvInCiA/Chuvas/Evaluation/classificados'  # Pastas finais de destino

# Mapear classes para pastas corretas
class_map = {
    "chuva_com_alagamento": os.path.join(base_output_folder, "chuva_com_alagamento"),
    "chuva_forte": os.path.join(base_output_folder, "chuva_forte"),
    "chuva_leve": os.path.join(base_output_folder, "chuva_leve"),
    "sem_chuva": os.path.join(base_output_folder, "sem_chuva"),
    "indefinido": os.path.join(base_output_folder, "indefinido")  # Para imagens não classificáveis
}

# ==== Configuração do Dataflow para Proveniência (DFAnalyzer) ====
dataflow_tag = "classificar_frames"
dflow = Dataflow(dataflow_tag)

transf = Transformation("ClassificarFrames")

input_set = Set("FramesNaoClassificados", SetType.  INPUT, [
    Attribute("frame_path", AttributeType.FILE)
])

output_set = Set("FramesClassificados", SetType.OUTPUT, [
    Attribute("frame_path", AttributeType.FILE),
    Attribute("predicted_class", AttributeType.TEXT)
])

system_metadata = Set("system_metadata3", SetType.INPUT, [
    Attribute("executed_by", AttributeType.TEXT),
    Attribute("cwd", AttributeType.TEXT),
    Attribute("hostname", AttributeType.TEXT),
    Attribute("python_version", AttributeType.TEXT)
])

script_metadata = Set("script_metadata3", SetType.INPUT, [
    Attribute("script_path", AttributeType.TEXT),
    Attribute("script_last_modified", AttributeType.TEXT),
    Attribute("script_hash", AttributeType.TEXT)
])

transf.set_sets([input_set, output_set, system_metadata, script_metadata])
dflow.add_transformation(transf)
dflow.save()

# ==== Metadados de sistema/script ====
executed_by = getpass.getuser()
cwd = os.getcwd()
hostname = socket.gethostname()
python_version = platform.python_version()

script_path = os.path.abspath(__file__)
script_last_modified = datetime.fromtimestamp(os.path.getmtime(__file__)).isoformat()
with open(__file__, 'rb') as f:
    script_hash = hashlib.sha256(f.read()).hexdigest()

# ==== Função para Classificar Imagem ====
def classificar_imagem(img_path):
    img = cv2.imread(img_path, cv2.IMREAD_GRAYSCALE)
    if img is None:
        return "indefinido"

    brightness = np.mean(img)

    if brightness < 50:
        return "chuva_com_alagamento"  # Muito escuro
    elif brightness < 100:
        return "chuva_forte"
    elif brightness < 150:
        return "chuva_leve"
    else:
        return "sem_chuva"

# ==== Função Principal ====
def classificar_frames():
    count = 1

    for frame_file in os.listdir(frames_folder):
        frame_path = os.path.join(frames_folder, frame_file)

        if not os.path.isfile(frame_path):
            continue

        classe = classificar_imagem(frame_path)
        destino_classe = class_map.get(classe, class_map["indefinido"])

        os.makedirs(destino_classe, exist_ok=True)
        destino_path = os.path.join(destino_classe, frame_file)

        task = Task(count, dataflow_tag, "ClassificarFrames")
        task.begin()

        shutil.copy2(frame_path, destino_path)

        task.add_dataset(DataSet("FramesNaoClassificados", [Element([frame_path])]))
        task.add_dataset(DataSet("FramesClassificados", [Element([destino_path, classe])]))
        task.add_dataset(DataSet("system_metadata3", [Element([
            executed_by, cwd, hostname, python_version
        ])]))
        task.add_dataset(DataSet("script_metadata3", [Element([
            script_path, script_last_modified, script_hash
        ])]))

        task.end()

        print(f"{frame_file} classificado como {classe}")
        count += 1

if __name__ == "__main__":
    classificar_frames()
    print("Classificação concluída e proveniência registrada.")
