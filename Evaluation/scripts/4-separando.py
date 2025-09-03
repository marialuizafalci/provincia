import os
import random
import shutil
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
dataset_path = 'classificados'  # Pasta onde estão os frames classificados em subpastas
output_base_path = 'separados'  # Pasta de saída para train/test/val
split_ratios = (0.7, 0.15, 0.15)  # Treino, validação e teste

# ==== Configuração da Proveniência (DFAnalyzer) ====
dataflow_tag = "separando_frames"
dflow = Dataflow(dataflow_tag)

transf = Transformation("SepararTrainTestVali")

# Conjuntos
input_set = Set("FramesClassificados", SetType.INPUT, [
    Attribute("frame_path", AttributeType.FILE)
])


output_set = Set("FramesSeparados", SetType.OUTPUT, [
    Attribute("path_final", AttributeType.FILE)
])

system_metadata4 = Set("system_metadata4", SetType.INPUT, [
    Attribute("executed_by", AttributeType.TEXT),
    Attribute("cwd", AttributeType.TEXT),
    Attribute("hostname", AttributeType.TEXT),
    Attribute("python_version", AttributeType.TEXT)
])

script_metadata4 = Set("script_metadata4", SetType.INPUT, [
    Attribute("script_path", AttributeType.TEXT),
    Attribute("script_last_modified", AttributeType.TEXT),
    Attribute("script_hash", AttributeType.TEXT)
])

transf.set_sets([input_set, output_set, system_metadata4, script_metadata4])
dflow.add_transformation(transf)
dflow.save()

# Metadados de sistema/script
executed_by = getpass.getuser()
cwd = os.getcwd()
hostname = socket.gethostname()
python_version = platform.python_version()

script_path = os.path.abspath(__file__)
script_last_modified = datetime.fromtimestamp(os.path.getmtime(__file__)).isoformat()
with open(__file__, 'rb') as f:
    script_hash = hashlib.sha256(f.read()).hexdigest()

# ==== Função Principal ====
def separar_dataset():
    classes = os.listdir(dataset_path)
    count = 1

    for classe in classes:
        class_path = os.path.join(dataset_path, classe)
        if not os.path.isdir(class_path):
            continue

        imagens = os.listdir(class_path)
        random.shuffle(imagens)

        total = len(imagens)
        treino = imagens[:int(total * split_ratios[0])]
        validacao = imagens[int(total * split_ratios[0]):int(total * (split_ratios[0] + split_ratios[1]))]
        teste = imagens[int(total * (split_ratios[0] + split_ratios[1])):]

        # Criar pastas
        for tipo, lista in zip(['train', 'validation', 'test'], [treino, validacao, teste]):
            dest_dir = os.path.join(output_base_path, tipo, classe)
            os.makedirs(dest_dir, exist_ok=True)

            for img in lista:
                origem = os.path.join(class_path, img)
                destino = os.path.join(dest_dir, img)

                task = Task(count, dataflow_tag, "SepararTrainTestVali")
                task.begin()

                shutil.copy2(origem, destino)

                task.add_dataset(DataSet("FramesClassificados_Separacao", [Element([origem])]))
                task.add_dataset(DataSet("FramesSeparados", [Element([destino])]))
                task.add_dataset(DataSet("system_metadata4", [Element([executed_by, cwd, hostname, python_version])]))
                task.add_dataset(DataSet("script_metadata4", [Element([script_path, script_last_modified, script_hash])]))

                task.end()
                count += 1

if __name__ == "__main__":
    separar_dataset()
    print("Separacao concluida e proveniencia registrada.")
