#!/usr/bin/env python3
# 5-treinamento.py
import os, getpass, socket, platform, hashlib
from datetime import datetime
import tensorflow as tf
from tensorflow.keras.preprocessing.image import ImageDataGenerator
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Conv2D, MaxPooling2D, Flatten, Dense, Dropout

from dfa_lib_python.dataflow import Dataflow
from dfa_lib_python.transformation import Transformation
from dfa_lib_python.set import Set, SetType
from dfa_lib_python.attribute import Attribute
from dfa_lib_python.attribute_type import AttributeType
from dfa_lib_python.task import Task
from dfa_lib_python.dataset import DataSet
from dfa_lib_python.element import Element

# ------------------------------------------------------------------
# 1. DEFINIÇÕES BÁSICAS
# ------------------------------------------------------------------
dataflow_tag = "treinamento_cnn"          # mantenha o mesmo tag
transformation_name = "treinamentomodelov1"

# ------------------------------------------------------------------
# 2. (OPCIONAL) REGISTRAR DATAFLOW + TRANSFORMAÇÃO
#    Rode apenas na primeira vez (ou deixe; o save ignora duplicatas).
# ------------------------------------------------------------------
dflow = Dataflow(dataflow_tag)

transf = Transformation(transformation_name)
transf.set_sets([
    Set("imagens_treinamento", SetType.INPUT,
        [Attribute("caminho_imagem", AttributeType.TEXT)]),
    Set("modelo_treinado", SetType.OUTPUT,
        [Attribute("modelo_path", AttributeType.FILE)]),
    Set("system_metadata5", SetType.INPUT, [
        Attribute("executed_by", AttributeType.TEXT),
        Attribute("cwd", AttributeType.TEXT),
        Attribute("hostname", AttributeType.TEXT),
        Attribute("python_version", AttributeType.TEXT)]),
    Set("script_metadata5", SetType.INPUT, [
        Attribute("script_path", AttributeType.TEXT),
        Attribute("script_last_modified", AttributeType.TEXT),
        Attribute("script_hash", AttributeType.TEXT),
        Attribute("start_time", AttributeType.TEXT),
        Attribute("end_time", AttributeType.TEXT)])
])
dflow.add_transformation(transf)
dflow.save()                   # cria (ou só versiona) todas as tabelas necessárias

# ------------------------------------------------------------------
# 3. METADADOS DE AMBIENTE
# ------------------------------------------------------------------
executed_by, cwd = getpass.getuser(), os.getcwd()
hostname, python_version = socket.gethostname(), platform.python_version()
script_path = os.path.abspath(__file__)
script_last_modified = datetime.fromtimestamp(os.path.getmtime(__file__)).isoformat()
with open(__file__, "rb") as f:
    script_hash = hashlib.sha256(f.read()).hexdigest()

# ------------------------------------------------------------------
# 4. ABRE O TASK
# ------------------------------------------------------------------
task = Task(1, dataflow_tag, transformation_name)
task.begin()
start_time = datetime.now().isoformat()

# ------------------------------------------------------------------
# 5. TREINAMENTO DO MODELO
# ------------------------------------------------------------------
train_gen = ImageDataGenerator(rescale=1./255).flow_from_directory(
    "separados/train", target_size=(64, 64), batch_size=32, class_mode="binary"
)
val_gen = ImageDataGenerator(rescale=1./255).flow_from_directory(
    "separados/validation", target_size=(64, 64), batch_size=32, class_mode="binary"
)

model = Sequential([
    Conv2D(32, (3,3), activation="relu", input_shape=(64, 64, 3)),
    MaxPooling2D(2,2),
    Conv2D(32, (3,3), activation="relu"),
    MaxPooling2D(2,2),
    Flatten(),
    Dense(128, activation="relu"),
    Dropout(0.5),
    Dense(1, activation="sigmoid")
])
model.compile(optimizer="adam", loss="binary_crossentropy", metrics=["accuracy"])
model.fit(train_gen, epochs=10, validation_data=val_gen)

# ------------------------------------------------------------------
# 6. SALVA O MODELO
# ------------------------------------------------------------------
os.makedirs("modelo_salvo", exist_ok=True)
model_path = "modelo_salvo/modelo_cnn.h5"
model.save(model_path)

# ------------------------------------------------------------------
# --- 7. PROVENIÊNCIA ---------------------------------------------
imagens_treino = train_gen.filepaths

# Adiciona cada caminho de imagem individualmente
for caminho_p_imagem in imagens_treino:
    task.add_dataset(
        DataSet("imagens_treinamento",
                [Element([caminho_p_imagem])])  # Um Element por DataSet
    )


task.add_dataset(DataSet("modelo_treinado",
                         [Element([model_path])]))
task.add_dataset(DataSet("system_metadata5",
                         [Element([executed_by, cwd, hostname, python_version])]))

end_time = datetime.now().isoformat()
task.add_dataset(DataSet("script_metadata5",
                         [Element([script_path, script_last_modified,
                                   script_hash, start_time, end_time])]))

task.end()
print(f"✅ Treinamento concluído – {len(imagens_treino)} imagens registradas.")
